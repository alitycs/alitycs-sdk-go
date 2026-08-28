package alitycs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

const maxBackoff = 10 * time.Second

// transport POSTs batch payloads to the ingest endpoint with retry.
type transport struct {
	endpoint   string
	apiKey     string
	maxRetries int
	client     *http.Client
	debug      bool
	store      *fileBatchStore

	// backoff overrides the retry schedule; tests use it to avoid real
	// sleeps. nil means the production schedule.
	backoff func(attempt int) time.Duration

	// sleep overrides the inter-attempt wait; tests inject a recorder here.
	// nil means sleepContext (a ctx-aware timer).
	sleep func(ctx context.Context, d time.Duration) error
}

// send delivers one batch, retrying 5xx and 429 responses plus network
// errors with exponential backoff (1s, 2s, 4s … capped at 10s, jittered ±20%).
// A 429 carrying Retry-After (delta-seconds or HTTP-date) is honoured: the next
// attempt waits at least that long instead of the default schedule. Other 4xx
// responses are terminal and are returned as an error. The payload is
// marshalled once and the identical body is retried so a batch keeps its
// batchId across attempts — only the timing changes between attempts.
func (t *transport) send(ctx context.Context, payload *BatchPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("alitycs: encode batch: %w", err)
	}

	record := durableBatchRecord{
		BatchID: payload.BatchID, Body: string(body), EventCount: len(payload.Events),
	}
	if err := t.store.put(record); err != nil {
		return err
	}
	err = t.sendRecord(ctx, record)
	if err != nil && t.store.enabled() {
		return &durableBatchError{cause: err}
	}
	return err
}

// durableBatchError proves the exact body is already owned by the WAL.
type durableBatchError struct{ cause error }

func (e *durableBatchError) Error() string { return e.cause.Error() }
func (e *durableBatchError) Unwrap() error { return e.cause }

type retryAfterError struct {
	cause   error
	untilMS int64
}

func (e *retryAfterError) Error() string { return e.cause.Error() }
func (e *retryAfterError) Unwrap() error { return e.cause }

func (t *transport) recover(ctx context.Context) error {
	for _, record := range t.store.snapshot() {
		if remaining := time.Until(time.UnixMilli(record.PausedUntilMS)); record.PausedUntilMS > 0 && remaining > 0 {
			sleepFn := t.sleep
			if sleepFn == nil {
				sleepFn = sleepContext
			}
			if err := sleepFn(ctx, remaining); err != nil {
				return err
			}
		}
		if err := t.sendRecord(ctx, record); err != nil {
			var terminal *terminalStatusError
			if errors.As(err, &terminal) {
				continue
			}
			return err
		}
	}
	return nil
}

func (t *transport) sendRecord(ctx context.Context, record durableBatchRecord) error {
	body := []byte(record.Body)
	var lastErr error
	retryAfter := time.Duration(0)
	hasRetryAfter := false
	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		if attempt > 0 {
			delay := t.delayFor(attempt)
			if hasRetryAfter {
				delay = retryAfter
			}
			hasRetryAfter = false
			t.debugf("attempt %d failed (%v), retrying…", attempt, lastErr)
			sleepFn := t.sleep
			if sleepFn == nil {
				sleepFn = sleepContext
			}
			if err := sleepFn(ctx, delay); err != nil {
				return fmt.Errorf("alitycs: send cancelled during backoff: %w", err)
			}
		}

		suggested, suggestedOK, err := t.post(ctx, body)
		if err == nil {
			if storeErr := t.store.acknowledge(record.BatchID); storeErr != nil {
				return storeErr
			}
			return nil
		}
		lastErr = err
		retryAfter, hasRetryAfter = suggested, suggestedOK
		if errors.Is(err, errTerminalStatus) {
			if storeErr := t.store.acknowledge(record.BatchID); storeErr != nil {
				return storeErr
			}
			t.debugf("%v — not retrying", err)
			return err
		}
	}

	wrapped := fmt.Errorf("alitycs: all %d retries exhausted: %w", t.maxRetries, lastErr)
	untilMS := int64(0)
	if hasRetryAfter {
		untilMS = time.Now().Add(retryAfter).UnixMilli()
	}
	if err := t.store.pause(record.BatchID, untilMS); err != nil {
		return err
	}
	return &retryAfterError{cause: wrapped, untilMS: untilMS}
}

var errTerminalStatus = errors.New("terminal status")

// terminalStatusError reports a non-retryable 4xx response. Status is kept so
// the batcher can tell an HTTP 400 whole-batch rejection (one invalid event
// poisons the entire batch) from other terminal failures.
type terminalStatusError struct {
	status int
}

func (e *terminalStatusError) Error() string {
	return fmt.Sprintf("alitycs: unexpected status %d sending batch", e.status)
}

func (e *terminalStatusError) Is(target error) bool { return target == errTerminalStatus }

// post performs one POST. On a 429 it also reports the server's Retry-After
// suggestion (hasRetryAfter is false when the header is absent or unparseable).
func (t *transport) post(ctx context.Context, body []byte) (retryAfter time.Duration, hasRetryAfter bool, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, false, fmt.Errorf("alitycs: build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+t.apiKey)

	response, err := t.client.Do(request)
	if err != nil {
		return 0, false, fmt.Errorf("alitycs: POST %s: %w", t.endpoint, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return 0, false, nil
	}

	if response.StatusCode == http.StatusTooManyRequests {
		retryAfter, hasRetryAfter = parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}

	statusErr := fmt.Errorf("alitycs: unexpected status %d sending batch", response.StatusCode)
	if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
		return retryAfter, hasRetryAfter, errors.Join(statusErr, &terminalStatusError{status: response.StatusCode})
	}
	return retryAfter, hasRetryAfter, statusErr
}

// parseRetryAfter converts a Retry-After header value into a wait duration:
// a delta-seconds integer or an HTTP-date. ok is false when absent or garbage;
// a date in the past yields zero.
func parseRetryAfter(value string, now time.Time) (d time.Duration, ok bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		const maxDurationSeconds = uint64((1<<63 - 1) / int64(time.Second))
		if seconds > maxDurationSeconds {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if when, err := http.ParseTime(value); err == nil {
		delay := when.Sub(now)
		if delay < 0 {
			delay = 0
		}
		return delay, true
	}
	return 0, false
}

func (t *transport) debugf(format string, args ...any) {
	debugLog(t.debug, format, args...)
}

// delayFor is the wait before retry attempt.
func (t *transport) delayFor(attempt int) time.Duration {
	if t.backoff != nil {
		return t.backoff(attempt)
	}
	return jitter(backoffDelay(attempt))
}

// jitter widens a delay by ±20% so many clients retrying after a shared failure do
// not hammer the server in lockstep. The injectable test schedule bypasses it.
func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// backoffDelay is the un-jittered wait before retry attempt (1-indexed): 1s
// doubling to a 10s cap — the same schedule as the JavaScript and JVM SDKs.
func backoffDelay(attempt int) time.Duration {
	delay := time.Second << (attempt - 1)
	if delay <= 0 || delay > maxBackoff {
		return maxBackoff
	}
	return delay
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func debugLog(enabled bool, format string, args ...any) {
	if enabled {
		log.Printf("[Alitycs] "+format, args...)
	}
}

// warnLog reports warn-level conditions — dropped or locally rejected events —
// regardless of the debug setting: silent data loss is never acceptable.
func warnLog(format string, args ...any) {
	log.Printf("[Alitycs] WARN "+format, args...)
}
