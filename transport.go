package alitycs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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

	// backoff overrides the retry schedule; tests use it to avoid real
	// sleeps. nil means the production schedule.
	backoff func(attempt int) time.Duration
}

// send delivers one batch, retrying 5xx and 429 responses plus network
// errors with exponential backoff (1s, 2s, 4s … capped at 10s). Other 4xx
// responses are terminal and are returned as an error. The payload is
// marshalled once and the identical body is retried so a batch keeps its
// batchId across attempts.
func (t *transport) send(ctx context.Context, payload *BatchPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("alitycs: encode batch: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		if attempt > 0 {
			t.debugf("attempt %d failed (%v), retrying…", attempt, lastErr)
			if err := sleepContext(ctx, t.delayFor(attempt)); err != nil {
				return fmt.Errorf("alitycs: send cancelled during backoff: %w", err)
			}
		}

		err := t.post(ctx, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, errTerminalStatus) {
			t.debugf("%v — not retrying", err)
			return err
		}
	}

	return fmt.Errorf("alitycs: all %d retries exhausted: %w", t.maxRetries, lastErr)
}

var errTerminalStatus = errors.New("terminal status")

func (t *transport) post(ctx context.Context, body []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("alitycs: build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+t.apiKey)

	response, err := t.client.Do(request)
	if err != nil {
		return fmt.Errorf("alitycs: POST %s: %w", t.endpoint, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}

	statusErr := fmt.Errorf("alitycs: unexpected status %d sending batch", response.StatusCode)
	if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
		return errors.Join(statusErr, errTerminalStatus)
	}
	return statusErr
}

func (t *transport) debugf(format string, args ...any) {
	debugLog(t.debug, format, args...)
}

// delayFor is the wait before retry attempt.
func (t *transport) delayFor(attempt int) time.Duration {
	if t.backoff != nil {
		return t.backoff(attempt)
	}
	return backoffDelay(attempt)
}

// backoffDelay is the wait before retry attempt (1-indexed): 1s doubling to a
// 10s cap — the same schedule as the JavaScript and JVM SDKs.
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
