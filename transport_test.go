package alitycs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestTransport(t *testing.T, serverURL string, maxRetries int) *transport {
	t.Helper()
	return &transport{
		endpoint:   serverURL,
		apiKey:     "pk_transport",
		maxRetries: maxRetries,
		client:     &http.Client{Timeout: 2 * time.Second},
		backoff:    func(int) time.Duration { return time.Millisecond },
	}
}

func samplePayload() *BatchPayload {
	return &BatchPayload{
		BatchID: "batch_test",
		SentAt:  nowMillis(),
		Events:  []Event{testEvent("transport_probe")},
	}
}

func TestTransportSendsHeadersAndBody(t *testing.T) {
	var gotAuth, gotContentType, gotMethod, gotPath string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotMethod = r.Method
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL+"/ingest", 0)
	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("send: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %s", gotMethod)
	}
	if gotPath != "/ingest" {
		t.Errorf("path = %s", gotPath)
	}
	if gotAuth != "Bearer pk_transport" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if !strings.HasPrefix(gotContentType, "application/json") {
		t.Errorf("Content-Type = %q", gotContentType)
	}
	if !strings.Contains(string(gotBody), `"batchId":"batch_test"`) || !strings.Contains(string(gotBody), `"event":"transport_probe"`) {
		t.Errorf("body = %s", gotBody)
	}
}

func TestTransportRetries5xxUntilSuccess(t *testing.T) {
	var attempts atomic.Int32
	var firstBody, secondBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		n := attempts.Add(1)
		if n == 1 {
			firstBody = body
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		secondBody = body
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 3)
	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("send should succeed after retry: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if string(firstBody) != string(secondBody) {
		t.Errorf("retry must resend the identical payload (same batchId):\n%s\n%s", firstBody, secondBody)
	}
}

func TestTransportExhaustsRetriesOnPersistent500(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 2)
	err := tr.send(context.Background(), samplePayload())
	if err == nil {
		t.Fatal("persistent 500 must surface an error")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Errorf("error = %v, want the last status error", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want initial + 2 retries = 3", got)
	}
}

func TestTransportDoesNotRetryClientErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 5)
	err := tr.send(context.Background(), samplePayload())
	if err == nil {
		t.Fatal("400 must surface an error")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (no retry on 4xx)", got)
	}
}

func TestTransportRetries429(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 3)
	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("429 then success: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestTransportRetriesNetworkErrors(t *testing.T) {
	// A server we close immediately produces connection refused.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	server.Close()

	tr := newTestTransport(t, server.URL, 2)
	err := tr.send(context.Background(), samplePayload())
	if err == nil {
		t.Fatal("network failure must surface an error")
	}
	if !strings.Contains(err.Error(), "retries exhausted") {
		t.Errorf("error = %v", err)
	}
}

func TestTransportContextCancellationDuringBackoff(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 5)
	tr.backoff = func(int) time.Duration { return time.Hour } // never reached naturally

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := tr.send(ctx, samplePayload())
	if err == nil {
		t.Fatal("cancelled send must surface an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap the ctx error", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Errorf("cancellation during backoff took %s; backoff is not ctx-aware", time.Since(started))
	}
	_ = attempts.Load()
}

func TestBackoffDelaySchedule(t *testing.T) {
	cases := map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		5: 10 * time.Second, // cap
		9: 10 * time.Second,
	}
	for attempt, want := range cases {
		if got := backoffDelay(attempt); got != want {
			t.Errorf("backoffDelay(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func TestTransportHonoursRetryAfterSeconds(t *testing.T) {
	var attempts atomic.Int32
	var firstBody, secondBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if attempts.Add(1) == 1 {
			firstBody = body
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		secondBody = body
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 3)
	var waits []time.Duration
	tr.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}

	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("send after 429+Retry-After: %v", err)
	}
	if len(waits) != 1 || waits[0] < 2*time.Second {
		t.Fatalf("waits = %v, want the full Retry-After of 2s honoured", waits)
	}
	// Only timing changes between attempts: the redelivered batch is byte-identical.
	if string(firstBody) != string(secondBody) {
		t.Errorf("retry must resend the identical payload:\n%s\n%s", firstBody, secondBody)
	}
}

func TestTransportHonoursRetryAfterHTTPDate(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", time.Now().Add(3*time.Second).UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 3)
	var waits []time.Duration
	tr.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}

	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("send after 429+Retry-After date: %v", err)
	}
	if len(waits) != 1 || waits[0] < 2*time.Second || waits[0] > 3*time.Second {
		// http.TimeFormat has second precision, so formatting now+3s truncates the
		// sub-second part and the honoured wait lands anywhere in (2s, 3s].
		t.Fatalf("waits = %v, want ~3s derived from the HTTP-date", waits)
	}
}

func TestTransportDoesNotShortenRetryAfterToMaxBackoff(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 3)
	var waits []time.Duration
	tr.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}

	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(waits) != 1 || waits[0] != time.Hour {
		t.Fatalf("waits = %v, want the full server Retry-After of 1h", waits)
	}
}

func TestTransportRetryAfterAppliesOnlyToTheNextAttempt(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch attempts.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, 3)
	tr.backoff = func(int) time.Duration { return 7 * time.Second } // deterministic fallback
	var waits []time.Duration
	tr.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}

	if err := tr.send(context.Background(), samplePayload()); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(waits) != 2 {
		t.Fatalf("waits = %v, want two", waits)
	}
	if waits[0] < 2*time.Second {
		t.Errorf("first wait = %s, want the Retry-After of at least 2s (not the 7s schedule)", waits[0])
	}
	// The second retry follows a 500, so the Retry-After no longer applies and the
	// default schedule returns.
	if waits[1] != 7*time.Second {
		t.Errorf("second wait = %s, want the 7s default schedule", waits[1])
	}
}

// jitteredBounds returns the ±20% window jitter() may produce for a delay.
type jitterBounds struct{ min, max time.Duration }

func jitteredBounds(d time.Duration) jitterBounds {
	return jitterBounds{min: time.Duration(float64(d) * 0.8), max: time.Duration(float64(d) * 1.2)}
}

func TestProductionBackoffIsJitteredWithinTwentyPercent(t *testing.T) {
	tr := &transport{}
	for attempt := 1; attempt <= 5; attempt++ {
		base := backoffDelay(attempt)
		bounds := jitteredBounds(base)
		for i := 0; i < 200; i++ {
			got := tr.delayFor(attempt)
			if got < bounds.min || got > bounds.max {
				t.Fatalf("delayFor(%d) = %s, want within [%s, %s]", attempt, got, bounds.min, bounds.max)
			}
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

	if _, ok := parseRetryAfter("", now); ok {
		t.Error("empty header must not be treated as a suggestion")
	}
	if _, ok := parseRetryAfter("soon", now); ok {
		t.Error("garbage header must not be treated as a suggestion")
	}
	if got, ok := parseRetryAfter("120", now); !ok || got != 120*time.Second {
		t.Errorf("delta-seconds = %s (%v), want 2m0s", got, ok)
	}
	if _, ok := parseRetryAfter("-5", now); ok {
		t.Error("negative delta-seconds must be rejected")
	}
	past := now.Add(-time.Minute).Format(http.TimeFormat)
	if got, ok := parseRetryAfter(past, now); !ok || got != 0 {
		t.Errorf("past HTTP-date = %s, want zero", got)
	}
	future := now.Add(90 * time.Second).Format(http.TimeFormat)
	if got, ok := parseRetryAfter(future, now); !ok || got < 89*time.Second || got > 90*time.Second {
		t.Errorf("future HTTP-date = %s, want ~90s", got)
	}
}
