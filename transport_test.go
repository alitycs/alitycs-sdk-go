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
