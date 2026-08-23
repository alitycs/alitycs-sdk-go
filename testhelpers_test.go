package alitycs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// capturedRequest records one ingest request as the transport sent it.
type capturedRequest struct {
	method      string
	auth        string
	contentType string
	body        []byte
	payload     BatchPayload
}

// captureServer is an httptest server recording ingest batches. It can fail
// requests with a 500 to exercise retry, and can block handlers until
// released.
type captureServer struct {
	t *testing.T

	mu       sync.Mutex
	requests []capturedRequest

	failRemaining int // requests answered 500 while > 0

	blocking bool          // handlers block until release is closed
	release  chan struct{} // closed by unblock()

	endpoint string
}

func newCaptureServer(t *testing.T) *captureServer {
	s := &captureServer{t: t, release: make(chan struct{})}
	server := httptest.NewServer(s.serve())
	s.endpoint = server.URL + "/events"
	t.Cleanup(server.Close)
	return s
}

func (s *captureServer) serve() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var payload BatchPayload
		_ = json.Unmarshal(body, &payload)

		s.mu.Lock()
		s.requests = append(s.requests, capturedRequest{
			method:      r.Method,
			auth:        r.Header.Get("Authorization"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
			payload:     payload,
		})
		fail := s.failRemaining > 0
		if fail {
			s.failRemaining--
		}
		block := s.blocking
		s.mu.Unlock()

		if block {
			<-s.release
		}
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"test_fail"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	})
}

// url returns the endpoint the SDK should be pointed at.
func (s *captureServer) url() string {
	return s.endpoint
}

func (s *captureServer) failNext() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRemaining++
}

// failForever answers 500 until unblocked by resetFailures.
func (s *captureServer) failForever() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRemaining = 1 << 30
}

func (s *captureServer) block() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocking = true
}

func (s *captureServer) unblock() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blocking {
		s.blocking = false
		close(s.release)
	}
}

func (s *captureServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *captureServer) request(index int) capturedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index >= len(s.requests) {
		s.t.Fatalf("expected more than %d requests, have %d", index, len(s.requests))
	}
	return s.requests[index]
}

func (s *captureServer) all() []capturedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]capturedRequest, len(s.requests))
	copy(out, s.requests)
	return out
}

func (s *captureServer) events() []Event {
	var events []Event
	for _, request := range s.all() {
		events = append(events, request.payload.Events...)
	}
	return events
}

func (s *captureServer) eventByName(name string) Event {
	events := s.events()
	for _, event := range events {
		if event.Event == name {
			return event
		}
	}
	s.t.Fatalf("no event named %q arrived; got %v", name, namesOf(events))
	return Event{}
}

func namesOf(events []Event) []string {
	names := make([]string, len(events))
	for i, event := range events {
		names[i] = event.Event
	}
	return names
}

// newTestClient builds a Client pointed at a fresh capture server. The timer
// flush is disabled — tests drive sends explicitly.
func newTestClient(t *testing.T, extra ...Option) (*Client, *captureServer) {
	t.Helper()

	capture := newCaptureServer(t)
	client, err := New("pk_test_key", append([]Option{
		WithEndpoint(capture.url()),
		WithFlushInterval(0),
		WithMaxRetries(3),
	}, extra...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.transporter.backoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(func() { _ = client.Shutdown(context.Background()) })
	return client, capture
}
