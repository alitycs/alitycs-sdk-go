package alitycs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewRequiresAPIKey(t *testing.T) {
	if _, err := New(""); !errors.Is(err, ErrAPIKeyRequired) {
		t.Fatalf("New(\"\") = %v, want ErrAPIKeyRequired", err)
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	cases := []struct {
		name  string
		opt   Option
		error string
	}{
		{"endpoint scheme", WithEndpoint("ftp://example.com"), "http or https"},
		{"bad endpoint", WithEndpoint("://"), "invalid endpoint"},
		{"flush size", WithFlushSize(0), "flush size"},
		{"flush interval", WithFlushInterval(-time.Second), "interval"},
		{"queue size", WithMaxQueueSize(0), "queue size"},
		{"retries", WithMaxRetries(-1), "retries"},
		{"session timeout", WithSessionTimeout(0), "session timeout"},
		{"http client", WithHTTPClient(nil), "http client"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New("pk_test", tc.opt); err == nil || !strings.Contains(err.Error(), tc.error) {
				t.Fatalf("New() error = %v, want containing %q", err, tc.error)
			}
		})
	}
}

func TestTrackBatchesAtFlushSize(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(2))

	client.Track(context.Background(), "conformance_track", Props{"n": 1})
	client.Track(context.Background(), "conformance_second", Props{"n": "two"})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := capture.count(); got != 1 {
		t.Fatalf("got %d requests after 2 events and a flush, want 1", got)
	}

	request := capture.request(0)
	if request.method != http.MethodPost {
		t.Errorf("method = %s, want POST", request.method)
	}
	if request.auth != "Bearer pk_test_key" {
		t.Errorf("Authorization = %q", request.auth)
	}
	if contentType := strings.Split(request.contentType, ";")[0]; contentType != "application/json" {
		t.Errorf("Content-Type = %q", request.contentType)
	}
	if len(request.payload.Events) != 2 {
		t.Fatalf("batch holds %d events, want 2", len(request.payload.Events))
	}

	first := request.payload.Events[0]
	if first.EventID == "" || !strings.HasPrefix(first.EventID, prefixEvent) {
		t.Errorf("eventId = %q, want evt_ prefix", first.EventID)
	}
	if first.EventType != eventTypeTrack {
		t.Errorf("eventType = %q, want track", first.EventType)
	}
	if first.Timestamp <= 0 {
		t.Errorf("timestamp = %d, want a real epoch millisecond value", first.Timestamp)
	}
	if first.Properties["n"] != "1" {
		t.Errorf("properties.n = %q, want \"1\" (schema requires string values)", first.Properties["n"])
	}
	if first.Context.SDKLanguage != "go" || first.Context.SDKVersion != Version {
		t.Errorf("context = %+v, want sdkLanguage go and sdkVersion %s", first.Context, Version)
	}
	if !strings.HasPrefix(first.AnonymousID, prefixAnon) || !strings.HasPrefix(first.SessionID, prefixSession) {
		t.Errorf("identity prefixes wrong: anon=%q session=%q", first.AnonymousID, first.SessionID)
	}
	if request.payload.BatchID == "" || request.payload.SentAt == 0 {
		t.Errorf("batch envelope incomplete: batchId=%q sentAt=%d", request.payload.BatchID, request.payload.SentAt)
	}
}

func TestFlushSendsPartialGroup(t *testing.T) {
	client, capture := newTestClient(t)

	client.Track(context.Background(), "lonely_event", nil)
	if capture.count() != 0 {
		t.Fatalf("event sent before flush")
	}
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	// A second flush with nothing new must not fabricate an empty batch.
	before := capture.count()
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if capture.count() != before {
		t.Fatalf("empty flush produced %d extra requests", capture.count()-before)
	}
}

func TestFlushAfterSizeTriggerDoesNotDuplicate(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(2))

	client.Track(context.Background(), "a", nil)
	client.Track(context.Background(), "b", nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := capture.count(); got != 1 {
		t.Fatalf("size trigger plus explicit flush produced %d requests, want exactly 1", got)
	}
	if events := capture.events(); len(events) != 2 {
		t.Fatalf("%d events delivered, want 2", len(events))
	}
}

func TestIdentifyStampsUserOnFollowingEvents(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	client.Identify(context.Background(), "usr_1842", Props{"plan": "pro"})
	client.Track(context.Background(), "after_identify", nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	identify := capture.eventByName("identify")
	if identify.EventType != eventTypeIdentify {
		t.Errorf("identify eventType = %q", identify.EventType)
	}
	if identify.UserID != "usr_1842" {
		t.Errorf("identify userId = %q", identify.UserID)
	}
	if identify.Properties["userId"] != "usr_1842" || identify.Properties["plan"] != "pro" {
		t.Errorf("identify properties = %v, want userId and plan", identify.Properties)
	}

	tracked := capture.eventByName("after_identify")
	if tracked.UserID != "usr_1842" {
		t.Errorf("tracked event userId = %q, want the identified user stamped on", tracked.UserID)
	}
}

func TestIdentifyRequiresUser(t *testing.T) {
	client, capture := newTestClient(t)

	client.Identify(context.Background(), "", Props{})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if capture.count() != 0 {
		t.Fatalf("blank identify produced a request")
	}
}

func TestResetRotatesIdentityAndClearsUser(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	client.Identify(context.Background(), "usr_before", nil)
	client.Reset()
	client.Track(context.Background(), "after_reset", nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	events := capture.events()
	var before, after *Event
	for i := range events {
		switch events[i].Event {
		case "identify":
			before = &events[i]
		case "after_reset":
			after = &events[i]
		}
	}
	if before == nil || after == nil {
		t.Fatalf("missing events: %v", namesOf(events))
	}
	if before.AnonymousID == after.AnonymousID {
		t.Errorf("reset kept anonymousId %q", before.AnonymousID)
	}
	if before.SessionID == after.SessionID {
		t.Errorf("reset kept sessionId %q", before.SessionID)
	}
	if after.UserID != "" {
		t.Errorf("event after reset carries userId %q", after.UserID)
	}
}

func TestWithUserIDOverridesSingleEvent(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	client.Identify(context.Background(), "usr_session", nil)
	client.Track(context.Background(), "override", nil, WithUserID("usr_other"))
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	event := capture.eventByName("override")
	if event.UserID != "usr_other" {
		t.Errorf("userId = %q, want the per-call override", event.UserID)
	}
}

func TestGlobalPropertiesMergeIntoSubsequentEventsOnly(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	client.Track(context.Background(), "before_globals", nil)
	client.SetGlobalProperties(Props{"suite": "go-test", "env": "test"})
	client.SetGlobalProperties(Props{"extra": "1"})
	client.Track(context.Background(), "after_globals", Props{"suite": "local-wins"})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	before := capture.eventByName("before_globals")
	if _, ok := before.Properties["suite"]; ok {
		t.Errorf("global properties leaked into earlier event: %v", before.Properties)
	}

	after := capture.eventByName("after_globals")
	if after.Properties["suite"] != "local-wins" {
		t.Errorf("per-event property should win, got %q", after.Properties["suite"])
	}
	if after.Properties["env"] != "test" || after.Properties["extra"] != "1" {
		t.Errorf("merged globals missing: %v", after.Properties)
	}

	client.ClearGlobalProperties()
	if got := client.GlobalProperties(); len(got) != 0 {
		t.Errorf("ClearGlobalProperties left %v", got)
	}
	client.Track(context.Background(), "after_clear", nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cleared := capture.eventByName("after_clear")
	if _, ok := cleared.Properties["suite"]; ok {
		t.Errorf("globals survived clear: %v", cleared.Properties)
	}
}

func TestCaptureErrorEventType(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	client.CaptureError(context.Background(), "checkout_failed", Props{"code": "E_TEST"})
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	event := capture.eventByName("checkout_failed")
	if event.EventType != eventTypeError {
		t.Errorf("eventType = %q, want error", event.EventType)
	}
	if event.Properties["code"] != "E_TEST" {
		t.Errorf("properties.code = %q", event.Properties["code"])
	}
}

func TestPageNames(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	client.Page(context.Background(), "Dashboard", nil)
	client.Page(context.Background(), "", nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	named := capture.eventByName("Dashboard")
	if named.EventType != eventTypePage {
		t.Errorf("named page eventType = %q", named.EventType)
	}
	fallback := capture.eventByName("page_view")
	if fallback.EventType != eventTypePage {
		t.Errorf("fallback page eventType = %q", fallback.EventType)
	}
}

func TestBlankEventNameIsDropped(t *testing.T) {
	client, capture := newTestClient(t)

	client.Track(context.Background(), "", nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if capture.count() != 0 {
		t.Fatalf("blank name produced a request")
	}
}

func TestTrackRevenueVariants(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(10))

	transaction, err := NewTransaction("fact-1", "19.99", "USD")
	if err != nil {
		t.Fatalf("NewTransaction: %v", err)
	}
	snapshot, err := NewMRRSnapshot("fact-2", "sub-1", "cust-1", "250.00", "USD")
	if err != nil {
		t.Fatalf("NewMRRSnapshot: %v", err)
	}
	baseline, err := NewMRRBaselineComplete("fact-3", "USD", 0)
	if err != nil {
		t.Fatalf("NewMRRBaselineComplete: %v", err)
	}

	client.TrackRevenue(context.Background(), transaction, Props{"source": "test"})
	client.TrackRevenue(context.Background(), snapshot, nil)
	client.TrackRevenue(context.Background(), baseline, nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	waitForRequests(t, capture, 1)

	txEvent := capture.eventByName("revenue_transaction")
	if txEvent.EventType != eventTypeTrack {
		t.Errorf("revenue eventType = %q, want track", txEvent.EventType)
	}
	wantTx := Revenue{Version: 1, Kind: "transaction", FactID: "fact-1", Amount: "19.99", Currency: "USD"}
	if !reflect.DeepEqual(*txEvent.Revenue, wantTx) {
		t.Errorf("transaction revenue = %+v, want %+v", *txEvent.Revenue, wantTx)
	}
	if txEvent.Revenue.ExpectedActiveSubscriptions != nil || txEvent.Revenue.SubscriptionID != "" {
		t.Errorf("transaction variant must not carry other variants' fields: %+v", *txEvent.Revenue)
	}
	if txEvent.Properties["source"] != "test" {
		t.Errorf("revenue properties = %v", txEvent.Properties)
	}

	snapshotEvent := capture.eventByName("revenue_mrr_snapshot")
	if snapshotEvent.Revenue.SubscriptionID != "sub-1" || snapshotEvent.Revenue.CustomerID != "cust-1" || snapshotEvent.Revenue.MRRAmount != "250.00" {
		t.Errorf("snapshot payload = %+v", *snapshotEvent.Revenue)
	}

	// Zero subscriptions is a legitimate baseline and must survive as a
	// present field, not be omitted by omitempty.
	baselineJSON, err := json.Marshal(capture.eventByName("revenue_mrr_baseline_complete").Revenue)
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	if !strings.Contains(string(baselineJSON), `"expectedActiveSubscriptions":0`) {
		t.Errorf("baseline JSON lost zero expectedActiveSubscriptions: %s", baselineJSON)
	}
}

func TestTrackRevenueDropsInvalidPayload(t *testing.T) {
	client, capture := newTestClient(t)

	invalid := Revenue{Version: 1, Kind: "transaction", FactID: "fact-x", Amount: "not-a-decimal", Currency: "USD"}

	client.TrackRevenue(context.Background(), invalid, nil)
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if capture.count() != 0 {
		t.Fatalf("invalid revenue produced %d requests, want 0", capture.count())
	}
}

// TestShutdownDrainsQueuedEvents pins the no-loss contract: shutdown closes
// the queue, drains it, and every enqueued event arrives exactly once —
// without an explicit flush.
func TestShutdownDrainsQueuedEvents(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(1000))

	const total = 50
	for i := 0; i < total; i++ {
		client.Track(context.Background(), "drain_me", Props{"i": i})
	}
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	events := capture.events()
	if len(events) != total {
		t.Fatalf("after shutdown %d of %d events arrived — the drain lost some", len(events), total)
	}
	seen := map[string]int{}
	for _, event := range events {
		seen[event.EventID]++
		if seen[event.EventID] > 1 {
			t.Fatalf("event %s arrived twice", event.EventID)
		}
	}
	stats := client.Stats()
	if stats.Enqueued != total || stats.Delivered != total {
		t.Errorf("stats = %+v, want enqueued=delivered=%d", stats, total)
	}
}

// TestShutdownReportsLostBatch proves permanently failed sends are reported
// at shutdown instead of being silently forgotten. Here the events are still
// queued when shutdown starts, so the drain's own failure surfaces directly;
// TestBatcherWaitDoneReportsResult covers losses from earlier background
// sends.
func TestShutdownReportsLostBatch(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(2), WithMaxRetries(0))
	capture.failForever()

	client.Track(context.Background(), "doomed_a", nil)
	client.Track(context.Background(), "doomed_b", nil)
	shutdownErr := client.Shutdown(context.Background())
	if shutdownErr == nil {
		t.Fatal("Shutdown returned nil although every send failed permanently")
	}
	if !strings.Contains(shutdownErr.Error(), "500") {
		t.Errorf("Shutdown error = %v, want it to name the endpoint rejection", shutdownErr)
	}
	if stats := client.Stats(); stats.Failed != 2 || stats.Delivered != 0 {
		t.Fatalf("stats = %+v, want both events failed", stats)
	}
	if capture.count() == 0 {
		t.Fatalf("the failing server saw no attempts at all")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	client, _ := newTestClient(t)
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestFlushAfterShutdownReportsClosed(t *testing.T) {
	client, _ := newTestClient(t)
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := client.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after Shutdown = %v, want ErrClosed", err)
	}
}

func TestTrackAfterShutdownIsDroppedNotPanicking(t *testing.T) {
	client, capture := newTestClient(t)
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	enqueuedBefore := client.Stats().Enqueued

	client.Track(context.Background(), "too_late", nil)
	if got := client.Stats().Enqueued; got != enqueuedBefore {
		t.Fatalf("track after shutdown changed enqueued from %d to %d", enqueuedBefore, got)
	}
	if capture.count() != 0 {
		t.Fatalf("track after shutdown sent a request")
	}
}

// TestShutdownDeadlineReportsUndelivered proves Shutdown never returns nil
// over events it failed to confirm: with the endpoint wedged, the ctx
// deadline produces an UndeliveredError naming the unconfirmed count.
func TestShutdownDeadlineReportsUndelivered(t *testing.T) {
	capture := newCaptureServer(t)
	capture.block()

	client, err := New("pk_test_key",
		WithEndpoint(capture.url()),
		WithFlushInterval(0),
		WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	client.Track(context.Background(), "wedged_1", nil)
	client.Track(context.Background(), "wedged_2", nil)
	time.Sleep(20 * time.Millisecond) // let the loop pick the events up

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownErr := client.Shutdown(ctx)
	if shutdownErr == nil {
		t.Fatal("Shutdown returned nil while the endpoint was wedged and events were unconfirmed")
	}
	var undelivered *UndeliveredError
	if !errors.As(shutdownErr, &undelivered) {
		t.Fatalf("Shutdown error = %v, want UndeliveredError", shutdownErr)
	}
	if undelivered.Undelivered < 1 {
		t.Errorf("undelivered count = %d, want at least 1", undelivered.Undelivered)
	}
	if !strings.Contains(undelivered.Error(), "not yet delivered") || !errors.Is(undelivered, context.DeadlineExceeded) {
		t.Errorf("UndeliveredError message/unwrap incomplete: %q", undelivered.Error())
	}

	// The background drain keeps trying; releasing the endpoint lets it land.
	capture.unblock()
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("final Shutdown: %v", err)
	}
	waitForRequests(t, capture, 1)
}

// TestConcurrentUseIsRaceFree exercises interleaved Track/Flush/Shutdown from
// many goroutines; run under -race this is the concurrency regression gate.
func TestConcurrentUseIsRaceFree(t *testing.T) {
	client, capture := newTestClient(t, WithFlushSize(7))

	const workers = 16
	const perWorker = 40

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				client.Track(context.Background(), "parallel_track", Props{"worker": worker, "i": i})
				if i%10 == 9 {
					_ = client.Flush(context.Background())
				}
			}
		}(w)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Flush(context.Background())
		}()
	}

	wg.Wait()

	total := workers * perWorker
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	stats := client.Stats()
	if stats.Dropped != 0 || stats.Failed != 0 {
		t.Errorf("stats = %+v, want no drops or failures at queue size 1000", stats)
	}
	if stats.Delivered != int64(total) {
		t.Errorf("delivered %d of %d events (enqueued=%d)", stats.Delivered, total, stats.Enqueued)
	}
	delivered := len(capture.events())
	if delivered != total {
		t.Errorf("capture saw %d events, want %d", delivered, total)
	}
	seen := make(map[string]int, total)
	for _, event := range capture.events() {
		seen[event.EventID]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("event %s arrived %d times", id, n)
		}
	}
}

func TestQueueOverflowInvariants(t *testing.T) {
	client, _ := newTestClient(t,
		WithMaxQueueSize(4),
		WithFlushSize(1000),
	)

	// No trigger can fire (size too large, timer off), so most of these stay
	// queued; the exact split between queued and dropped depends on how fast
	// the loop drains, but conservation must hold.
	const total = 30
	for i := 0; i < total; i++ {
		client.Track(context.Background(), "overflow", Props{"i": i})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	stats := client.Stats()
	if stats.Enqueued+stats.Dropped != total {
		t.Errorf("enqueued %d + dropped %d = %d, want every one of %d events accounted for",
			stats.Enqueued, stats.Dropped, stats.Enqueued+stats.Dropped, total)
	}
	if stats.Failed != 0 {
		t.Errorf("failed = %d, want 0 against a healthy endpoint", stats.Failed)
	}
}

func TestDebugLoggingExplainsDrops(t *testing.T) {
	var buffer bytes.Buffer
	log.SetOutput(&buffer)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	client, _ := newTestClient(t, WithDebug(true))
	client.Track(context.Background(), "", nil) // dropped with a diagnostic
	if err := client.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if !strings.Contains(buffer.String(), "[Alitycs]") {
		t.Errorf("debug output missing [Alitycs] prefix: %q", buffer.String())
	}
	if !strings.Contains(buffer.String(), "dropped") {
		t.Errorf("debug output should explain the drop: %q", buffer.String())
	}
}

func waitForRequests(t *testing.T, capture *captureServer, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if capture.count() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d requests; have %d", n, capture.count())
}
