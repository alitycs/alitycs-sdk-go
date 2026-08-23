package alitycs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeSend records payloads without touching the network.
type fakeSend struct {
	mu       sync.Mutex
	payloads []*BatchPayload
	err      error // returned for every send when set
	block    chan struct{}
}

func (f *fakeSend) send(ctx context.Context, payload *BatchPayload) error {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	f.payloads = append(f.payloads, payload)
	f.mu.Unlock()
	return f.err
}

func (f *fakeSend) batches() []*BatchPayload {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*BatchPayload{}, f.payloads...)
}

func newTestBatcher(flushSize, queueLimit int, send func(ctx context.Context, payload *BatchPayload) error) *batcher {
	return newBatcher(&config{
		flushSize:     flushSize,
		flushInterval: 0,
		maxQueueSize:  queueLimit,
	}, send)
}

func testEvent(name string) Event {
	return Event{
		EventID:     prefixEvent + name,
		Event:       name,
		EventType:   eventTypeTrack,
		AnonymousID: prefixAnon + "test",
		SessionID:   prefixSession + "test",
		Timestamp:   nowMillis(),
		Properties:  map[string]string{},
		Context:     Context{SDKVersion: Version, SDKLanguage: "go"},
	}
}

func TestBatcherSizeTriggerSendsAtThreshold(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(2, 100, sender.send)
	go b.loop()
	defer func() { b.stop(); <-b.doneCh }()

	if !b.enqueue(testEvent("one")) || !b.enqueue(testEvent("two")) {
		t.Fatal("enqueue rejected within budget")
	}

	waitForBatches(t, sender, 1)
	batches := sender.batches()
	if len(batches[0].Events) != 2 {
		t.Fatalf("size-triggered batch holds %d events, want 2", len(batches[0].Events))
	}
}

func TestBatcherExplicitFlushDrainsPartialGroup(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(25, 100, sender.send)
	go b.loop()
	defer func() { b.stop(); <-b.doneCh }()

	b.enqueue(testEvent("partial"))
	if err := b.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if batches := sender.batches(); len(batches) != 1 || len(batches[0].Events) != 1 {
		t.Fatalf("flush produced %+v, want one batch with the queued event", batches)
	}

	// Empty flush sends nothing.
	before := len(sender.batches())
	if err := b.flush(context.Background()); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if got := len(sender.batches()); got != before {
		t.Fatalf("empty flush produced %d extra batches", got-before)
	}
}

func TestBatcherTimerFlushesPartialGroup(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(25, 100, sender.send)
	b.interval = 5 * time.Millisecond
	go b.loop()
	defer func() { b.stop(); <-b.doneCh }()

	b.enqueue(testEvent("timed"))
	waitForBatches(t, sender, 1)
}

func TestBatcherStopDeliversEverythingInChunks(t *testing.T) {
	sender := &fakeSend{}
	const total = 7
	b := newTestBatcher(3, 100, sender.send)
	b.start()
	for i := 0; i < total; i++ {
		if !b.enqueue(testEvent("evt")) {
			t.Fatal("enqueue rejected within budget")
		}
	}
	b.stop()
	if err := b.waitDone(context.Background()); err != nil {
		t.Fatalf("waitDone: %v", err)
	}

	delivered := 0
	for _, batch := range sender.batches() {
		delivered += len(batch.Events)
		if len(batch.Events) > 3 {
			t.Fatalf("chunk of %d exceeds flush size 3", len(batch.Events))
		}
	}
	if delivered != total {
		t.Fatalf("stop delivered %d of %d events — lost in drain", delivered, total)
	}
}

func TestBatcherBudgetRejectsBeyondLimit(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(100, 2, sender.send) // no loop started: nothing drains

	if !b.enqueue(testEvent("a")) || !b.enqueue(testEvent("b")) {
		t.Fatal("first two enqueues must fit the budget")
	}
	for i := 0; i < 3; i++ {
		if b.enqueue(testEvent("c")) {
			t.Fatal("enqueue beyond budget succeeded")
		}
	}
	counters := b.counters()
	if counters.Enqueued != 2 || counters.Dropped != 3 {
		t.Fatalf("counters = %+v, want enqueued 2 dropped 3", counters)
	}

	// Resolving outcomes frees budget: deliver one of the two drained events
	// through the real send path and the next enqueue fits again.
	b.drainChannel()
	if len(b.pending) != 2 {
		t.Fatalf("drain moved %d events, want 2", len(b.pending))
	}
	if err := b.sendChunk(context.Background(), b.pending[:1]); err != nil {
		t.Fatalf("sendChunk: %v", err)
	}
	if !b.enqueue(testEvent("d")) {
		t.Fatal("enqueue after a resolved outcome should fit")
	}
}

func TestBatcherWaitDoneReportsResult(t *testing.T) {
	sender := &fakeSend{err: errors.New("endpoint down")}
	b := newTestBatcher(2, 100, sender.send)
	b.start()
	if !b.enqueue(testEvent("x")) || !b.enqueue(testEvent("y")) {
		t.Fatal("enqueue rejected within budget")
	}
	b.stop()

	err := b.waitDone(context.Background())
	if !errors.Is(err, sender.err) {
		t.Fatalf("waitDone = %v, want the send failure surfaced", err)
	}
	var lost *LostEventsError
	if !errors.As(err, &lost) || lost.Lost != 2 {
		t.Fatalf("waitDone = %v, want LostEventsError with 2 events", err)
	}
}

// TestBatcherWaitDoneHonoursContextCancellation pins the reporting contract:
// a cancelled wait returns UndeliveredError instead of nil or blocking forever.
func TestBatcherWaitDoneHonoursContextCancellation(t *testing.T) {
	sender := &fakeSend{block: make(chan struct{})}
	b := newTestBatcher(2, 100, sender.send)
	b.enqueue(testEvent("x"))
	b.enqueue(testEvent("y")) // size trigger fires and blocks inside the send
	b.start()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- b.waitDone(ctx) }()
	select {
	case err := <-done:
		var undelivered *UndeliveredError
		if !errors.As(err, &undelivered) {
			t.Fatalf("cancelled waitDone = %v, want UndeliveredError", err)
		}
		if undelivered.Undelivered < 1 {
			t.Fatalf("undelivered = %d, want at least 1", undelivered.Undelivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitDone ignored ctx cancellation")
	}

	close(sender.block)
	b.stop()
	<-b.doneCh
}

func TestBatcherFlushAfterStopReturnsClosed(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(2, 100, sender.send)
	b.start()
	b.stop()
	<-b.doneCh

	if err := b.flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("flush after stop = %v, want ErrClosed", err)
	}
}

func waitForBatches(t *testing.T, sender *fakeSend, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(sender.batches()) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d batches; have %d", n, len(sender.batches()))
}
