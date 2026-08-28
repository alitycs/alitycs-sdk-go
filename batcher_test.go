package alitycs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	}, send, nil, func(context.Context) error { return nil }, func() int { return 0 }, false)
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

	if !b.enqueue(testEvent("one"), context.Background()) || !b.enqueue(testEvent("two"), context.Background()) {
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

	b.enqueue(testEvent("partial"), context.Background())
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

	b.enqueue(testEvent("timed"), context.Background())
	waitForBatches(t, sender, 1)
}

func TestBatcherStopDeliversEverythingInChunks(t *testing.T) {
	sender := &fakeSend{}
	const total = 7
	b := newTestBatcher(3, 100, sender.send)
	b.start()
	for i := 0; i < total; i++ {
		if !b.enqueue(testEvent("evt"), context.Background()) {
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

func TestBatcherShutdownPersistsAcceptedEventsAfterRecoveryFailure(t *testing.T) {
	recoveryErr := errors.New("existing WAL recovery failed")
	var durablePending atomic.Int64
	var persisted []string
	var sendCalls atomic.Int64
	persist := func(payload *BatchPayload) error {
		for _, event := range payload.Events {
			persisted = append(persisted, event.Event)
		}
		durablePending.Add(int64(len(payload.Events)))
		return nil
	}
	b := newBatcher(
		&config{flushSize: 100, maxQueueSize: 100},
		func(context.Context, *BatchPayload) error {
			sendCalls.Add(1)
			return errors.New("network send must not run after recovery fails")
		},
		persist,
		func(context.Context) error { return recoveryErr },
		func() int { return int(durablePending.Load()) },
		true,
	)
	b.start()
	for _, name := range []string{"first", "second", "third"} {
		if !b.enqueue(testEvent(name), context.Background()) {
			t.Fatalf("enqueue %q rejected within budget", name)
		}
	}
	b.stop()

	err := b.waitDone(context.Background())
	var undelivered *UndeliveredError
	if !errors.As(err, &undelivered) || undelivered.Undelivered != 3 {
		t.Fatalf("waitDone = %v, want UndeliveredError with 3 persisted events", err)
	}
	if sendCalls.Load() != 0 {
		t.Fatalf("network sends = %d, want zero after recovery failure", sendCalls.Load())
	}
	want := []string{"first", "second", "third"}
	if len(persisted) != len(want) {
		t.Fatalf("persisted = %v, want %v", persisted, want)
	}
	for index := range want {
		if persisted[index] != want[index] {
			t.Fatalf("persisted = %v, want FIFO order %v", persisted, want)
		}
	}
}

func TestBatcherShutdownReportsEventsWhenRecoveryAndPersistenceFail(t *testing.T) {
	recoveryErr := errors.New("existing WAL recovery failed")
	persistenceErr := errors.New("persist pending batch failed")
	b := newBatcher(
		&config{flushSize: 100, maxQueueSize: 100},
		func(context.Context, *BatchPayload) error { return nil },
		func(*BatchPayload) error { return persistenceErr },
		func(context.Context) error { return recoveryErr },
		func() int { return 0 },
		true,
	)
	b.start()
	for _, name := range []string{"first", "second"} {
		if !b.enqueue(testEvent(name), context.Background()) {
			t.Fatalf("enqueue %q rejected within budget", name)
		}
	}
	b.stop()

	err := b.waitDone(context.Background())
	var lost *LostEventsError
	if !errors.As(err, &lost) || lost.Lost != 2 || !errors.Is(err, persistenceErr) {
		t.Fatalf("waitDone = %v, want LostEventsError with 2 events and persistence cause", err)
	}
}

func TestBatcherShutdownReportsRetainedAndLostEventsTogether(t *testing.T) {
	recoveryErr := errors.New("existing WAL recovery failed")
	persistenceErr := errors.New("persist pending batch failed")
	var durablePending atomic.Int64
	durablePending.Store(1)
	b := newBatcher(
		&config{flushSize: 100, maxQueueSize: 100},
		func(context.Context, *BatchPayload) error { return nil },
		func(*BatchPayload) error { return persistenceErr },
		func(context.Context) error { return recoveryErr },
		func() int { return int(durablePending.Load()) },
		true,
	)
	b.start()
	for _, name := range []string{"first", "second"} {
		if !b.enqueue(testEvent(name), context.Background()) {
			t.Fatalf("enqueue %q rejected within budget", name)
		}
	}
	b.stop()

	err := b.waitDone(context.Background())
	var undelivered *UndeliveredError
	if !errors.As(err, &undelivered) || undelivered.Undelivered != 1 {
		t.Fatalf("waitDone = %v, want UndeliveredError with 1 retained event", err)
	}
	var lost *LostEventsError
	if !errors.As(err, &lost) || lost.Lost != 2 || !errors.Is(err, persistenceErr) {
		t.Fatalf("waitDone = %v, want LostEventsError with 2 lost events", err)
	}
}

func TestBatcherBudgetRejectsBeyondLimit(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(100, 2, sender.send) // no loop started: nothing drains

	if !b.enqueue(testEvent("a"), context.Background()) || !b.enqueue(testEvent("b"), context.Background()) {
		t.Fatal("first two enqueues must fit the budget")
	}
	for i := 0; i < 3; i++ {
		if b.enqueue(testEvent("c"), context.Background()) {
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
	if !b.enqueue(testEvent("d"), context.Background()) {
		t.Fatal("enqueue after a resolved outcome should fit")
	}
}

func TestBatcherClassifiesCommittedPersistenceOutcomes(t *testing.T) {
	event := testEvent("committed")

	t.Run("retained put", func(t *testing.T) {
		cause := errors.New("post-commit put sync failed")
		b := newTestBatcher(1, 10, func(context.Context, *BatchPayload) error {
			return &durableBatchError{cause: cause}
		})
		b.unsent.Store(1)
		if err := b.sendChunk(context.Background(), []Event{event}); !errors.Is(err, cause) {
			t.Fatalf("sendChunk = %v, want retained cause", err)
		}
		if counters := b.counters(); counters.Delivered != 0 || counters.Failed != 0 {
			t.Fatalf("counters = %+v, want retained without delivery or loss", counters)
		}
		if b.durableCurrent.Load() != 1 || b.unsent.Load() != 0 {
			t.Fatalf("durableCurrent = %d, unsent = %d; want 1, 0", b.durableCurrent.Load(), b.unsent.Load())
		}
	})

	t.Run("delivered acknowledgement", func(t *testing.T) {
		cause := errors.New("post-commit acknowledgement sync failed")
		b := newTestBatcher(1, 10, func(context.Context, *BatchPayload) error {
			return &deliveredBatchError{cause: cause}
		})
		b.unsent.Store(1)
		if err := b.sendChunk(context.Background(), []Event{event}); !errors.Is(err, cause) {
			t.Fatalf("sendChunk = %v, want acknowledgement cause", err)
		}
		if counters := b.counters(); counters.Delivered != 1 || counters.Failed != 0 {
			t.Fatalf("counters = %+v, want one delivered and none failed", counters)
		}
		if b.unsent.Load() != 0 {
			t.Fatalf("unsent = %d, want 0", b.unsent.Load())
		}
	})
}

func TestBatcherRecoveryProgressResolvesCurrentDurableEvents(t *testing.T) {
	b := newTestBatcher(1, 10, func(context.Context, *BatchPayload) error { return nil })
	b.enqueued.Store(2)
	b.durableCurrent.Store(2)
	b.recordRecoveryProgress(2, 0)
	if counters := b.counters(); counters.Delivered != 2 || counters.Failed != 0 {
		t.Fatalf("counters = %+v, want two recovered deliveries", counters)
	}
	if b.durableCurrent.Load() != 0 {
		t.Fatalf("durableCurrent = %d, want 0", b.durableCurrent.Load())
	}
}

func TestBatcherWaitDoneReportsResult(t *testing.T) {
	sender := &fakeSend{err: errors.New("endpoint down")}
	b := newTestBatcher(2, 100, sender.send)
	b.start()
	if !b.enqueue(testEvent("x"), context.Background()) || !b.enqueue(testEvent("y"), context.Background()) {
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
	b.enqueue(testEvent("x"), context.Background())
	b.enqueue(testEvent("y"), context.Background()) // size trigger fires and blocks inside the send
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

// TestBatcherSendPendingChunksByFlushSize pins the chunking contract: the
// timer and explicit-flush paths must send flushSize-sized batches instead of
// draining everything queued into one payload.
func TestBatcherSendPendingChunksByFlushSize(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(3, 100, sender.send)
	go b.loop()
	defer func() { b.stop(); <-b.doneCh }()

	const total = 7 // 3 + 3 + 1: the final chunk may be smaller
	for i := 0; i < total; i++ {
		if !b.enqueue(testEvent("evt"), context.Background()) {
			t.Fatal("enqueue rejected within budget")
		}
	}
	if err := b.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batches := sender.batches()
	if got := len(batches); got != 3 {
		t.Fatalf("flush produced %d batches, want 3 chunks of flushSize 3", got)
	}
	delivered := 0
	for i, batch := range batches {
		limit := 3
		if i == len(batches)-1 {
			limit = 1
		}
		if len(batch.Events) > 3 || (i < 2 && len(batch.Events) != limit) {
			t.Fatalf("batch %d holds %d events, want exactly %d", i, len(batch.Events), limit)
		}
		delivered += len(batch.Events)
	}
	if delivered != total {
		t.Fatalf("chunks delivered %d of %d events", delivered, total)
	}
}

// TestBatcherWholeBatch400SplitsAndDeliversSingles mirrors the server contract:
// an HTTP 400 rejects the entire batch over a single invalid event, so the
// batcher splits in half and retries until valid singles land.
func TestBatcherWholeBatch400SplitsAndDeliversSingles(t *testing.T) {
	sender := &fakeSend{}
	send := func(ctx context.Context, payload *BatchPayload) error {
		if err := sender.send(ctx, payload); err != nil {
			return err
		}
		if len(payload.Events) > 1 {
			return &terminalStatusError{status: batchRejectStatus} // whole-batch rejection
		}
		return nil
	}
	b := newTestBatcher(100, 100, send)
	go b.loop()
	defer func() { b.stop(); <-b.doneCh }()

	names := []string{"a", "b", "c", "d"}
	for _, name := range names {
		if !b.enqueue(testEvent(name), context.Background()) {
			t.Fatal("enqueue rejected within budget")
		}
	}
	if err := b.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batches := sender.batches()
	sizes := make([]int, 0, len(batches))
	var singles []string
	for _, batch := range batches {
		sizes = append(sizes, len(batch.Events))
		if len(batch.Events) == 1 {
			singles = append(singles, batch.Events[0].Event)
		}
	}
	if len(batches) != 7 {
		t.Fatalf("got %d batches (%v), want the full split: one 4-event batch, two 2s, four 1s", len(batches), sizes)
	}
	wantShapes := []int{4, 2, 1, 1, 2, 1, 1}
	for i, want := range wantShapes {
		if sizes[i] != want {
			t.Fatalf("batch shapes %v, want %v", sizes, wantShapes)
		}
	}
	// Recursion visits halves left-to-right, so accepted singles keep queue order.
	if len(singles) != 4 {
		t.Fatalf("got %d singles (%v), want 4", len(singles), singles)
	}
	for i, name := range names {
		if singles[i] != name {
			t.Fatalf("singles = %v, want queue order %v", singles, names)
		}
	}
	counters := b.counters()
	if counters.Delivered != 4 || counters.Failed != 0 {
		t.Fatalf("counters = %+v, want 4 delivered and none failed", counters)
	}
}

func TestBatcherWholeBatch400SplitIsBounded(t *testing.T) {
	sender := &fakeSend{}
	send := func(ctx context.Context, payload *BatchPayload) error {
		if err := sender.send(ctx, payload); err != nil {
			return err
		}
		return &terminalStatusError{status: batchRejectStatus}
	}
	b := newTestBatcher(100, 100, send)
	events := make([]Event, 0, 100)
	for index := 0; index < 100; index++ {
		events = append(events, testEvent(fmt.Sprintf("event-%d", index)))
	}
	b.unsent.Store(int64(len(events)))
	if err := b.sendChunk(context.Background(), events); err == nil {
		t.Fatal("bounded split unexpectedly reported success")
	}
	if got := len(sender.batches()); got != maxSplitSends {
		t.Fatalf("split sent %d requests, want cap %d", got, maxSplitSends)
	}
	counters := b.counters()
	if counters.Failed != 100 || b.unsent.Load() != 0 {
		t.Fatalf("counters = %+v, unsent = %d; want 100 failed and 0 unsent", counters, b.unsent.Load())
	}
}

func TestBatcherLocalRejectionSurfacesInStatsAndShutdown(t *testing.T) {
	sender := &fakeSend{}
	b := newTestBatcher(100, 100, sender.send)

	b.reject("too_big", errors.New("value exceeds 1000 characters"))

	if counters := b.counters(); counters.Rejected != 1 {
		t.Fatalf("counters = %+v, want Rejected=1", counters)
	}
	b.start()
	b.stop()
	err := b.waitDone(context.Background())
	var lost *LostEventsError
	if !errors.As(err, &lost) {
		t.Fatalf("waitDone = %v, want LostEventsError for the locally rejected event", err)
	}
	if lost.Rejected != 1 {
		t.Fatalf("LostEventsError.Rejected = %d, want 1", lost.Rejected)
	}
}
