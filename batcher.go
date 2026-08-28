package alitycs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by Flush after Shutdown has been called.
var ErrClosed = errors.New("alitycs: client is shut down")

// LostEventsError reports events lost to permanently failed background sends
// earlier in the client's life, surfaced by Shutdown so a silent loss can
// never go unnoticed at exit. Cause carries the most recent send failure.
// Rejected counts events refused locally at build time for violating the
// canonical ingestion limits.
type LostEventsError struct {
	Lost     int64
	Rejected int64
	Cause    error
}

func (e *LostEventsError) Error() string {
	message := fmt.Sprintf("alitycs: %d events were lost to failed sends", e.Lost)
	if e.Rejected > 0 {
		message = fmt.Sprintf("%s and %d were rejected locally for violating ingestion limits", message, e.Rejected)
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", message, e.Cause)
	}
	return message
}

func (e *LostEventsError) Unwrap() error { return e.Cause }

// flushRequest asks the loop goroutine to send everything currently queued
// and report the outcome on the reply channel. Its ctx bounds both the wait
// and the sends the drain performs: cancelling it aborts those sends like any
// other caller cancellation.
type flushRequest struct {
	reply chan error
	ctx   context.Context
}

// inbound pairs an event with the context of the call that enqueued it: when
// that call completes a full batch, the size-triggered send runs under it.
type inbound struct {
	event Event
	ctx   context.Context
}

// batcher owns the event queue. Exactly one goroutine (loop) touches pending
// and issues sends, so no mutex protects them: events enter through a
// buffered channel, flushes through flushRequests, and shutdown through
// stopCh. The channel ordering gives Flush its contract — a flush request is
// queued behind every event enqueued before it, so when its cycle runs those
// events have already been sent.
type batcher struct {
	events        chan inbound
	flushRequests chan flushRequest
	stopCh        chan struct{}
	doneCh        chan struct{}

	send           func(ctx context.Context, payload *BatchPayload) error
	recover        func(ctx context.Context) error
	durablePending func() int
	durable        bool
	flushSize      int
	interval       time.Duration
	queueLimit     int
	debug          bool

	enqueued  atomic.Int64 // accepted into the queue
	delivered atomic.Int64 // accepted by the endpoint
	dropped   atomic.Int64 // rejected because the queue was full
	rejected  atomic.Int64 // refused locally for violating ingestion limits
	failed    atomic.Int64 // exhausted retries in a background send
	unsent    atomic.Int64 // in the channel or in pending, awaiting an outcome

	pending   []Event // owned by the loop goroutine only
	lastCause error   // most recent send failure; read only after doneCh closes
}

func newBatcher(
	cfg *config,
	send func(ctx context.Context, payload *BatchPayload) error,
	recoverFn func(ctx context.Context) error,
	durablePending func() int,
	durable bool,
) *batcher {
	return &batcher{
		events:         make(chan inbound, cfg.maxQueueSize),
		flushRequests:  make(chan flushRequest, defaultMaxQueueSize),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		send:           send,
		recover:        recoverFn,
		durablePending: durablePending,
		durable:        durable,
		flushSize:      cfg.flushSize,
		interval:       cfg.flushInterval,
		queueLimit:     cfg.maxQueueSize,
		debug:          cfg.debug,
	}
}

// start launches the loop goroutine.
func (b *batcher) start() {
	go b.loop()
}

// enqueue queues one event under the enqueuing call's ctx (nil means
// background). It reports false when the unsent-event budget is exhausted —
// the event is dropped rather than blocking the caller.
func (b *batcher) enqueue(event Event, ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if b.unsent.Load() >= int64(b.queueLimit) {
		b.dropped.Add(1)
		debugLog(b.debug, "queue full (%d) — dropping event %s", b.queueLimit, event.EventID)
		return false
	}
	select {
	case b.events <- inbound{event: event, ctx: ctx}:
		b.enqueued.Add(1)
		b.unsent.Add(1)
		return true
	default:
		b.dropped.Add(1)
		debugLog(b.debug, "queue channel full — dropping event %s", event.EventID)
		return false
	}
}

// flush waits until everything enqueued before this call has been sent. The
// request travels through the same channel as the events themselves, so its
// cycle observes all of them. The drain's sends run under ctx: cancellation
// aborts them, not just the wait.
func (b *batcher) flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	request := flushRequest{reply: make(chan error, 1), ctx: ctx}
	select {
	case b.flushRequests <- request:
	case <-b.doneCh:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-request.reply:
		return err
	case <-b.doneCh:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stop signals the loop to drain everything queued and exit; waitDone joins it.
func (b *batcher) stop() {
	close(b.stopCh)
}

// waitDone blocks until the loop goroutine has exited and reports whether
// every enqueued event reached the endpoint. Losses — whether from earlier
// background sends or from the shutdown drain itself — surface uniformly as
// LostEventsError wrapping the most recent cause; locally rejected events are
// reported on the same error.
func (b *batcher) waitDone(ctx context.Context) error {
	select {
	case <-b.doneCh:
		if pending := b.durablePending(); pending > 0 {
			return &UndeliveredError{Undelivered: pending, Cause: b.lastCause}
		}
		failed := b.failed.Load()
		rejected := b.rejected.Load()
		if failed > 0 || rejected > 0 {
			return &LostEventsError{Lost: failed, Rejected: rejected, Cause: b.lastCause}
		}
		return nil
	case <-ctx.Done():
		return &UndeliveredError{
			Undelivered: int(b.enqueued.Load() - b.delivered.Load()),
			Cause:       ctx.Err(),
		}
	}
}

// reject records an event refused at build time for violating ingestion
// limits. It never enters the queue, so nothing downstream can send it.
func (b *batcher) reject(eventName string, err error) {
	b.rejected.Add(1)
	warnLog("event %q rejected locally and not queued: %v", eventName, err)
}

// counters exposes the delivery counters atomically.
type batcherCounters struct {
	Enqueued  int64
	Delivered int64
	Dropped   int64
	Rejected  int64
	Failed    int64
}

func (b *batcher) counters() batcherCounters {
	return batcherCounters{
		Enqueued:  b.enqueued.Load(),
		Delivered: b.delivered.Load(),
		Dropped:   b.dropped.Load(),
		Rejected:  b.rejected.Load(),
		Failed:    b.failed.Load(),
	}
}

func (b *batcher) loop() {
	defer func() {
		b.replyPendingFlushes()
		close(b.doneCh)
	}()

	var timerC <-chan time.Time
	if b.interval > 0 {
		timer := time.NewTicker(b.interval)
		defer timer.Stop()
		timerC = timer.C
	}

	for {
		select {
		case <-b.stopCh:
			// The shutdown drain keeps a fresh context: waitDone's ctx
			// bounds only how long the caller waits, not the drain.
			b.finish(context.Background())
			return
		case in := <-b.events:
			b.pending = append(b.pending, in.event)
			if len(b.pending) >= b.flushSize {
				// A size trigger dispatches exactly its threshold so
				// batches keep the configured shape even while more
				// events stream in; anything extra waits its turn. The
				// send answers to the call that completed the batch —
				// its ctx can cancel or deadline the dispatch.
				if err := b.recover(in.ctx); err != nil {
					b.lastCause = err
					break
				}
				chunk := b.pending[:b.flushSize]
				b.pending = b.pending[b.flushSize:]
				b.sendChunk(in.ctx, chunk)
			}
		case <-timerC:
			// No caller is attached to a timer tick, so the flush runs
			// on a fresh background context, immune to cancellations.
			b.sendPending(context.Background())
		case request := <-b.flushRequests:
			b.drainChannel()
			request.reply <- b.sendPending(request.ctx)
		}
	}
}

// finish drains everything still queued after stop and sends it in batches of
// flushSize. Losses are counted by sendChunk and reported through waitDone.
func (b *batcher) finish(ctx context.Context) {
	if err := b.recover(ctx); err != nil {
		b.lastCause = err
		return
	}
	for {
		b.drainChannel()
		if len(b.pending) == 0 {
			break
		}
		size := b.flushSize
		if size > len(b.pending) {
			size = len(b.pending)
		}
		chunk := b.pending[:size]
		b.pending = b.pending[size:]
		if err := b.sendChunk(ctx, chunk); err != nil {
			debugLog(b.debug, "shutdown lost %d events: %v", len(chunk), err)
		}
	}
}

// drainChannel moves every queued event into pending without blocking,
// stopping at the unsent-event budget so maxQueueSize stays meaningful even
// when sends lag behind. Called from the loop goroutine only.
func (b *batcher) drainChannel() {
	for len(b.pending) < b.queueLimit {
		select {
		case in := <-b.events:
			b.pending = append(b.pending, in.event)
		default:
			return
		}
	}
}

// sendPending drains the channel and sends everything in flushSize-sized
// chunks — the same shape the size trigger and shutdown drain produce. An
// empty queue sends nothing; flushes never fabricate empty requests. The
// first terminal failure is returned after every chunk has had its turn.
func (b *batcher) sendPending(ctx context.Context) error {
	if err := b.recover(ctx); err != nil {
		b.lastCause = err
		return err
	}
	b.drainChannel()
	var firstErr error
	for len(b.pending) > 0 {
		size := b.flushSize
		if size > len(b.pending) {
			size = len(b.pending)
		}
		chunk := b.pending[:size]
		b.pending = b.pending[size:]
		if err := b.sendChunk(ctx, chunk); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// maxSplitDepth bounds the recursion of whole-batch rejection splitting; deep
// enough for ~2^32 events, far beyond any queue limit.
const maxSplitDepth = 32

// batchRejectStatus is the HTTP status the ingest endpoint answers with when it
// rejects an entire batch over a single invalid event.
const batchRejectStatus = http.StatusBadRequest

// sendChunk sends one payload and resolves its fate.
func (b *batcher) sendChunk(ctx context.Context, events []Event) error {
	return b.deliver(ctx, events, 0)
}

// deliver sends one payload and resolves its fate. On an HTTP 400 the server
// rejected the whole batch — possibly over a single invalid event — so the
// chunk is split in half and each half retried recursively until only valid
// singles remain. Any other failure counts the chunk as lost: retries already
// ran inside send, and re-queueing a refused event would poison future batches.
func (b *batcher) deliver(ctx context.Context, events []Event, depth int) error {
	payload := &BatchPayload{
		BatchID: prefixBatch + generateID(),
		SentAt:  nowMillis(),
		Events:  events,
	}
	err := b.send(ctx, payload)
	if err == nil {
		b.unsent.Add(-int64(len(events))) // the outcome resolved either way
		b.delivered.Add(int64(len(events)))
		return nil
	}

	var terminal *terminalStatusError
	if errors.As(err, &terminal) && terminal.status == batchRejectStatus &&
		len(events) > 1 && depth < maxSplitDepth {
		warnLog("batch %s rejected whole (HTTP %d) — splitting %d events in half and retrying",
			payload.BatchID, terminal.status, len(events))
		mid := len(events) / 2
		leftErr := b.deliver(ctx, events[:mid], depth+1)
		rightErr := b.deliver(ctx, events[mid:], depth+1)
		if leftErr != nil {
			return leftErr
		}
		return rightErr
	}

	b.unsent.Add(-int64(len(events)))
	var retained *durableBatchError
	if !errors.As(err, &retained) {
		b.failed.Add(int64(len(events)))
	}
	b.lastCause = err
	if errors.As(err, &retained) {
		debugLog(b.debug, "batch %s failed after retries — retaining %d events for restart: %v", payload.BatchID, len(events), err)
	} else {
		debugLog(b.debug, "batch %s failed after retries — dropping %d events: %v", payload.BatchID, len(events), err)
	}
	return err
}

func (b *batcher) replyPendingFlushes() {
	for {
		select {
		case request := <-b.flushRequests:
			request.reply <- ErrClosed
		default:
			return
		}
	}
}
