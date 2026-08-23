package alitycs

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by Flush after Shutdown has been called.
var ErrClosed = errors.New("alitycs: client is shut down")

// LostEventsError reports events lost to permanently failed background sends
// earlier in the client's life, surfaced by Shutdown so a silent loss can
// never go unnoticed at exit. Cause carries the most recent send failure.
type LostEventsError struct {
	Lost  int64
	Cause error
}

func (e *LostEventsError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("alitycs: %d events were lost to failed sends: %v", e.Lost, e.Cause)
	}
	return fmt.Sprintf("alitycs: %d events were lost to failed sends", e.Lost)
}

func (e *LostEventsError) Unwrap() error { return e.Cause }

// flushRequest asks the loop goroutine to send everything currently queued
// and report the outcome on the reply channel.
type flushRequest struct {
	reply chan error
}

// batcher owns the event queue. Exactly one goroutine (loop) touches pending
// and issues sends, so no mutex protects them: events enter through a
// buffered channel, flushes through flushRequests, and shutdown through
// stopCh. The channel ordering gives Flush its contract — a flush request is
// queued behind every event enqueued before it, so when its cycle runs those
// events have already been sent.
type batcher struct {
	events        chan Event
	flushRequests chan flushRequest
	stopCh        chan struct{}
	doneCh        chan struct{}

	send       func(ctx context.Context, payload *BatchPayload) error
	flushSize  int
	interval   time.Duration
	queueLimit int
	debug      bool

	enqueued  atomic.Int64 // accepted into the queue
	delivered atomic.Int64 // accepted by the endpoint
	dropped   atomic.Int64 // rejected because the queue was full
	failed    atomic.Int64 // exhausted retries in a background send
	unsent    atomic.Int64 // in the channel or in pending, awaiting an outcome

	pending   []Event // owned by the loop goroutine only
	lastCause error   // most recent send failure; read only after doneCh closes
}

func newBatcher(cfg *config, send func(ctx context.Context, payload *BatchPayload) error) *batcher {
	return &batcher{
		events:        make(chan Event, cfg.maxQueueSize),
		flushRequests: make(chan flushRequest, defaultMaxQueueSize),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		send:          send,
		flushSize:     cfg.flushSize,
		interval:      cfg.flushInterval,
		queueLimit:    cfg.maxQueueSize,
		debug:         cfg.debug,
	}
}

// start launches the loop goroutine.
func (b *batcher) start() {
	go b.loop()
}

// enqueue queues one event. It reports false when the unsent-event budget is
// exhausted — the event is dropped rather than blocking the caller.
func (b *batcher) enqueue(event Event) bool {
	if b.unsent.Load() >= int64(b.queueLimit) {
		b.dropped.Add(1)
		debugLog(b.debug, "queue full (%d) — dropping event %s", b.queueLimit, event.EventID)
		return false
	}
	select {
	case b.events <- event:
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
// cycle observes all of them.
func (b *batcher) flush(ctx context.Context) error {
	request := flushRequest{reply: make(chan error, 1)}
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
// LostEventsError wrapping the most recent cause.
func (b *batcher) waitDone(ctx context.Context) error {
	select {
	case <-b.doneCh:
		if failed := b.failed.Load(); failed > 0 {
			return &LostEventsError{Lost: failed, Cause: b.lastCause}
		}
		return nil
	case <-ctx.Done():
		return &UndeliveredError{
			Undelivered: int(b.enqueued.Load() - b.delivered.Load()),
			Cause:       ctx.Err(),
		}
	}
}

// counters exposes the delivery counters atomically.
type batcherCounters struct {
	Enqueued  int64
	Delivered int64
	Dropped   int64
	Failed    int64
}

func (b *batcher) counters() batcherCounters {
	return batcherCounters{
		Enqueued:  b.enqueued.Load(),
		Delivered: b.delivered.Load(),
		Dropped:   b.dropped.Load(),
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
			b.finish(context.Background())
			return
		case event := <-b.events:
			b.pending = append(b.pending, event)
			if len(b.pending) >= b.flushSize {
				// A size trigger dispatches exactly its threshold so
				// batches keep the configured shape even while more
				// events stream in; anything extra waits its turn.
				chunk := b.pending[:b.flushSize]
				b.pending = b.pending[b.flushSize:]
				b.sendChunk(context.Background(), chunk)
			}
		case <-timerC:
			b.sendPending(context.Background())
		case request := <-b.flushRequests:
			b.drainChannel()
			request.reply <- b.sendPending(context.Background())
		}
	}
}

// finish drains everything still queued after stop and sends it in batches of
// flushSize. Losses are counted by sendChunk and reported through waitDone.
func (b *batcher) finish(ctx context.Context) {
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
		case event := <-b.events:
			b.pending = append(b.pending, event)
		default:
			return
		}
	}
}

// sendPending drains the channel and sends everything as one batch. An empty
// queue sends nothing — flushes never fabricate empty requests.
func (b *batcher) sendPending(ctx context.Context) error {
	b.drainChannel()
	if len(b.pending) == 0 {
		return nil
	}
	events := b.pending
	b.pending = nil
	return b.sendChunk(ctx, events)
}

func (b *batcher) sendChunk(ctx context.Context, events []Event) error {
	payload := &BatchPayload{
		BatchID: prefixBatch + generateID(),
		SentAt:  nowMillis(),
		Events:  events,
	}
	err := b.send(ctx, payload)
	b.unsent.Add(-int64(len(events))) // the outcome resolved either way
	if err != nil {
		b.failed.Add(int64(len(events)))
		b.lastCause = err
		debugLog(b.debug, "batch %s failed after retries — dropping %d events: %v", payload.BatchID, len(events), err)
		return err
	}
	b.delivered.Add(int64(len(events)))
	return nil
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
