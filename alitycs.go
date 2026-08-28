package alitycs

import (
	"context"
	"fmt"
	"sync"
)

// Client emits analytics events to Alitycs. Create one with New; every method
// is safe for concurrent use.
type Client struct {
	config      *config
	transporter *transport
	sessions    *sessionManager
	batch       *batcher

	globalsMu sync.RWMutex
	globals   Props

	stateMu   sync.Mutex
	closeOnce sync.Once
	closed    bool
}

// EventOption adjusts a single emitted event.
type EventOption func(*eventOptions)

type eventOptions struct {
	userID string
}

// WithUserID attaches a user ID to this event without identifying the
// session.
func WithUserID(userID string) EventOption {
	return func(o *eventOptions) {
		o.userID = userID
	}
}

// New creates a Client with the given publishable key and options. The
// background batching goroutine starts immediately; call Shutdown when done.
func New(apiKey string, opts ...Option) (*Client, error) {
	if apiKey == "" {
		return nil, ErrAPIKeyRequired
	}
	cfg, err := newConfig(apiKey, opts...)
	if err != nil {
		return nil, err
	}

	store, err := newFileBatchStore(cfg.persistencePath, cfg.maxQueueSize)
	if err != nil {
		return nil, err
	}
	t := &transport{
		endpoint:   cfg.endpoint,
		apiKey:     cfg.apiKey,
		maxRetries: cfg.maxRetries,
		client:     cfg.httpClientOrDefault(),
		debug:      cfg.debug,
		store:      store,
	}
	c := &Client{
		config:      cfg,
		transporter: t,
		sessions:    newSessionManager(cfg.sessionTimeout),
		globals:     make(Props),
	}
	c.batch = newBatcher(cfg, t.send, t.persist, t.recover, store.pendingEvents, store.enabled())
	c.batch.start()
	return c, nil
}

// Track queues a track event. The call never blocks on network I/O; use Flush
// or Shutdown to guarantee delivery. An empty event name is dropped.
//
// The context travels with the event: when this call completes a full batch
// (the queue reaches the flush size), that send runs under ctx — cancelling
// it aborts the batch as failed deliveries. Pass context.Background() unless
// the caller's lifetime should bound delivery; see "Delivery behaviour" in
// the package documentation.
func (c *Client) Track(ctx context.Context, eventName string, properties Props, opts ...EventOption) {
	c.enqueue(ctx, eventTypeTrack, eventName, properties, nil, opts)
}

// CaptureError queues an error event. An empty error name is dropped. Like
// Track, it carries ctx into any size-triggered send this call completes.
func (c *Client) CaptureError(ctx context.Context, errorName string, properties Props, opts ...EventOption) {
	c.enqueue(ctx, eventTypeError, errorName, properties, nil, opts)
}

// Page queues a page view event. An empty name falls back to "page_view".
// Like Track, it carries ctx into any size-triggered send this call completes.
func (c *Client) Page(ctx context.Context, name string, properties Props, opts ...EventOption) {
	if len(name) == 0 {
		name = "page_view"
	}
	c.enqueue(ctx, eventTypePage, name, properties, nil, opts)
}

// Identify marks the session as belonging to userID and queues an identify
// event carrying the traits. An empty userID is ignored. Like Track, it
// carries ctx into any size-triggered send this call completes.
func (c *Client) Identify(ctx context.Context, userID string, traits Props) {
	if userID == "" {
		debugLog(c.config.debug, "identify ignored: userId is required")
		return
	}
	c.sessions.setUserID(userID)

	properties := make(Props, len(traits)+1)
	for key, value := range traits {
		properties[key] = value
	}
	properties["userId"] = userID
	c.enqueue(ctx, eventTypeIdentify, "identify", properties, nil, nil)
}

// TrackRevenue queues a trusted revenue event. The payload must be valid —
// build it with NewTransaction, NewMRRSnapshot or NewMRRBaselineComplete, or
// call Validate on a hand-built Revenue first; invalid payloads are rejected
// locally (warn log + Stats.Rejected), never queued. Like Track, it carries
// ctx into any size-triggered send this call completes.
func (c *Client) TrackRevenue(ctx context.Context, revenue Revenue, properties Props, opts ...EventOption) {
	if err := revenue.Validate(); err != nil {
		c.batch.reject("revenue_"+revenue.Kind, err)
		return
	}
	eventName := "revenue_" + revenue.Kind
	c.enqueue(ctx, eventTypeTrack, eventName, properties, &revenue, opts)
}

// SetGlobalProperties merges properties into every subsequently enqueued
// event. Per-call properties win on key conflicts.
func (c *Client) SetGlobalProperties(properties Props) {
	if len(properties) == 0 {
		return
	}
	c.globalsMu.Lock()
	defer c.globalsMu.Unlock()
	for key, value := range properties {
		c.globals[key] = value
	}
}

// GlobalProperties returns a copy of the current global properties.
func (c *Client) GlobalProperties() Props {
	c.globalsMu.RLock()
	defer c.globalsMu.RUnlock()
	out := make(Props, len(c.globals))
	for key, value := range c.globals {
		out[key] = value
	}
	return out
}

// ClearGlobalProperties removes every global property.
func (c *Client) ClearGlobalProperties() {
	c.globalsMu.Lock()
	defer c.globalsMu.Unlock()
	c.globals = make(Props)
}

// Reset clears the identified user and starts a fresh anonymous identity and
// session. Events enqueued afterwards no longer share IDs with earlier ones.
func (c *Client) Reset() {
	c.sessions.reset()
}

// Flush waits until every event enqueued before this call has been accepted by
// the endpoint, honouring ctx: it bounds both the wait and the sends the drain
// performs — cancelling ctx aborts those sends. It reports the first terminal
// failure if a batch exhausted its retries.
func (c *Client) Flush(ctx context.Context) error {
	return c.batch.flush(ctx)
}

// Shutdown stops the client for good: it signals the batch loop, drains every
// queued event, delivers it, and only then returns. Shutdown is idempotent;
// later calls wait for the same completion.
//
// The ctx deadline is honoured: if it expires before delivery completes,
// Shutdown returns an UndeliveredError saying how many events had not been
// confirmed at that point — it never silently reports success over lost
// events. Drain continues in the background on a fresh context, so events may
// still land.
func (c *Client) Shutdown(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.stateMu.Lock()
		c.closed = true
		c.stateMu.Unlock()
		c.batch.stop()
	})
	return c.batch.waitDone(ctx)
}

// Stats reports delivery counters: events accepted into the queue, confirmed
// delivered by the endpoint, dropped because the queue was full, rejected
// locally for violating ingestion limits, and lost to exhausted retries.
func (c *Client) Stats() Stats {
	counters := c.batch.counters()
	return Stats(counters)
}

// Stats is a snapshot of the client's delivery counters.
type Stats struct {
	Enqueued  int64
	Delivered int64
	Dropped   int64
	Rejected  int64
	Failed    int64
}

// UndeliveredError reports events retained for a later restart or not yet
// confirmed when a shutdown deadline expired.
type UndeliveredError struct {
	Undelivered int
	Cause       error
}

func (e *UndeliveredError) Error() string {
	return fmt.Sprintf("alitycs: shutdown left %d events not yet delivered: %v", e.Undelivered, e.Cause)
}

func (e *UndeliveredError) Unwrap() error { return e.Cause }

// enqueue builds the wire event and hands it to the batcher under the
// enqueuing call's ctx (nil means background). Holding stateMu across the
// enqueue means every event accepted precedes the shutdown signal.
func (c *Client) enqueue(ctx context.Context, eventType eventType, eventName string, properties Props, revenue *Revenue, opts []EventOption) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed {
		debugLog(c.config.debug, "%s dropped: client is shut down", eventName)
		return
	}
	if eventName == "" {
		debugLog(c.config.debug, "%s dropped: event name is required", eventType)
		return
	}

	options := eventOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	session := c.sessions.next()
	serialized, err := c.mergeProperties(properties)
	if err != nil {
		// Rejected locally before the event exists on the wire; surfaced via
		// warn log and the Rejected counter.
		c.batch.reject(eventName, err)
		return
	}
	event := Event{
		EventID:     prefixEvent + generateID(),
		Event:       eventName,
		EventType:   eventType,
		UserID:      options.userID,
		AnonymousID: session.AnonymousID,
		SessionID:   session.ID,
		Timestamp:   nowMillis(),
		Properties:  serialized,
		Revenue:     revenue,
		Context:     collectContext(),
	}
	if session.UserID != "" && options.userID == "" {
		// Identified sessions stamp their user on every event unless the
		// call overrides it. Identify has just set the session user, so its
		// own event carries it too.
		event.UserID = session.UserID
	}
	if err := validateEvent(event); err != nil {
		// Rejected locally: never queued, never sent. The server refuses an
		// entire batch over one invalid event, so sending would poison every
		// other event in it.
		c.batch.reject(eventName, err)
		return
	}
	c.batch.enqueue(event, ctx) // batcher treats a nil ctx as background
}

func (c *Client) mergeProperties(callProps Props) (map[string]string, error) {
	merged := make(Props, len(c.globals)+len(callProps))
	c.globalsMu.RLock()
	for key, value := range c.globals {
		merged[key] = value
	}
	c.globalsMu.RUnlock()
	for key, value := range callProps {
		merged[key] = value
	}
	return serializeProps(merged)
}
