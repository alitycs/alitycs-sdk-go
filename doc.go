// Package alitycs provides the Go SDK for the Alitycs Analytics Platform.
//
// The client batches analytics events and POSTs them to the worker ingest
// endpoint (https://api.alitycs.com/events by default) with a publishable key:
//
//	client, err := alitycs.New("pk_live_...",
//	    alitycs.WithEndpoint(url), alitycs.WithFlushSize(20))
//	if err != nil {
//	    return err
//	}
//	defer client.Shutdown(context.Background()) //nolint:errcheck // logged by the SDK in debug mode
//
//	client.Track(ctx, "signup_completed", alitycs.Props{"plan": "free"})
//	client.Identify(ctx, "usr_1842", alitycs.Props{"plan": "pro"})
//	err = client.Flush(ctx)
//
// Every method is safe for concurrent use. Track, Identify, Page, CaptureError
// and TrackRevenue enqueue synchronously and never block on network I/O; a
// single background goroutine owns batching, retrying and sending.
//
// Flush blocks until every event enqueued before the call has been accepted by
// the endpoint. Shutdown stops the client and delivers everything still
// queued; it honours ctx — if the deadline expires first it returns an error
// describing how many events may not have arrived rather than silently
// dropping them.
//
// # Local ingestion limits
//
// The ingest endpoint validates every event against canonical limits (≤50
// properties per event, keys ≤100 chars, values ≤1000 chars, estimated size
// ≤64KB, a non-blank name plus userId or anonymousId, epoch-millisecond
// timestamps no older than 7 days and never in the future) and rejects an
// entire batch over a single violating event. This SDK therefore enforces the
// same limits at build time: an offending event is rejected locally — never
// queued, never sent, never truncated — surfaced through a warn-level log
// (never debug-gated), Stats().Rejected, and Shutdown's LostEventsError.
// Revenue payloads reject cross-kind fields exactly like the server.
//
// # Delivery behaviour
//
// Timer- and flush-triggered dispatch sends flushSize-sized chunks instead of
// one giant payload. When the server answers HTTP 400 — a whole-batch
// rejection — the batch is split in half and each half retried recursively so
// valid events still land. Retries reuse the identical marshalled body so a
// batch keeps its batchId for server-side dedup.
//
// # Contexts
//
// Every enqueueing call carries its ctx into the SDK: when that call completes
// a full batch, the size-triggered send runs under it, so cancelling or
// deadline-expiring ctx aborts the dispatch — the affected events count as
// failed deliveries (Stats().Failed, Shutdown's LostEventsError). Flush bounds
// both its wait and the sends its drain performs the same way. Timer-driven
// dispatch and the shutdown drain deliberately run on a fresh background
// context so background flushing stays immune to unrelated cancellations;
// pass context.Background() from enqueueing calls whose lifetime should not
// bound delivery.
//
// The capability surface is intentionally limited to what Alitycs offers:
// track, identify/reset, page, error capture, revenue ingestion, global
// properties, and flush/shutdown. Feature flags, session recording, group
// analytics and log ingestion are not part of this SDK.
package alitycs

const (
	// Version is the SDK version reported in event context.
	Version = "1.0.0"
)
