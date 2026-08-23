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
// The capability surface is intentionally limited to what Alitycs offers:
// track, identify/reset, page, error capture, revenue ingestion, global
// properties, and flush/shutdown. Feature flags, session recording, group
// analytics and log ingestion are not part of this SDK.
package alitycs

const (
	// Version is the SDK version reported in event context.
	Version = "1.0.0"
)
