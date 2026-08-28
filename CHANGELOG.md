# Changelog

This project follows [Semantic Versioning](https://semver.org/). User-visible changes are recorded
here before a version tag is created.

## [Unreleased]

## [1.0.0] - 2026-08-28

### Added
- Optional `WithPersistence(path)` exact-batch write-ahead logging. A serialized in-flight batch
  is stored atomically before its first attempt and replayed byte-identically after restart,
  including any remaining final `Retry-After` deadline. Terminal responses acknowledge the WAL;
  pre-flush events remain in memory during normal operation and are appended in FIFO order during
  shutdown if recovery of existing WAL records fails.

### Changed
- Retry backoff is now jittered ±20% so many clients retrying after a shared failure do not hit
  the ingest endpoint in lockstep. The overall schedule is unchanged: 1s doubling to a 10s cap.
- The ctx passed to Track, Identify, Page, CaptureError and TrackRevenue is no longer ignored:
  when a call completes a full batch, that size-triggered send runs under its ctx, so cancelling
  or deadline-expiring it aborts the dispatch (the affected events count as failed deliveries via
  Stats().Failed and Shutdown's LostEventsError). Flush(ctx) likewise bounds the sends its drain
  performs, not just its wait. Pass context.Background() from calls whose lifetime should not
  bound delivery; timer-driven dispatch and the shutdown drain stay on a fresh background context.
  Method signatures are unchanged.
- WithHTTPClient now rejects an *http.Client that has no deadline anywhere (zero Timeout on the
  client and no ResponseHeaderTimeout on a *http.Transport): such a client let one wedged
  connection block the single batching goroutine forever. Opaque RoundTripper implementations are
  still accepted as-is. Previously the client was injected as provided.

### Fixed
- A 429 response's `Retry-After` header (delta-seconds or HTTP-date) is now honoured: the retry
  after it waits that long instead of the default backoff, capped at one minute so a malicious or
  malformed response cannot stall indefinitely. Restart recovery defers a still-paused record
  without sleeping on the single batching goroutine, while preserving WAL-first delivery order.
  The redelivered batch stays byte-identical — only the timing changes.
- New now rejects a flush size above the max queue size instead of accepting the configuration:
  with flushSize > maxQueueSize the queue budget filled first, so the size trigger could never
  fire and batches only left via the timer or explicit Flush/Shutdown. Equal values remain legal.
- WAL growth is capped by `maxQueueSize`; persisted state above the configured cap fails startup,
  serialized bodies are validated against their outer batch ID and event count, and failed
  pre-commit mutations roll back their in-memory state while post-commit sync failures retain the
  disk-equivalent state. File contents are fsynced before replace, followed by required fsyncs for
  the WAL directory and every newly created parent on supported platforms.
- HTTP 400 isolation is capped at 64 sends, preventing a large rejected batch from amplifying into
  an unbounded request storm. Concurrent admission now reserves queue capacity atomically and
  includes retained durable events in the budget.
- Terminal rejections of persisted single events now count in `Stats().Failed` and
  `LostEventsError` after their WAL records are removed, including rejections observed only during
  restart recovery; `Flush` also reports that recovery loss. Shutdown reports any accepted events
  that cannot be appended to the WAL after a recovery failure. When retained and permanently lost
  events coexist, the returned error exposes both `UndeliveredError` and `LostEventsError`.

[Unreleased]: https://github.com/alitycs/alitycs-sdk-go/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/alitycs/alitycs-sdk-go/releases/tag/v1.0.0
