# Changelog

This project follows [Semantic Versioning](https://semver.org/). User-visible changes are recorded
here before a version tag is created.

## [Unreleased]

## [1.0.0] - 2026-08-28

### Added
- Optional `WithPersistence(path)` exact-batch write-ahead logging. A serialized in-flight batch
  is stored atomically before its first attempt and replayed byte-identically after restart,
  including any remaining final `Retry-After` deadline. Terminal responses acknowledge the WAL;
  pre-flush in-memory events remain outside this durability boundary.

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
  after it waits at least that long instead of the default backoff. Only SDK-generated exponential
  backoff is capped at 10s; server deadlines are not shortened.
  The redelivered batch stays byte-identical — only the timing changes.
- New now rejects a flush size above the max queue size instead of accepting the configuration:
  with flushSize > maxQueueSize the queue budget filled first, so the size trigger could never
  fire and batches only left via the timer or explicit Flush/Shutdown. Equal values remain legal.
- WAL growth is capped by `maxQueueSize`; persisted state above the configured cap fails startup,
  and failed mutations roll back their in-memory state. File contents are fsynced before replace,
  followed by a best-effort directory fsync.
- HTTP 400 isolation is capped at 64 sends, preventing a large rejected batch from amplifying into
  an unbounded request storm. Concurrent admission now reserves queue capacity atomically and
  includes retained durable events in the budget.

[Unreleased]: https://github.com/alitycs/alitycs-sdk-go/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/alitycs/alitycs-sdk-go/releases/tag/v1.0.0
