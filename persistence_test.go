package alitycs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestFileBatchStoreLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	record := durableBatchRecord{
		BatchID: "batch_exact", Body: `{"batchId":"batch_exact","events":[{},{}]}`, EventCount: 2,
	}
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}
	if got := store.pendingEvents(); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	if err := store.pause(record.BatchID, 123456); err != nil {
		t.Fatal(err)
	}
	if err := store.put(durableBatchRecord{
		BatchID: "batch_second", Body: `{"batchId":"batch_second","events":[{}]}`, EventCount: 1,
	}); err != nil {
		t.Fatal(err)
	}

	restarted, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := restarted.snapshot()
	if len(snapshot) != 2 || snapshot[0].Body != record.Body || snapshot[0].PausedUntilMS != 123456 || snapshot[1].BatchID != "batch_second" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if err := restarted.acknowledge(record.BatchID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.acknowledge("batch_second"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("empty WAL still exists: %v", err)
	}
}

func TestFileBatchStoreRejectsInvalidSerializedBodies(t *testing.T) {
	tests := []struct {
		name   string
		record durableBatchRecord
	}{
		{name: "missing metadata", record: durableBatchRecord{}},
		{name: "invalid json", record: durableBatchRecord{BatchID: "batch", Body: "not-json", EventCount: 1}},
		{name: "mismatched batch id", record: durableBatchRecord{
			BatchID: "batch", Body: `{"batchId":"other","events":[{}]}`, EventCount: 1,
		}},
		{name: "mismatched event count", record: durableBatchRecord{
			BatchID: "batch", Body: `{"batchId":"batch","events":[{}]}`, EventCount: 2,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal.json")
			raw, err := json.Marshal(durableBatchState{Version: 1, Batches: []durableBatchRecord{test.record}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := newFileBatchStore(path, defaultMaxQueueSize); err == nil {
				t.Fatal("invalid serialized body must fail initialization")
			}
		})
	}
}

func TestSyncDirectoryReportsSupportedOpenFailure(t *testing.T) {
	err := syncDirectory(filepath.Join(t.TempDir(), "missing"))
	switch runtime.GOOS {
	case "darwin", "ios", "windows", "plan9", "js", "wasip1":
		if err != nil {
			t.Fatalf("unsupported directory sync returned %v", err)
		}
	default:
		if err == nil {
			t.Fatal("supported directory sync hid an open failure")
		}
	}
}

func TestFileBatchStoreRejectsCorruptState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newFileBatchStore(path, defaultMaxQueueSize); err == nil {
		t.Fatal("corrupt state must fail initialization")
	}
}

func TestFileBatchStoreBoundsPendingEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.put(durableBatchRecord{
		BatchID: "batch_full", Body: `{"batchId":"batch_full","events":[{},{}]}`, EventCount: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.put(durableBatchRecord{BatchID: "batch_overflow", Body: "{}", EventCount: 1}); err == nil {
		t.Fatal("put beyond persistence event limit succeeded")
	}
	if got := store.pendingEvents(); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	if _, err := newFileBatchStore(path, 1); err == nil {
		t.Fatal("restart below persisted event count succeeded")
	}
}

func TestFileBatchStoreRollsBackMemoryAfterWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	record := durableBatchRecord{
		BatchID: "batch_exact", Body: `{"batchId":"batch_exact","events":[{}]}`, EventCount: 1,
	}
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}
	store.path = t.TempDir() // rename over an existing directory must fail
	if err := store.pause(record.BatchID, 123); err == nil {
		t.Fatal("pause unexpectedly succeeded")
	}
	snapshot := store.snapshot()
	if len(snapshot) != 1 || snapshot[0].PausedUntilMS != 0 {
		t.Fatalf("failed pause changed memory: %#v", snapshot)
	}
	if err := os.WriteFile(filepath.Join(store.path, "marker"), []byte("keep directory non-empty"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.acknowledge(record.BatchID); err == nil {
		t.Fatal("acknowledge unexpectedly succeeded")
	}
	if got := store.pendingEvents(); got != 1 {
		t.Fatalf("failed acknowledge left %d pending, want 1", got)
	}
}

func TestFileBatchStoreKeepsCommittedMemoryAfterDirectorySyncFailure(t *testing.T) {
	record := durableBatchRecord{
		BatchID: "batch_exact", Body: `{"batchId":"batch_exact","events":[{}]}`, EventCount: 1,
	}
	syncFailure := errors.New("directory sync failed")

	t.Run("put", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.json")
		store, err := newFileBatchStore(path, defaultMaxQueueSize)
		if err != nil {
			t.Fatal(err)
		}
		store.syncDir = func(string) error { return syncFailure }

		if err := store.put(record); !errors.Is(err, syncFailure) {
			t.Fatalf("put error = %v, want %v", err, syncFailure)
		}
		if got := store.snapshot(); len(got) != 1 || got[0] != record || store.pendingEvents() != 1 {
			t.Fatalf("committed put not retained in memory: %#v, pending = %d", got, store.pendingEvents())
		}
		restarted, err := newFileBatchStore(path, defaultMaxQueueSize)
		if err != nil {
			t.Fatal(err)
		}
		if got := restarted.snapshot(); len(got) != 1 || got[0] != record {
			t.Fatalf("committed put not retained on disk: %#v", got)
		}
	})

	t.Run("pause", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.json")
		store, err := newFileBatchStore(path, defaultMaxQueueSize)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.put(record); err != nil {
			t.Fatal(err)
		}
		store.syncDir = func(string) error { return syncFailure }

		const pausedUntilMS = int64(123456)
		if err := store.pause(record.BatchID, pausedUntilMS); !errors.Is(err, syncFailure) {
			t.Fatalf("pause error = %v, want %v", err, syncFailure)
		}
		if got := store.snapshot(); len(got) != 1 || got[0].PausedUntilMS != pausedUntilMS {
			t.Fatalf("committed pause not retained in memory: %#v", got)
		}
		restarted, err := newFileBatchStore(path, defaultMaxQueueSize)
		if err != nil {
			t.Fatal(err)
		}
		if got := restarted.snapshot(); len(got) != 1 || got[0].PausedUntilMS != pausedUntilMS {
			t.Fatalf("committed pause not retained on disk: %#v", got)
		}
	})

	t.Run("acknowledge", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.json")
		store, err := newFileBatchStore(path, defaultMaxQueueSize)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.put(record); err != nil {
			t.Fatal(err)
		}
		store.syncDir = func(string) error { return syncFailure }

		if err := store.acknowledge(record.BatchID); !errors.Is(err, syncFailure) {
			t.Fatalf("acknowledge error = %v, want %v", err, syncFailure)
		}
		if got := store.snapshot(); len(got) != 0 || store.pendingEvents() != 0 {
			t.Fatalf("committed acknowledge not retained in memory: %#v, pending = %d", got, store.pendingEvents())
		}
		restarted, err := newFileBatchStore(path, defaultMaxQueueSize)
		if err != nil {
			t.Fatal(err)
		}
		if got := restarted.snapshot(); len(got) != 0 || restarted.pendingEvents() != 0 {
			t.Fatalf("committed acknowledge not retained on disk: %#v, pending = %d", got, restarted.pendingEvents())
		}
	})
}

func TestFileBatchStoreSyncsNewDirectoryParents(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "one", "two", "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	var synced []string
	store.syncDir = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}
	record := durableBatchRecord{
		BatchID: "batch_exact", Body: `{"batchId":"batch_exact","events":[{}]}`, EventCount: 1,
	}
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Clean(root), filepath.Join(root, "one"), filepath.Join(root, "one", "two")}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("synced directories = %#v, want %#v", synced, want)
	}
}

func TestTransportRecoversExactPersistedBody(t *testing.T) {
	var attempts atomic.Int32
	bodies := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- string(body)
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "wal.json")
	store, _ := newFileBatchStore(path, defaultMaxQueueSize)
	first := newTestTransport(t, server.URL, 0)
	first.store = store
	err := first.send(context.Background(), samplePayload())
	var retained *durableBatchError
	if !errors.As(err, &retained) || store.pendingEvents() != 1 {
		t.Fatalf("send error = %v, pending = %d", err, store.pendingEvents())
	}

	restartedStore, _ := newFileBatchStore(path, defaultMaxQueueSize)
	restarted := newTestTransport(t, server.URL, 0)
	restarted.store = restartedStore
	if err := restarted.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	firstBody, replayedBody := <-bodies, <-bodies
	if firstBody != replayedBody {
		t.Fatalf("replay changed body:\n%s\n%s", firstBody, replayedBody)
	}
	if restartedStore.pendingEvents() != 0 {
		t.Fatalf("pending = %d, want 0", restartedStore.pendingEvents())
	}
}

func TestTransportRecoveryHonoursPersistedRetryAfter(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "wal.json")
	store, _ := newFileBatchStore(path, defaultMaxQueueSize)
	first := newTestTransport(t, server.URL, 0)
	first.store = store
	if err := first.send(context.Background(), samplePayload()); err == nil {
		t.Fatal("first 429 must remain pending")
	}

	restartedStore, _ := newFileBatchStore(path, defaultMaxQueueSize)
	restarted := newTestTransport(t, server.URL, 0)
	restarted.store = restartedStore
	restarted.sleep = func(context.Context, time.Duration) error {
		t.Fatal("recovery must not sleep on the batcher loop")
		return nil
	}
	err := restarted.recover(context.Background())
	var deferred *recoveryDeferredError
	if !errors.As(err, &deferred) {
		t.Fatalf("recover error = %v, want recoveryDeferredError", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("recovery made %d requests before Retry-After elapsed, want 1", got)
	}
	snapshot := restartedStore.snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("pending records = %d, want 1", len(snapshot))
	}
	if err := restartedStore.pause(snapshot[0].BatchID, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := restarted.recover(context.Background()); err != nil {
		t.Fatalf("recover after Retry-After: %v", err)
	}
	if got := attempts.Load(); got != 2 || restartedStore.pendingEvents() != 0 {
		t.Fatalf("attempts = %d, pending = %d; want 2, 0", got, restartedStore.pendingEvents())
	}
}

func TestTransportRecoveryCapsAndDefersUntrustedPersistedRetryAfter(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	record, err := durableRecord(samplePayload())
	if err != nil {
		t.Fatal(err)
	}
	record.PausedUntilMS = time.Now().Add(24 * time.Hour).UnixMilli()
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}

	restarted, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	transport := newTestTransport(t, server.URL, 0)
	transport.store = restarted
	transport.sleep = func(context.Context, time.Duration) error {
		t.Fatal("recovery must not sleep on the batcher loop")
		return nil
	}
	before := time.Now()
	err = transport.recover(context.Background())
	var deferred *recoveryDeferredError
	if !errors.As(err, &deferred) {
		t.Fatalf("recover error = %v, want recoveryDeferredError", err)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("recovery made %d requests before the bounded deadline, want 0", got)
	}
	maximum := before.Add(maxRetryAfter + time.Second).UnixMilli()
	snapshot := restarted.snapshot()
	if len(snapshot) != 1 || snapshot[0].PausedUntilMS > maximum {
		t.Fatalf("bounded persisted deadline = %#v, want no later than %d", snapshot, maximum)
	}
}

func TestTransportRecoveryPrefersCallerCancellationToDeferredRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	record, err := durableRecord(samplePayload())
	if err != nil {
		t.Fatal(err)
	}
	record.PausedUntilMS = time.Now().Add(time.Minute).UnixMilli()
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}

	transport := newTestTransport(t, "http://127.0.0.1:1", 0)
	transport.store = store
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := transport.recover(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("recover error = %v, want context.Canceled", err)
	}
}

func TestPersistentTerminalRejectionCountsAsFailedAndLeavesNoWALRecord(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "wal.json")
	client, err := New(
		"pk_test",
		WithEndpoint(server.URL),
		WithFlushSize(100),
		WithFlushInterval(0),
		WithMaxRetries(0),
		WithPersistence(path),
	)
	if err != nil {
		t.Fatal(err)
	}
	client.Track(context.Background(), "terminal", nil)

	err = client.Shutdown(context.Background())
	var lost *LostEventsError
	if !errors.As(err, &lost) || lost.Lost != 1 {
		t.Fatalf("Shutdown = %v, want LostEventsError with one terminal rejection", err)
	}
	if stats := client.Stats(); stats.Failed != 1 || stats.Delivered != 0 {
		t.Fatalf("Stats = %+v, want one failed and zero delivered", stats)
	}
	restarted, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.pendingEvents(); got != 0 {
		t.Fatalf("pending WAL events = %d, want terminal rejection removed", got)
	}
}

func TestRecoveredTerminalRejectionCountsAsFailedAndLeavesNoWALRecord(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	record, err := durableRecord(samplePayload())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}
	client, err := New(
		"pk_test",
		WithEndpoint(server.URL),
		WithFlushSize(100),
		WithFlushInterval(0),
		WithMaxRetries(0),
		WithPersistence(path),
	)
	if err != nil {
		t.Fatal(err)
	}
	flushErr := client.Flush(context.Background())
	var outcome *recoveryOutcomeError
	if !errors.As(flushErr, &outcome) || outcome.Lost != 1 || outcome.Blocked {
		t.Fatalf("Flush = %v, want one non-blocking recovered loss", flushErr)
	}
	if got := client.Stats().Failed; got != 1 {
		t.Fatalf("Stats().Failed = %d, want 1", got)
	}
	shutdownErr := client.Shutdown(context.Background())
	var lost *LostEventsError
	if !errors.As(shutdownErr, &lost) || lost.Lost != 1 {
		t.Fatalf("Shutdown = %v, want LostEventsError with one recovered loss", shutdownErr)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("terminal recovery left WAL behind: %v", err)
	}
}

func TestShutdownDeadlineIncludesRecoveredWALRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	record, err := durableRecord(samplePayload())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-unblock
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	released := false
	defer func() {
		if !released {
			close(unblock)
		}
	}()
	client, err := New(
		"pk_test",
		WithEndpoint(server.URL),
		WithFlushInterval(0),
		WithMaxRetries(0),
		WithPersistence(path),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownErr := client.Shutdown(ctx)
	select {
	case <-started:
	default:
		t.Fatal("shutdown deadline elapsed before recovery reached the endpoint")
	}
	var undelivered *UndeliveredError
	if !errors.As(shutdownErr, &undelivered) || undelivered.Undelivered != 1 {
		t.Fatalf("Shutdown = %v, want UndeliveredError with one recovered WAL event", shutdownErr)
	}
	close(unblock)
	released = true
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatalf("completed Shutdown: %v", err)
	}
}

func TestShutdownDeadlineReportsRecoveredLossBeforeBlockedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.json")
	store, err := newFileBatchStore(path, defaultMaxQueueSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, batchID := range []string{"batch_rejected", "batch_blocked"} {
		payload := samplePayload()
		payload.BatchID = batchID
		record, err := durableRecord(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.put(record); err != nil {
			t.Fatal(err)
		}
	}

	var attempts atomic.Int32
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		close(blocked)
		<-unblock
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	released := false
	defer func() {
		if !released {
			close(unblock)
		}
	}()
	client, err := New(
		"pk_test",
		WithEndpoint(server.URL),
		WithFlushInterval(0),
		WithMaxRetries(0),
		WithPersistence(path),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownErr := client.Shutdown(ctx)
	select {
	case <-blocked:
	default:
		t.Fatal("shutdown deadline elapsed before recovery reached the blocked record")
	}
	var undelivered *UndeliveredError
	if !errors.As(shutdownErr, &undelivered) || undelivered.Undelivered != 1 {
		t.Fatalf("Shutdown = %v, want UndeliveredError with one retained event", shutdownErr)
	}
	var lost *LostEventsError
	if !errors.As(shutdownErr, &lost) || lost.Lost != 1 {
		t.Fatalf("Shutdown = %v, want LostEventsError with one recovered loss", shutdownErr)
	}
	close(unblock)
	released = true
	finalErr := client.Shutdown(context.Background())
	if !errors.As(finalErr, &lost) || lost.Lost != 1 {
		t.Fatalf("completed Shutdown = %v, want the same single recovered loss", finalErr)
	}
}
