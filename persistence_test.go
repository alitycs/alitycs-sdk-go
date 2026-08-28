package alitycs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	record := durableBatchRecord{BatchID: "batch_exact", Body: `{"batchId":"batch_exact"}`, EventCount: 2}
	if err := store.put(record); err != nil {
		t.Fatal(err)
	}
	if got := store.pendingEvents(); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	if err := store.pause(record.BatchID, 123456); err != nil {
		t.Fatal(err)
	}
	if err := store.put(durableBatchRecord{BatchID: "batch_second", Body: "{}", EventCount: 1}); err != nil {
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
	if err := store.put(durableBatchRecord{BatchID: "batch_full", Body: "{}", EventCount: 2}); err != nil {
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
	record := durableBatchRecord{BatchID: "batch_exact", Body: "{}", EventCount: 1}
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

func TestTransportRecoversExactPersistedBody(t *testing.T) {
	var attempts atomic.Int32
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
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
	if string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("replay changed body:\n%s\n%s", bodies[0], bodies[1])
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
	var waited time.Duration
	restarted.sleep = func(_ context.Context, delay time.Duration) error {
		waited = delay
		return nil
	}
	if err := restarted.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if waited < 2500*time.Millisecond {
		t.Fatalf("waited %s, want remaining Retry-After", waited)
	}
}
