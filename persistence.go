package alitycs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
)

type durableBatchRecord struct {
	BatchID       string `json:"batchId"`
	Body          string `json:"body"`
	EventCount    int    `json:"eventCount"`
	PausedUntilMS int64  `json:"pausedUntilMs,omitempty"`
}

type durableBatchState struct {
	Version int                  `json:"version"`
	Batches []durableBatchRecord `json:"batches"`
}

// fileBatchStore atomically snapshots serialized batches awaiting a terminal outcome.
type fileBatchStore struct {
	mu               sync.Mutex
	path             string
	maxPendingEvents int
	pending          atomic.Int64
	records          map[string]durableBatchRecord
	order            []string
}

func newFileBatchStore(path string, maxPendingEvents int) (*fileBatchStore, error) {
	if maxPendingEvents < 1 {
		return nil, errors.New("alitycs: persistence event limit must be positive")
	}
	store := &fileBatchStore{
		path: path, maxPendingEvents: maxPendingEvents, records: make(map[string]durableBatchRecord),
	}
	if path == "" {
		return store, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return nil, fmt.Errorf("alitycs: read persistence file: %w", err)
	}
	var state durableBatchState
	if err := json.Unmarshal(raw, &state); err != nil || state.Version != 1 {
		return nil, fmt.Errorf("alitycs: invalid persistence file %q", path)
	}
	for _, record := range state.Batches {
		if err := validateDurableRecord(record); err != nil {
			return nil, fmt.Errorf("alitycs: invalid persistence record in %q", path)
		}
		if _, exists := store.records[record.BatchID]; exists {
			return nil, fmt.Errorf("alitycs: duplicate persistence batch %q", record.BatchID)
		}
		store.order = append(store.order, record.BatchID)
		store.records[record.BatchID] = record
		pending := store.pending.Add(int64(record.EventCount))
		if pending > int64(store.maxPendingEvents) {
			return nil, fmt.Errorf("alitycs: persistence file exceeds the configured event limit (%d)", maxPendingEvents)
		}
	}
	return store, nil
}

func validateDurableRecord(record durableBatchRecord) error {
	if record.BatchID == "" || record.EventCount < 1 || record.Body == "" {
		return errors.New("alitycs: invalid persistence record metadata")
	}
	var envelope struct {
		BatchID string            `json:"batchId"`
		Events  []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal([]byte(record.Body), &envelope); err != nil {
		return fmt.Errorf("alitycs: invalid persisted batch body: %w", err)
	}
	if envelope.BatchID != record.BatchID || len(envelope.Events) != record.EventCount {
		return errors.New("alitycs: persisted batch body does not match its record")
	}
	return nil
}

func (s *fileBatchStore) enabled() bool { return s != nil && s.path != "" }

func (s *fileBatchStore) put(record durableBatchRecord) error {
	if !s.enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.records[record.BatchID]; exists {
		if existing == record {
			return nil
		}
		return fmt.Errorf("alitycs: persistence batch id collision %q", record.BatchID)
	}
	if record.EventCount < 1 || s.pending.Load()+int64(record.EventCount) > int64(s.maxPendingEvents) {
		return fmt.Errorf("alitycs: persistence event limit exceeded (%d)", s.maxPendingEvents)
	}
	s.order = append(s.order, record.BatchID)
	s.records[record.BatchID] = record
	s.pending.Add(int64(record.EventCount))
	if err := s.writeLocked(); err != nil {
		delete(s.records, record.BatchID)
		s.order = s.order[:len(s.order)-1]
		s.pending.Add(-int64(record.EventCount))
		return err
	}
	return nil
}

func (s *fileBatchStore) acknowledge(batchID string) error {
	if !s.enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[batchID]
	if !exists {
		return nil
	}
	delete(s.records, batchID)
	removedIndex := -1
	for index, existing := range s.order {
		if existing == batchID {
			removedIndex = index
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
	s.pending.Add(-int64(record.EventCount))
	if err := s.writeLocked(); err != nil {
		s.records[batchID] = record
		if removedIndex >= 0 {
			s.order = append(s.order, "")
			copy(s.order[removedIndex+1:], s.order[removedIndex:])
			s.order[removedIndex] = batchID
		}
		s.pending.Add(int64(record.EventCount))
		return err
	}
	return nil
}

func (s *fileBatchStore) pause(batchID string, untilMS int64) error {
	if !s.enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[batchID]
	if !ok {
		return nil
	}
	previous := record
	record.PausedUntilMS = untilMS
	s.records[batchID] = record
	if err := s.writeLocked(); err != nil {
		s.records[batchID] = previous
		return err
	}
	return nil
}

func (s *fileBatchStore) snapshot() []durableBatchRecord {
	if !s.enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]durableBatchRecord, 0, len(s.records))
	for _, batchID := range s.order {
		if record, ok := s.records[batchID]; ok {
			out = append(out, record)
		}
	}
	return out
}

func (s *fileBatchStore) pendingEvents() int {
	if !s.enabled() {
		return 0
	}
	return int(s.pending.Load())
}

func (s *fileBatchStore) writeLocked() error {
	if len(s.records) == 0 {
		if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("alitycs: remove empty persistence file: %w", err)
		}
		if err := syncDirectory(filepath.Dir(s.path)); err != nil {
			return err
		}
		return nil
	}
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("alitycs: create persistence directory: %w", err)
	}
	batches := make([]durableBatchRecord, 0, len(s.records))
	for _, batchID := range s.order {
		if record, ok := s.records[batchID]; ok {
			batches = append(batches, record)
		}
	}
	raw, err := json.Marshal(durableBatchState{Version: 1, Batches: batches})
	if err != nil {
		return fmt.Errorf("alitycs: encode persistence state: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".alitycs-wal-*")
	if err != nil {
		return fmt.Errorf("alitycs: create persistence temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if _, err = temporary.Write(raw); err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("alitycs: write persistence state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("alitycs: replace persistence state: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	return nil
}

func syncDirectory(directory string) error {
	// Directory fsync is unavailable on these targets. Atomic rename/remove still
	// applies, but the platform cannot provide the stronger power-loss durability
	// guarantee available on Unix filesystems that support syncing directories.
	switch runtime.GOOS {
	case "darwin", "ios", "windows", "plan9", "js", "wasip1":
		return nil
	}
	handle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("alitycs: open persistence directory for sync: %w", err)
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	if syncErr != nil {
		return fmt.Errorf("alitycs: sync persistence directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("alitycs: close persistence directory: %w", closeErr)
	}
	return nil
}
