package alitycs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	mu      sync.Mutex
	path    string
	records map[string]durableBatchRecord
	order   []string
}

func newFileBatchStore(path string) (*fileBatchStore, error) {
	store := &fileBatchStore{path: path, records: make(map[string]durableBatchRecord)}
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
		if _, exists := store.records[record.BatchID]; !exists {
			store.order = append(store.order, record.BatchID)
		}
		store.records[record.BatchID] = record
	}
	return store, nil
}

func (s *fileBatchStore) enabled() bool { return s != nil && s.path != "" }

func (s *fileBatchStore) put(record durableBatchRecord) error {
	if !s.enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[record.BatchID]; !exists {
		s.order = append(s.order, record.BatchID)
	}
	s.records[record.BatchID] = record
	return s.writeLocked()
}

func (s *fileBatchStore) acknowledge(batchID string) error {
	if !s.enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, batchID)
	for index, existing := range s.order {
		if existing == batchID {
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
	return s.writeLocked()
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
	record.PausedUntilMS = untilMS
	s.records[batchID] = record
	return s.writeLocked()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, record := range s.records {
		total += record.EventCount
	}
	return total
}

func (s *fileBatchStore) writeLocked() error {
	if len(s.records) == 0 {
		if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("alitycs: remove empty persistence file: %w", err)
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
	defer os.Remove(temporaryPath)
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
	return nil
}
