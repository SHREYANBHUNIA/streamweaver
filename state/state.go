package state

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/manus/streamweaver/ingestion"
	"github.com/manus/streamweaver/storage"
	"github.com/manus/streamweaver/windows"
)

const aggregatePrefix = "aggregate/"
const processedPrefix = "processed/"

type Aggregate struct {
	ID          string    `json:"id"`
	Key         string    `json:"key"`
	WindowStart time.Time `json:"windowStart"`
	WindowEnd   time.Time `json:"windowEnd"`
	Sum         float64   `json:"sum"`
	Count       int64     `json:"count"`
	Emitted     bool      `json:"emitted"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Store struct{ kv storage.KV }

func NewStore(kv storage.KV) *Store { return &Store{kv: kv} }

func (s *Store) IsProcessed(eventID string) (bool, error) {
	_, exists, err := s.kv.Get([]byte(processedPrefix + eventID))
	return exists, err
}

func (s *Store) MarkProcessed(eventID string) error {
	return s.kv.Put([]byte(processedPrefix+eventID), []byte(time.Now().UTC().Format(time.RFC3339Nano)))
}

// Apply records all aggregate deltas plus the event ID in one RocksDB write
// batch. A redelivered event finds its persisted ID and cannot change SUM(amount)
// a second time.
func (s *Store) Apply(event ingestion.Event, assigned []windows.Window) ([]Aggregate, bool, error) {
	seen, err := s.IsProcessed(event.ID)
	if err != nil || seen {
		return nil, seen, err
	}
	changed := make([]Aggregate, 0, len(assigned))
	mutations := make([]storage.Mutation, 0, len(assigned)+1)
	for _, window := range assigned {
		key := aggregateKey(window.ID, event.Key)
		aggregate, err := s.loadAggregate(key)
		if err != nil {
			return nil, false, err
		}
		if aggregate.ID == "" {
			aggregate = Aggregate{ID: window.ID, Key: event.Key, WindowStart: window.Start, WindowEnd: window.End}
		}
		aggregate.Sum += event.Amount
		aggregate.Count++
		aggregate.UpdatedAt = time.Now().UTC()
		encoded, err := json.Marshal(aggregate)
		if err != nil {
			return nil, false, err
		}
		mutations = append(mutations, storage.Mutation{Key: []byte(key), Value: encoded})
		changed = append(changed, aggregate)
	}
	mutations = append(mutations, storage.Mutation{Key: []byte(processedPrefix + event.ID), Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))})
	if err := s.kv.Write(mutations); err != nil {
		return nil, false, err
	}
	return changed, false, nil
}

func (s *Store) List() ([]Aggregate, error) {
	entries, err := s.kv.ScanPrefix([]byte(aggregatePrefix))
	if err != nil {
		return nil, err
	}
	aggregates := make([]Aggregate, 0, len(entries))
	for _, entry := range entries {
		var aggregate Aggregate
		if err := json.Unmarshal(entry.Value, &aggregate); err != nil {
			return nil, fmt.Errorf("decode aggregate state: %w", err)
		}
		aggregates = append(aggregates, aggregate)
	}
	sort.Slice(aggregates, func(i, j int) bool { return aggregates[i].WindowEnd.After(aggregates[j].WindowEnd) })
	return aggregates, nil
}

func (s *Store) Emittable(watermark time.Time) ([]Aggregate, error) {
	all, err := s.List()
	if err != nil {
		return nil, err
	}
	ready := make([]Aggregate, 0)
	for _, aggregate := range all {
		if !aggregate.Emitted && !aggregate.WindowEnd.After(watermark) {
			ready = append(ready, aggregate)
		}
	}
	return ready, nil
}

func (s *Store) MarkEmitted(aggregates []Aggregate) error {
	mutations := make([]storage.Mutation, 0, len(aggregates))
	for _, aggregate := range aggregates {
		aggregate.Emitted = true
		aggregate.UpdatedAt = time.Now().UTC()
		encoded, err := json.Marshal(aggregate)
		if err != nil {
			return err
		}
		mutations = append(mutations, storage.Mutation{Key: []byte(aggregateKey(aggregate.ID, aggregate.Key)), Value: encoded})
	}
	return s.kv.Write(mutations)
}

func (s *Store) Close() { s.kv.Close() }

func (s *Store) loadAggregate(key string) (Aggregate, error) {
	value, exists, err := s.kv.Get([]byte(key))
	if err != nil || !exists {
		return Aggregate{}, err
	}
	var aggregate Aggregate
	if err := json.Unmarshal(value, &aggregate); err != nil {
		return Aggregate{}, fmt.Errorf("decode aggregate state: %w", err)
	}
	return aggregate, nil
}

func aggregateKey(windowID, key string) string { return aggregatePrefix + windowID + "/" + key }
