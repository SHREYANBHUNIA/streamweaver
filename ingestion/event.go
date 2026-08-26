package ingestion

import (
	"errors"
	"strings"
	"time"
)

// Event is the normalized transaction envelope processed by StreamWeaver.
// ID must be stable across Kafka redelivery so state updates remain idempotent.
type Event struct {
	ID         string    `json:"id"`
	Stream     string    `json:"stream"`
	Key        string    `json:"key"`
	Amount     float64   `json:"amount"`
	EventTime  time.Time `json:"eventTime"`
	IngestedAt time.Time `json:"ingestedAt"`
	Topic      string    `json:"topic,omitempty"`
	Partition  int       `json:"partition,omitempty"`
	Offset     int64     `json:"offset,omitempty"`
}

func (e Event) Normalize() (Event, error) {
	if strings.TrimSpace(e.ID) == "" {
		return Event{}, errors.New("event id is required for idempotent processing")
	}
	if strings.TrimSpace(e.Key) == "" {
		return Event{}, errors.New("event key is required")
	}
	if e.EventTime.IsZero() {
		return Event{}, errors.New("event time is required")
	}
	if e.Stream == "" {
		e.Stream = "transactions"
	}
	e.EventTime = e.EventTime.UTC()
	if e.IngestedAt.IsZero() {
		e.IngestedAt = time.Now().UTC()
	} else {
		e.IngestedAt = e.IngestedAt.UTC()
	}
	return e, nil
}

func (e Event) SourceKey() string {
	if e.Topic == "" {
		return "http"
	}
	return e.Topic + ":" + strings.TrimSpace(string(rune(e.Partition)))
}
