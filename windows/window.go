package windows

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Kind string

const (
	Tumbling Kind = "tumbling"
	Sliding  Kind = "sliding"
)

type LateEventPolicy string

const (
	DropLate       LateEventPolicy = "drop"
	SideOutputLate LateEventPolicy = "side_output"
	AccumulateLate LateEventPolicy = "accumulate"
)

type Config struct {
	Kind            Kind            `json:"kind"`
	Size            time.Duration   `json:"size"`
	Slide           time.Duration   `json:"slide"`
	AllowedLateness time.Duration   `json:"allowedLateness"`
	LatePolicy      LateEventPolicy `json:"latePolicy"`
}

type Window struct {
	ID    string    `json:"id"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Observation struct {
	Windows   []Window
	Watermark time.Time
	IsLate    bool
}

// Tracker advances its watermark from the greatest event-time observed, rather
// than wall clock time. A window is eligible to emit when end <= watermark.
type Tracker struct {
	config       Config
	mu           sync.RWMutex
	maxEventTime time.Time
	watermark    time.Time
}

func NewTracker(config Config) (*Tracker, error) {
	if config.Size <= 0 {
		return nil, errors.New("window size must be positive")
	}
	if config.Kind != Tumbling && config.Kind != Sliding {
		return nil, fmt.Errorf("unsupported window kind %q", config.Kind)
	}
	if config.Kind == Sliding && config.Slide <= 0 {
		return nil, errors.New("sliding windows require a positive slide")
	}
	if config.Kind == Tumbling {
		config.Slide = config.Size
	}
	if config.LatePolicy == "" {
		config.LatePolicy = SideOutputLate
	}
	return &Tracker{config: config}, nil
}

func (t *Tracker) Observe(eventTime time.Time) Observation {
	eventTime = eventTime.UTC()
	t.mu.Lock()
	defer t.mu.Unlock()
	wasLate := !t.watermark.IsZero() && eventTime.Before(t.watermark)
	if t.maxEventTime.IsZero() || eventTime.After(t.maxEventTime) {
		t.maxEventTime = eventTime
		t.watermark = eventTime.Add(-t.config.AllowedLateness)
	}
	return Observation{Windows: assign(t.config, eventTime), Watermark: t.watermark, IsLate: wasLate}
}

func (t *Tracker) Watermark() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.watermark
}

func (t *Tracker) RestoreWatermark(watermark time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if watermark.After(t.watermark) {
		t.watermark = watermark.UTC()
		t.maxEventTime = watermark.Add(t.config.AllowedLateness).UTC()
	}
}

func assign(config Config, eventTime time.Time) []Window {
	if config.Kind == Tumbling {
		start := eventTime.Truncate(config.Size)
		return []Window{makeWindow(start, config.Size)}
	}
	anchor := eventTime.Truncate(config.Slide)
	oldest := eventTime.Add(-config.Size)
	result := make([]Window, 0, int(config.Size/config.Slide)+1)
	for start := anchor; start.After(oldest); start = start.Add(-config.Slide) {
		candidate := makeWindow(start, config.Size)
		if !eventTime.Before(candidate.Start) && eventTime.Before(candidate.End) {
			result = append(result, candidate)
		}
	}
	return result
}

func makeWindow(start time.Time, size time.Duration) Window {
	end := start.Add(size)
	return Window{ID: fmt.Sprintf("%d-%d", start.UnixMilli(), end.UnixMilli()), Start: start, End: end}
}
