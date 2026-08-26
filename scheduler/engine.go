package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/manus/streamweaver/checkpoint"
	"github.com/manus/streamweaver/ingestion"
	"github.com/manus/streamweaver/operators"
	"github.com/manus/streamweaver/state"
	"github.com/manus/streamweaver/storage"
	"github.com/manus/streamweaver/windows"
)

type Config struct {
	PipelineID          string
	Threshold           float64
	QueueCapacity       int
	DataDirectory       string
	CheckpointDirectory string
	Window              windows.Config
}

type Metrics struct {
	Received           int64         `json:"received"`
	Processed          int64         `json:"processed"`
	Filtered           int64         `json:"filtered"`
	Duplicates         int64         `json:"duplicates"`
	LateDropped        int64         `json:"lateDropped"`
	LateSideOutput     int64         `json:"lateSideOutput"`
	LateAccumulated    int64         `json:"lateAccumulated"`
	BackpressureReject int64         `json:"backpressureRejects"`
	CheckpointCount    int64         `json:"checkpointCount"`
	RecoveryFailures   int64         `json:"recoveryFailures"`
	LastLatency        time.Duration `json:"lastLatency"`
}

type Alert struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	WindowID  string    `json:"windowId,omitempty"`
	Message   string    `json:"message"`
	Sum       float64   `json:"sum,omitempty"`
	Threshold float64   `json:"threshold,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type Snapshot struct {
	PipelineID     string              `json:"pipelineId"`
	Status         string              `json:"status"`
	StartedAt      time.Time           `json:"startedAt"`
	Watermark      time.Time           `json:"watermark"`
	QueueDepth     int                 `json:"queueDepth"`
	QueueCapacity  int                 `json:"queueCapacity"`
	Metrics        Metrics             `json:"metrics"`
	Checkpoint     checkpoint.Manifest `json:"checkpoint"`
	Windows        []state.Aggregate   `json:"windows"`
	LateEvents     []ingestion.Event   `json:"lateEvents"`
	Alerts         []Alert             `json:"alerts"`
	RecoveryPolicy string              `json:"recoveryPolicy"`
}

type PipelineConfig struct {
	PipelineID      string  `json:"pipelineId"`
	AlertThreshold  float64 `json:"alertThreshold"`
	WindowKind      string  `json:"windowKind"`
	WindowSize      string  `json:"windowSize"`
	SlidingEvery    string  `json:"slidingEvery,omitempty"`
	AllowedLateness string  `json:"allowedLateness"`
	LateEventPolicy string  `json:"lateEventPolicy"`
	QueueCapacity   int     `json:"queueCapacity"`
}

type PipelineConfigUpdate struct {
	AlertThreshold *float64 `json:"alertThreshold"`
}

type Engine struct {
	config      Config
	queue       *ingestion.BoundedQueue
	pipeline    operators.Pipeline
	tracker     *windows.Tracker
	state       *state.Store
	checkpoints *checkpoint.Manager
	notifier    Notifier

	mu             sync.RWMutex
	startedAt      time.Time
	running        bool
	metrics        Metrics
	offsets        map[string]int64
	lastCheckpoint checkpoint.Manifest
	lateEvents     []ingestion.Event
	alerts         []Alert
	alertedWindows map[string]bool
}

func Open(config Config, notifier Notifier) (*Engine, error) {
	if config.PipelineID == "" {
		config.PipelineID = "transactions-sum-10s"
	}
	if config.DataDirectory == "" || config.CheckpointDirectory == "" {
		return nil, errors.New("data and checkpoint directories are required")
	}
	rocks, err := storage.Open(config.DataDirectory)
	if err != nil {
		return nil, err
	}
	tracker, err := windows.NewTracker(config.Window)
	if err != nil {
		rocks.Close()
		return nil, err
	}
	checkpoints, err := checkpoint.New(config.CheckpointDirectory)
	if err != nil {
		rocks.Close()
		return nil, err
	}
	engine := &Engine{
		config: config,
		queue:  ingestion.NewBoundedQueue(config.QueueCapacity),
		pipeline: operators.NewPipeline(
			operators.Filter(func(event ingestion.Event) bool { return event.Amount != 0 }),
			operators.Map(func(event ingestion.Event) ingestion.Event { event.Key = event.Key; return event }),
		),
		tracker:        tracker,
		state:          state.NewStore(rocks),
		checkpoints:    checkpoints,
		notifier:       notifier,
		startedAt:      time.Now().UTC(),
		offsets:        map[string]int64{},
		alertedWindows: map[string]bool{},
	}
	if err := engine.recover(); err != nil {
		engine.Close()
		return nil, err
	}
	return engine, nil
}

func (e *Engine) Run(ctx context.Context) error {
	e.mu.Lock()
	e.running = true
	e.mu.Unlock()
	for {
		submission, err := e.queue.Next(ctx)
		if err != nil {
			e.mu.Lock()
			e.running = false
			e.mu.Unlock()
			return err
		}
		err = e.process(ctx, submission.Event)
		submission.Ack <- err
		close(submission.Ack)
	}
}

func (e *Engine) SubmitAndWait(ctx context.Context, event ingestion.Event) error {
	event, err := event.Normalize()
	if err != nil {
		return err
	}
	ack, err := e.queue.Enqueue(ctx, event)
	if err != nil {
		if errors.Is(err, ingestion.ErrBackpressure) {
			e.mu.Lock()
			e.metrics.BackpressureReject++
			e.mu.Unlock()
		}
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) Submit(ctx context.Context, event ingestion.Event) error {
	event, err := event.Normalize()
	if err != nil {
		return err
	}
	_, err = e.queue.Enqueue(ctx, event)
	if errors.Is(err, ingestion.ErrBackpressure) {
		e.mu.Lock()
		e.metrics.BackpressureReject++
		e.mu.Unlock()
	}
	return err
}

func (e *Engine) process(ctx context.Context, event ingestion.Event) error {
	started := time.Now()
	e.mu.Lock()
	e.metrics.Received++
	e.mu.Unlock()
	processed, keep, err := e.pipeline.Apply(event)
	if err != nil {
		return err
	}
	if !keep {
		if err := e.state.MarkProcessed(event.ID); err != nil {
			return err
		}
		e.mu.Lock()
		e.metrics.Filtered++
		e.mu.Unlock()
		return e.checkpoint(ctx, processed)
	}

	observation := e.tracker.Observe(processed.EventTime)
	if observation.IsLate && e.config.Window.LatePolicy != windows.AccumulateLate {
		if err := e.state.MarkProcessed(processed.ID); err != nil {
			return err
		}
		e.mu.Lock()
		if e.config.Window.LatePolicy == windows.DropLate {
			e.metrics.LateDropped++
		} else {
			e.metrics.LateSideOutput++
			e.lateEvents = appendBounded(e.lateEvents, processed, 50)
		}
		e.mu.Unlock()
		return e.checkpoint(ctx, processed)
	}

	changed, duplicate, err := e.state.Apply(processed, observation.Windows)
	if err != nil {
		return err
	}
	if duplicate {
		e.mu.Lock()
		e.metrics.Duplicates++
		e.mu.Unlock()
	} else {
		e.mu.Lock()
		e.metrics.Processed++
		if observation.IsLate {
			e.metrics.LateAccumulated++
		}
		e.metrics.LastLatency = time.Since(started)
		e.mu.Unlock()
		for _, aggregate := range changed {
			e.maybeAlert(ctx, aggregate)
		}
	}
	if ready, err := e.state.Emittable(observation.Watermark); err == nil && len(ready) > 0 {
		if err := e.state.MarkEmitted(ready); err != nil {
			return err
		}
	}
	return e.checkpoint(ctx, processed)
}

func (e *Engine) checkpoint(ctx context.Context, event ingestion.Event) error {
	e.mu.Lock()
	if event.Topic != "" {
		e.offsets[fmt.Sprintf("%s:%d", event.Topic, event.Partition)] = event.Offset
	}
	offsets := make(map[string]int64, len(e.offsets))
	for key, value := range e.offsets {
		offsets[key] = value
	}
	manifest := checkpoint.Manifest{PipelineID: e.config.PipelineID, Watermark: e.tracker.Watermark(), Offsets: offsets, CreatedAt: time.Now().UTC()}
	e.mu.Unlock()
	if err := e.checkpoints.Save(manifest); err != nil {
		e.recordRecoveryFailure(ctx, err)
		return fmt.Errorf("persist checkpoint: %w", err)
	}
	e.mu.Lock()
	e.metrics.CheckpointCount++
	e.lastCheckpoint = manifest
	e.mu.Unlock()
	return nil
}

func (e *Engine) recover() error {
	manifest, err := e.checkpoints.Latest()
	if err != nil {
		return err
	}
	if manifest.PipelineID == "" {
		return nil
	}
	if manifest.PipelineID != e.config.PipelineID {
		return fmt.Errorf("checkpoint belongs to pipeline %q, not %q", manifest.PipelineID, e.config.PipelineID)
	}
	e.tracker.RestoreWatermark(manifest.Watermark)
	e.offsets = manifest.Offsets
	e.lastCheckpoint = manifest
	return nil
}

func (e *Engine) maybeAlert(ctx context.Context, aggregate state.Aggregate) {
	e.mu.Lock()
	threshold := e.config.Threshold
	if aggregate.Sum <= threshold {
		e.mu.Unlock()
		return
	}
	if e.alertedWindows[aggregate.ID] {
		e.mu.Unlock()
		return
	}
	alert := Alert{
		ID:        "threshold-" + aggregate.ID,
		Kind:      "threshold_crossed",
		WindowID:  aggregate.ID,
		Message:   "SUM(amount) exceeded the configured alert threshold in a 10-second window.",
		Sum:       aggregate.Sum,
		Threshold: threshold,
		CreatedAt: time.Now().UTC(),
	}
	e.alertedWindows[aggregate.ID] = true
	e.alerts = appendBounded(e.alerts, alert, 100)
	e.mu.Unlock()
	e.notify(ctx, alert)
}

func (e *Engine) PipelineConfig() PipelineConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.pipelineConfigLocked()
}

func (e *Engine) UpdatePipelineConfig(update PipelineConfigUpdate) (PipelineConfig, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if update.AlertThreshold != nil {
		if *update.AlertThreshold <= 0 {
			return PipelineConfig{}, errors.New("alert threshold must be positive")
		}
		e.config.Threshold = *update.AlertThreshold
	}
	return e.pipelineConfigLocked(), nil
}

func (e *Engine) pipelineConfigLocked() PipelineConfig {
	slide := e.config.Window.Slide
	if e.config.Window.Kind == windows.Tumbling {
		slide = e.config.Window.Size
	}
	return PipelineConfig{
		PipelineID: e.config.PipelineID, AlertThreshold: e.config.Threshold,
		WindowKind: string(e.config.Window.Kind), WindowSize: e.config.Window.Size.String(),
		SlidingEvery: slide.String(), AllowedLateness: e.config.Window.AllowedLateness.String(),
		LateEventPolicy: string(e.config.Window.LatePolicy), QueueCapacity: e.config.QueueCapacity,
	}
}

func (e *Engine) recordRecoveryFailure(ctx context.Context, cause error) {
	alert := Alert{ID: "recovery-" + fmt.Sprint(time.Now().UnixNano()), Kind: "recovery_failed", Message: "Checkpoint or recovery operation failed: " + cause.Error(), CreatedAt: time.Now().UTC()}
	e.mu.Lock()
	e.metrics.RecoveryFailures++
	e.alerts = appendBounded(e.alerts, alert, 100)
	e.mu.Unlock()
	e.notify(ctx, alert)
}

func (e *Engine) notify(ctx context.Context, alert Alert) {
	if e.notifier == nil {
		return
	}
	go func() { _ = e.notifier.Notify(ctx, alert) }()
}

func (e *Engine) Snapshot() (Snapshot, error) {
	aggregates, err := e.state.List()
	if err != nil {
		return Snapshot{}, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Snapshot{
		PipelineID:     e.config.PipelineID,
		Status:         map[bool]string{true: "running", false: "idle"}[e.running],
		StartedAt:      e.startedAt,
		Watermark:      e.tracker.Watermark(),
		QueueDepth:     e.queue.Depth(),
		QueueCapacity:  e.queue.Capacity(),
		Metrics:        e.metrics,
		Checkpoint:     e.lastCheckpoint,
		Windows:        aggregates,
		LateEvents:     append([]ingestion.Event(nil), e.lateEvents...),
		Alerts:         append([]Alert(nil), e.alerts...),
		RecoveryPolicy: "RocksDB state is durable before latest checkpoint advances; Kafka commits only after checkpoint success; persisted event IDs make source redelivery idempotent.",
	}, nil
}

func (e *Engine) Close() { e.state.Close() }

func appendBounded[T any](items []T, item T, maximum int) []T {
	items = append(items, item)
	if len(items) > maximum {
		return items[len(items)-maximum:]
	}
	return items
}
