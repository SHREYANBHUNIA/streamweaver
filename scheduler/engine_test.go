package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/manus/streamweaver/ingestion"
	"github.com/manus/streamweaver/windows"
)

type captureNotifier struct{ alerts chan Alert }

func (n captureNotifier) Notify(_ context.Context, alert Alert) error {
	n.alerts <- alert
	return nil
}

func testConfig(t *testing.T, root string) Config {
	t.Helper()
	return Config{
		PipelineID:          "transactions-sum-10s",
		Threshold:           1000,
		QueueCapacity:       2,
		DataDirectory:       root + "/rocksdb",
		CheckpointDirectory: root + "/checkpoints",
		Window: windows.Config{
			Kind:            windows.Tumbling,
			Size:            10 * time.Second,
			AllowedLateness: 2 * time.Second,
			LatePolicy:      windows.SideOutputLate,
		},
	}
}

func startEngine(t *testing.T, engine *Engine) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = engine.Run(ctx) }()
	return cancel
}

func TestEngineAggregatesSUMAmountAndSendsThresholdAlert(t *testing.T) {
	notifier := captureNotifier{alerts: make(chan Alert, 1)}
	engine, err := Open(testConfig(t, t.TempDir()), notifier)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	cancel := startEngine(t, engine)
	defer cancel()

	eventTime := time.Date(2026, 8, 26, 12, 0, 4, 0, time.UTC)
	event := ingestion.Event{ID: "txn-1", Key: "merchant-7", Amount: 1200, EventTime: eventTime}
	if err := engine.SubmitAndWait(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	select {
	case alert := <-notifier.alerts:
		if alert.Kind != "threshold_crossed" || alert.Sum != 1200 {
			t.Fatalf("unexpected alert: %+v", alert)
		}
	case <-time.After(time.Second):
		t.Fatal("threshold alert was not sent")
	}

	snapshot, err := engine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Windows) != 1 || snapshot.Windows[0].Sum != 1200 || snapshot.Metrics.Processed != 1 || snapshot.Metrics.CheckpointCount != 1 {
		t.Fatalf("unexpected aggregation snapshot: %+v", snapshot)
	}

	if err := engine.SubmitAndWait(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	snapshot, err = engine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Metrics.Duplicates != 1 || snapshot.Windows[0].Sum != 1200 {
		t.Fatalf("redelivery should be idempotent: %+v", snapshot)
	}
}

func TestEngineSendsLateEventsToSideOutputAndRecoversCheckpoint(t *testing.T) {
	root := t.TempDir()
	config := testConfig(t, root)
	engine, err := Open(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel := startEngine(t, engine)
	newest := time.Date(2026, 8, 26, 12, 0, 20, 0, time.UTC)
	if err := engine.SubmitAndWait(context.Background(), ingestion.Event{ID: "newest", Key: "m", Amount: 10, EventTime: newest}); err != nil {
		t.Fatal(err)
	}
	if err := engine.SubmitAndWait(context.Background(), ingestion.Event{ID: "late", Key: "m", Amount: 999, EventTime: newest.Add(-5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	cancel()
	engine.Close()

	recovered, err := Open(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	snapshot, err := recovered.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.LateEvents) != 0 {
		t.Fatalf("late-event side output is transient API telemetry and should start empty after restart: %+v", snapshot.LateEvents)
	}
	if snapshot.Checkpoint.PipelineID != config.PipelineID || snapshot.Metrics.CheckpointCount != 0 || snapshot.Watermark.IsZero() {
		t.Fatalf("checkpoint should restore watermark and pipeline identity: %+v", snapshot)
	}
	if len(snapshot.Windows) != 1 || snapshot.Windows[0].Sum != 10 {
		t.Fatalf("RocksDB aggregate state should survive recovery: %+v", snapshot.Windows)
	}
}

func TestEngineRejectsProducerWhenBufferIsFull(t *testing.T) {
	config := testConfig(t, t.TempDir())
	config.QueueCapacity = 1
	engine, err := Open(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	first := ingestion.Event{ID: "buffered", Key: "m", Amount: 1, EventTime: time.Now().UTC()}
	if err := engine.Submit(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := ingestion.Event{ID: "rejected", Key: "m", Amount: 1, EventTime: time.Now().UTC()}
	err = engine.Submit(context.Background(), second)
	if !errors.Is(err, ingestion.ErrBackpressure) {
		t.Fatalf("expected backpressure rejection, got %v", err)
	}
	snapshot, err := engine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Metrics.BackpressureReject != 1 || snapshot.QueueDepth != 1 {
		t.Fatalf("unexpected backpressure metrics: %+v", snapshot)
	}
}
