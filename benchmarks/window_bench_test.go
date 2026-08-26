package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"github.com/manus/streamweaver/ingestion"
	"github.com/manus/streamweaver/operators"
	"github.com/manus/streamweaver/state"
	"github.com/manus/streamweaver/storage"
	"github.com/manus/streamweaver/windows"
)

func BenchmarkTumblingWindowAssignment(b *testing.B) {
	tracker, err := windows.NewTracker(windows.Config{Kind: windows.Tumbling, Size: 10 * time.Second, AllowedLateness: 2 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now().UTC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Observe(now.Add(time.Duration(i) * time.Millisecond))
	}
}

func BenchmarkFilterMapOperatorPipeline(b *testing.B) {
	pipeline := operators.NewPipeline(
		operators.Filter(func(event ingestion.Event) bool { return event.Amount > 0 }),
		operators.Map(func(event ingestion.Event) ingestion.Event { event.Key = "normalized-" + event.Key; return event }),
	)
	event := ingestion.Event{ID: "operator-benchmark", Key: "merchant", Amount: 12, EventTime: time.Now().UTC()}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, keep, err := pipeline.Apply(event); err != nil || !keep {
			b.Fatal(err)
		}
	}
}

func BenchmarkRocksDBAggregateApply(b *testing.B) {
	rocks, err := storage.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer rocks.Close()
	store := state.NewStore(rocks)
	window := windows.Window{ID: "benchmark-window", Start: time.Now().UTC().Truncate(10 * time.Second), End: time.Now().UTC().Add(10 * time.Second)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := store.Apply(ingestion.Event{ID: fmt.Sprintf("event-%d", i), Key: "merchant", Amount: 10, EventTime: time.Now().UTC()}, []windows.Window{window})
		if err != nil {
			b.Fatal(err)
		}
	}
}
