package operators

import (
	"testing"
	"time"

	"github.com/manus/streamweaver/ingestion"
)

func TestPipelineFiltersAndMapsIncrementally(t *testing.T) {
	pipeline := NewPipeline(
		Filter(func(event ingestion.Event) bool { return event.Amount > 0 }),
		Map(func(event ingestion.Event) ingestion.Event { event.Key = "normalized-" + event.Key; return event }),
	)

	filtered, keep, err := pipeline.Apply(ingestion.Event{ID: "zero", Key: "a", Amount: 0, EventTime: time.Now()})
	if err != nil || keep || filtered.ID != "zero" {
		t.Fatalf("expected zero amount event to be filtered, got keep=%v event=%+v err=%v", keep, filtered, err)
	}

	mapped, keep, err := pipeline.Apply(ingestion.Event{ID: "positive", Key: "a", Amount: 12, EventTime: time.Now()})
	if err != nil || !keep || mapped.Key != "normalized-a" {
		t.Fatalf("expected mapped positive event, got keep=%v event=%+v err=%v", keep, mapped, err)
	}
}
