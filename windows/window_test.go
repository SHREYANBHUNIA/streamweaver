package windows

import (
	"testing"
	"time"
)

func TestTumblingWindowTracksWatermarkAndLateEvent(t *testing.T) {
	tracker, err := NewTracker(Config{Kind: Tumbling, Size: 10 * time.Second, AllowedLateness: 2 * time.Second, LatePolicy: SideOutputLate})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 26, 12, 0, 10, 0, time.UTC)
	first := tracker.Observe(base)
	if first.IsLate || !first.Watermark.Equal(base.Add(-2*time.Second)) || len(first.Windows) != 1 {
		t.Fatalf("unexpected first observation: %+v", first)
	}
	late := tracker.Observe(base.Add(-5 * time.Second))
	if !late.IsLate {
		t.Fatalf("expected event behind watermark to be late: %+v", late)
	}
}

func TestSlidingWindowAssignsOverlappingRanges(t *testing.T) {
	tracker, err := NewTracker(Config{Kind: Sliding, Size: 10 * time.Second, Slide: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	observation := tracker.Observe(time.Date(2026, 8, 26, 12, 0, 12, 0, time.UTC))
	if len(observation.Windows) != 2 {
		t.Fatalf("expected two overlapping windows, got %d: %+v", len(observation.Windows), observation.Windows)
	}
}
