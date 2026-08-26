package ingestion

import (
	"context"
	"errors"
)

var ErrBackpressure = errors.New("bounded input buffer is full; producer must slow down")

type Submission struct {
	Event Event
	Ack   chan error
}

// BoundedQueue is the explicit backpressure boundary between producers and operators.
// Enqueue never allocates an unbounded backlog; a full queue rejects the producer.
type BoundedQueue struct {
	items chan Submission
}

func NewBoundedQueue(capacity int) *BoundedQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &BoundedQueue{items: make(chan Submission, capacity)}
}

func (q *BoundedQueue) Enqueue(ctx context.Context, event Event) (<-chan error, error) {
	ack := make(chan error, 1)
	submission := Submission{Event: event, Ack: ack}
	select {
	case q.items <- submission:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, ErrBackpressure
	}
}

func (q *BoundedQueue) Next(ctx context.Context) (Submission, error) {
	select {
	case next := <-q.items:
		return next, nil
	case <-ctx.Done():
		return Submission{}, ctx.Err()
	}
}

func (q *BoundedQueue) Depth() int    { return len(q.items) }
func (q *BoundedQueue) Capacity() int { return cap(q.items) }
