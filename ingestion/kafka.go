package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// CheckpointingProcessor only returns success after state has been persisted and
// the corresponding checkpoint manifest has been atomically advanced.
type CheckpointingProcessor interface {
	SubmitAndWait(context.Context, Event) error
}

type KafkaConsumer struct {
	Brokers []string
	Topic   string
	GroupID string
}

func (c KafkaConsumer) Run(ctx context.Context, processor CheckpointingProcessor) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.Brokers,
		Topic:          c.Topic,
		GroupID:        c.GroupID,
		CommitInterval: 0,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
	})
	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var event Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return fmt.Errorf("decode kafka transaction at %s[%d]/%d: %w", message.Topic, message.Partition, message.Offset, err)
		}
		event.Topic = message.Topic
		event.Partition = message.Partition
		event.Offset = message.Offset
		if err := processor.SubmitAndWait(ctx, event); err != nil {
			// Do not commit. Kafka redelivers after restart and the engine's event ID
			// ledger makes the aggregate update idempotent.
			return err
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit kafka offset: %w", err)
		}
	}
}
