package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/manus/streamweaver/ingestion"
	"github.com/segmentio/kafka-go"
)

func main() {
	interval, err := time.ParseDuration(env("DEMO_INTERVAL", "700ms"))
	if err != nil {
		log.Fatal(err)
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")...),
		Topic:        env("KAFKA_TOPIC", "transactions"),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var sequence int64
	for range ticker.C {
		sequence++
		amount := 180 + rand.Float64()*300
		if sequence%12 == 0 {
			// A deterministic spike crosses the configured demo alert threshold.
			amount = 4_200
		}
		event := ingestion.Event{
			ID:        fmt.Sprintf("demo-%d", sequence),
			Stream:    "transactions",
			Key:       []string{"merchant-orbit", "merchant-nova", "merchant-lumen"}[sequence%3],
			Amount:    amount,
			EventTime: time.Now().UTC(),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			log.Printf("encode event: %v", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.Key), Value: payload, Time: event.EventTime})
		cancel()
		if err != nil {
			log.Printf("write demo transaction: %v", err)
		}
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
