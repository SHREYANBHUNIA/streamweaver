package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/manus/streamweaver/api"
	"github.com/manus/streamweaver/ingestion"
	"github.com/manus/streamweaver/scheduler"
	"github.com/manus/streamweaver/windows"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dataDirectory := env("STREAMWEAVER_DATA_DIR", "./data/rocksdb")
	checkpointDirectory := env("STREAMWEAVER_CHECKPOINT_DIR", "./data/checkpoints")
	engine, err := scheduler.Open(scheduler.Config{
		PipelineID:          "transactions-sum-10s",
		Threshold:           envFloat("ALERT_THRESHOLD", 1000),
		QueueCapacity:       1024,
		DataDirectory:       dataDirectory,
		CheckpointDirectory: checkpointDirectory,
		Window:              windows.Config{Kind: windows.Tumbling, Size: 10 * time.Second, AllowedLateness: 2 * time.Second, LatePolicy: windows.SideOutputLate},
	}, scheduler.WebhookNotifier{URL: os.Getenv("OWNER_ALERT_WEBHOOK_URL"), Token: os.Getenv("OWNER_ALERT_TOKEN")})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	go func() {
		if err := engine.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("engine stopped: %v", err)
		}
	}()

	if brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); brokers != "" {
		consumer := ingestion.KafkaConsumer{Brokers: strings.Split(brokers, ","), Topic: env("KAFKA_TOPIC", "transactions"), GroupID: env("KAFKA_GROUP_ID", "streamweaver")}
		go func() {
			if err := consumer.Run(ctx, engine); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("kafka consumer stopped: %v", err)
			}
		}()
	}

	server := &http.Server{Addr: env("STREAMWEAVER_HTTP_ADDR", ":8080"), Handler: api.NewServer(engine).Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	var value float64
	if _, err := fmt.Sscan(os.Getenv(key), &value); err == nil && value > 0 {
		return value
	}
	return fallback
}
