package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/manus/streamweaver/scheduler"
	"github.com/manus/streamweaver/windows"
)

func openTestEngine(t *testing.T) (*scheduler.Engine, context.CancelFunc) {
	t.Helper()
	root := t.TempDir()
	engine, err := scheduler.Open(scheduler.Config{PipelineID: "transactions-sum-10s", Threshold: 1000, QueueCapacity: 8, DataDirectory: root + "/rocksdb", CheckpointDirectory: root + "/checkpoints", Window: windows.Config{Kind: windows.Tumbling, Size: 10 * time.Second, AllowedLateness: 2 * time.Second, LatePolicy: windows.SideOutputLate}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = engine.Run(ctx) }()
	return engine, func() { cancel(); engine.Close() }
}

func TestPipelineConfigurationEndpoints(t *testing.T) {
	engine, closeEngine := openTestEngine(t)
	defer closeEngine()
	handler := NewServer(engine).Handler()

	read := httptest.NewRecorder()
	handler.ServeHTTP(read, httptest.NewRequest(http.MethodGet, "/v1/pipeline", nil))
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), "transactions-sum-10s") {
		t.Fatalf("unexpected read response %d: %s", read.Code, read.Body.String())
	}

	update := httptest.NewRecorder()
	handler.ServeHTTP(update, httptest.NewRequest(http.MethodPut, "/v1/pipeline", strings.NewReader(`{"alertThreshold":2500}`)))
	if update.Code != http.StatusOK || !strings.Contains(update.Body.String(), "2500") {
		t.Fatalf("unexpected update response %d: %s", update.Code, update.Body.String())
	}
}
