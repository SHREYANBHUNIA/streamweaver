package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/manus/streamweaver/ingestion"
	"github.com/manus/streamweaver/scheduler"
)

type Server struct{ engine *scheduler.Engine }

func NewServer(engine *scheduler.Engine) *Server { return &Server{engine: engine} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", s.health)
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /v1/pipeline", s.pipelineConfig)
	mux.HandleFunc("PUT /v1/pipeline", s.updatePipelineConfig)
	mux.HandleFunc("GET /v1/windows", s.windows)
	mux.HandleFunc("GET /v1/alerts", s.alerts)
	mux.HandleFunc("POST /v1/transactions", s.submitTransaction)
	return cors(mux)
}

func (s *Server) health(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"status": "ok", "service": "streamweaver-go-api"})
}

func (s *Server) status(writer http.ResponseWriter, request *http.Request) {
	snapshot, err := s.engine.Snapshot()
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

func (s *Server) pipelineConfig(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, s.engine.PipelineConfig())
}

func (s *Server) updatePipelineConfig(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	var update scheduler.PipelineConfigUpdate
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20)).Decode(&update); err != nil {
		writeError(writer, http.StatusBadRequest, fmt.Errorf("decode pipeline configuration: %w", err))
		return
	}
	config, err := s.engine.UpdatePipelineConfig(update)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	writeJSON(writer, http.StatusOK, config)
}

func (s *Server) windows(writer http.ResponseWriter, request *http.Request) {
	snapshot, err := s.engine.Snapshot()
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, snapshot.Windows)
}

func (s *Server) alerts(writer http.ResponseWriter, request *http.Request) {
	snapshot, err := s.engine.Snapshot()
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, snapshot.Alerts)
}

func (s *Server) submitTransaction(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	var event ingestion.Event
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20)).Decode(&event); err != nil {
		writeError(writer, http.StatusBadRequest, fmt.Errorf("decode transaction: %w", err))
		return
	}
	if event.ID == "" {
		event.ID = fmt.Sprintf("http-%d", time.Now().UnixNano())
	}
	if err := s.engine.SubmitAndWait(request.Context(), event); err != nil {
		writeError(writer, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]string{"status": "accepted", "eventId": event.ID})
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if request.Method == http.MethodOptions {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, map[string]string{"error": err.Error()})
}
