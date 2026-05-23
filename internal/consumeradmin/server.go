package consumeradmin

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"
)

// Server is the admin HTTP server each consumer runs on localhost.
// Exposes:
//
//	GET /skip-count  → {"count": N, "by_class": {class: N, …}}
//	GET /healthz     → 200 OK (also healthy while retrying infra;
//	                   only the default-case crash signals unhealthy
//	                   — see ADR 0006 Notes)
type Server struct {
	addr     string
	counter  *SkipCounter
	classes  []string // known class keys, always present in the JSON
}

// NewServer wires the counter and the set of always-present class
// keys (so the TUI sees a stable shape even before any skips happen).
func NewServer(addr string, counter *SkipCounter, classes []string) *Server {
	return &Server{addr: addr, counter: counter, classes: classes}
}

// Run starts the admin server and blocks until ctx is cancelled or the
// server errors out. Returns nil on clean shutdown. Designed to be
// called in its own goroutine.
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/skip-count", s.handleSkipCount)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		log.Printf("admin: server error: %v", err)
		return err
	}
}

func (s *Server) handleSkipCount(w http.ResponseWriter, _ *http.Request) {
	total, observed := s.counter.Snapshot()
	keys := classes(s.classes, observed)
	by := make(map[string]int, len(keys))
	for _, k := range keys {
		by[k] = observed[k] // 0 if unobserved, which is the point
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Count   int            `json:"count"`
		ByClass map[string]int `json:"by_class"`
	}{total, by})
}
