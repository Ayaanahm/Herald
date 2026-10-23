// Package provider simulates the external push, email and SMS services that a
// real notification platform would call (APNs/FCM, SES, Twilio...).
//
// It exists so delivery, retries, fallback and the circuit breaker can be
// exercised and load-tested without sending real messages. Each channel has
// configurable latency and failure rate, and can be taken down entirely at
// runtime (POST /v1/chaos) for demos and fault-injection tests.
//
// Like real providers, it accepts an idempotency key: a repeat of a key that
// was already delivered returns success without sending again. It also checks
// the worker's fencing token, so a stale worker that lost its lock cannot send.
// It counts both cases, which is how the load tests measure duplicate sends.
package provider

import (
	"encoding/json"
	"hash/fnv"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ChannelConfig struct {
	LatencyMs int     `json:"latency_ms"` // mean latency; actual is uniform in [0.5x, 1.5x]
	FailRate  float64 `json:"fail_rate"`  // probability of a 503
	Down      bool    `json:"down"`       // every request fails with 503
}

type channelStats struct {
	Sent        atomic.Int64 // unique messages actually sent
	Failed      atomic.Int64 // 5xx returned
	Rejected    atomic.Int64 // 4xx returned (invalid recipient)
	Duplicates  atomic.Int64 // idempotency key already delivered: suppressed
	StaleFenced atomic.Int64 // fencing token older than one already seen: rejected
}

type entry struct {
	fence     int64
	delivered bool
}

const shards = 64

type shard struct {
	mu sync.Mutex
	m  map[string]*entry
}

type Server struct {
	mu     sync.RWMutex
	config map[string]ChannelConfig
	stats  map[string]*channelStats
	keys   [shards]*shard
}

func DefaultConfig() map[string]ChannelConfig {
	return map[string]ChannelConfig{
		"push":  {LatencyMs: 15, FailRate: 0.02},
		"email": {LatencyMs: 40, FailRate: 0.02},
		"sms":   {LatencyMs: 60, FailRate: 0.02},
	}
}

func NewServer(cfg map[string]ChannelConfig) *Server {
	s := &Server{config: cfg, stats: map[string]*channelStats{}}
	for ch := range cfg {
		s.stats[ch] = &channelStats{}
	}
	s.resetKeys()
	return s
}

func (s *Server) resetKeys() {
	for i := range s.keys {
		s.keys[i] = &shard{m: map[string]*entry{}}
	}
}

func (s *Server) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.keys[h.Sum32()%shards]
}

type SendRequest struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Fence          int64           `json:"fence"`
	Recipient      string          `json:"recipient"`
	Payload        json.RawMessage `json:"payload"`
}

type SendResponse struct {
	Duplicate bool   `json:"duplicate,omitempty"`
	Error     string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/send/{channel}", s.send)
	mux.HandleFunc("GET /v1/chaos", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		writeJSON(w, 200, s.config)
	})
	mux.HandleFunc("POST /v1/chaos", s.chaos)
	mux.HandleFunc("GET /v1/stats", s.statsHandler)
	mux.HandleFunc("POST /v1/reset", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		for ch := range s.stats {
			s.stats[ch] = &channelStats{}
		}
		s.resetKeys()
		s.mu.Unlock()
		writeJSON(w, 200, map[string]bool{"ok": true})
	})
	return mux
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	s.mu.RLock()
	cfg, ok := s.config[ch]
	st := s.stats[ch]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, 404, SendResponse{Error: "unknown channel"})
		return
	}
	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IdempotencyKey == "" {
		writeJSON(w, 400, SendResponse{Error: "bad request"})
		return
	}

	if cfg.LatencyMs > 0 {
		d := time.Duration(float64(cfg.LatencyMs)*(0.5+rand.Float64())) * time.Millisecond
		time.Sleep(d)
	}
	if cfg.Down || rand.Float64() < cfg.FailRate {
		st.Failed.Add(1)
		writeJSON(w, 503, SendResponse{Error: "provider unavailable"})
		return
	}
	// Recipients whose ID starts with "invalid-" have no valid address on
	// this channel: a permanent 4xx, which should trigger fallback, not retries.
	if strings.HasPrefix(req.Recipient, "invalid-"+ch) || strings.HasPrefix(req.Recipient, "invalid-all") {
		st.Rejected.Add(1)
		writeJSON(w, 422, SendResponse{Error: "invalid recipient for " + ch})
		return
	}

	sh := s.shardFor(req.IdempotencyKey)
	sh.mu.Lock()
	e := sh.m[req.IdempotencyKey]
	if e == nil {
		e = &entry{}
		sh.m[req.IdempotencyKey] = e
	}
	switch {
	case req.Fence < e.fence:
		sh.mu.Unlock()
		st.StaleFenced.Add(1)
		writeJSON(w, 409, SendResponse{Error: "stale fencing token"})
		return
	case e.delivered:
		e.fence = req.Fence
		sh.mu.Unlock()
		st.Duplicates.Add(1)
		writeJSON(w, 200, SendResponse{Duplicate: true})
		return
	}
	e.fence = req.Fence
	e.delivered = true
	sh.mu.Unlock()
	st.Sent.Add(1)
	writeJSON(w, 200, SendResponse{})
}

type chaosRequest struct {
	Channel   string   `json:"channel"`
	LatencyMs *int     `json:"latency_ms"`
	FailRate  *float64 `json:"fail_rate"`
	Down      *bool    `json:"down"`
}

func (s *Server) chaos(w http.ResponseWriter, r *http.Request) {
	var req chaosRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.config[req.Channel]
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "unknown channel"})
		return
	}
	if req.LatencyMs != nil {
		cfg.LatencyMs = *req.LatencyMs
	}
	if req.FailRate != nil {
		cfg.FailRate = *req.FailRate
	}
	if req.Down != nil {
		cfg.Down = *req.Down
	}
	s.config[req.Channel] = cfg
	writeJSON(w, 200, s.config)
}

type StatsView struct {
	Sent        int64 `json:"sent"`
	Failed      int64 `json:"failed"`
	Rejected    int64 `json:"rejected"`
	Duplicates  int64 `json:"duplicates_suppressed"`
	StaleFenced int64 `json:"stale_fenced"`
}

func (s *Server) Stats() map[string]StatsView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]StatsView{}
	for ch, st := range s.stats {
		out[ch] = StatsView{st.Sent.Load(), st.Failed.Load(), st.Rejected.Load(), st.Duplicates.Load(), st.StaleFenced.Load()}
	}
	return out
}

func (s *Server) statsHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.Stats())
}
