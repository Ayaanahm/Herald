// Package api is Herald's HTTP and WebSocket front end.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Ayaanahm/herald/internal/config"
	"github.com/Ayaanahm/herald/internal/events"
	"github.com/Ayaanahm/herald/internal/ratelimit"
	"github.com/Ayaanahm/herald/internal/store"
	"github.com/Ayaanahm/herald/web"
)

var (
	mRequests = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herald_http_request_seconds", Help: "HTTP request latency by route and status.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 15),
	}, []string{"route", "code"})
	mRateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herald_rate_limited_total", Help: "Requests rejected by the rate limiter.",
	}, []string{"tenant"})
)

var validChannels = map[string]bool{"push": true, "email": true, "sms": true}

type Server struct {
	cfg      config.Config
	store    *store.Store
	rdb      *redis.Client
	limiter  ratelimit.Limiter
	batcher  *Batcher
	outbox   *Outbox
	hub      *Hub
	events   *events.Publisher
	adminKey string
	http     *http.Client
}

func NewServer(cfg config.Config, st *store.Store, rdb *redis.Client, lim ratelimit.Limiter,
	b *Batcher, ob *Outbox, hub *Hub, ev *events.Publisher, adminKey string) *Server {
	return &Server{cfg: cfg, store: st, rdb: rdb, limiter: lim, batcher: b, outbox: ob, hub: hub, events: ev,
		adminKey: adminKey, http: &http.Client{Timeout: 2 * time.Second}}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/notifications", s.instrument("create", s.create))
	mux.Handle("GET /v1/notifications/{id}", s.instrument("get", s.get))
	mux.Handle("DELETE /v1/notifications/{id}", s.instrument("cancel", s.cancel))
	mux.HandleFunc("GET /v1/ws", s.ws)
	mux.HandleFunc("POST /v1/admin/chaos", s.adminChaos)
	mux.HandleFunc("POST /v1/admin/reset-stats", s.adminReset)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /", http.FileServerFS(web.FS))
	return mux
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(c int) { r.code = c; r.ResponseWriter.WriteHeader(c) }

func (s *Server) instrument(route string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		h(rec, r)
		mRequests.WithLabelValues(route, strconv.Itoa(rec.code)).Observe(time.Since(t0).Seconds())
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func apiError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) tenant(r *http.Request) (config.Tenant, bool) {
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if key == "" {
		key = r.URL.Query().Get("api_key") // browsers can't set headers on WebSocket requests
	}
	t, ok := s.cfg.Tenants[key]
	return t, ok
}

type createRequest struct {
	RecipientID string          `json:"recipient_id"`
	Channels    []string        `json:"channels"`
	CollapseKey string          `json:"collapse_key"`
	Payload     json.RawMessage `json:"payload"`
}

func (req *createRequest) validate() error {
	if req.RecipientID == "" || len(req.RecipientID) > 128 {
		return errors.New("recipient_id is required (max 128 chars)")
	}
	if len(req.Channels) == 0 {
		req.Channels = []string{"push", "email", "sms"}
	}
	if len(req.Channels) > 3 {
		return errors.New("at most 3 channels")
	}
	seen := map[string]bool{}
	for _, c := range req.Channels {
		if !validChannels[c] {
			return errors.New("unknown channel " + strconv.Quote(c) + " (use push, email, sms)")
		}
		if seen[c] {
			return errors.New("duplicate channel " + strconv.Quote(c))
		}
		seen[c] = true
	}
	if len(req.CollapseKey) > 128 {
		return errors.New("collapse_key max 128 chars")
	}
	if len(req.Payload) == 0 || req.Payload[0] != '{' {
		return errors.New("payload must be a JSON object")
	}
	if len(req.Payload) > 16<<10 {
		return errors.New("payload max 16 KiB")
	}
	return nil
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(r)
	if !ok {
		apiError(w, http.StatusUnauthorized, "missing or invalid API key")
		return
	}

	rl, err := s.limiter.Allow(r.Context(), t.ID, t.RateLimit, t.RateWindow)
	if err != nil {
		// Fail open: losing Redis should degrade rate limiting, not take ingestion down.
		slog.Warn("rate limiter unavailable, allowing request", "err", err)
	} else {
		h := w.Header()
		h.Set("X-RateLimit-Limit", strconv.Itoa(rl.Limit))
		h.Set("X-RateLimit-Remaining", strconv.Itoa(rl.Remaining))
		if !rl.Allowed {
			secs := int((rl.RetryAfter + time.Second - 1) / time.Second)
			h.Set("Retry-After", strconv.Itoa(secs))
			mRateLimited.WithLabelValues(t.ID).Inc()
			s.events.Count("rate_limited", 1)
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error": "rate limit exceeded", "retry_after_ms": rl.RetryAfter.Milliseconds(),
			})
			return
		}
	}

	var req createRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	if err := dec.Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.validate(); err != nil {
		apiError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	idem := r.Header.Get("Idempotency-Key")
	if len(idem) > 128 {
		apiError(w, http.StatusBadRequest, "Idempotency-Key max 128 chars")
		return
	}

	id, _ := uuid.NewV7()
	ins, err := s.batcher.Submit(r.Context(), store.NewNotification{
		ID: id, TenantID: t.ID, IdempotencyKey: idem, RecipientID: req.RecipientID,
		Channels: req.Channels, CollapseKey: req.CollapseKey, Payload: req.Payload,
	})
	if errors.Is(err, ErrOverloaded) {
		w.Header().Set("Retry-After", "1")
		apiError(w, http.StatusServiceUnavailable, "overloaded, retry shortly")
		return
	}
	if err != nil {
		slog.Error("insert failed", "err", err)
		apiError(w, http.StatusServiceUnavailable, "could not store notification")
		return
	}
	code := http.StatusAccepted
	if ins.Replayed {
		code = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/v1/notifications/"+ins.ID.String())
	writeJSON(w, code, map[string]any{"id": ins.ID, "status": ins.Status, "created_at": ins.CreatedAt})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(r)
	if !ok {
		apiError(w, http.StatusUnauthorized, "missing or invalid API key")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	n, err := s.store.Get(r.Context(), t.ID, id)
	if errors.Is(err, store.ErrNotFound) {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		apiError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tenant(r)
	if !ok {
		apiError(w, http.StatusUnauthorized, "missing or invalid API key")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		apiError(w, http.StatusNotFound, "not found")
		return
	}
	status, err := s.store.Cancel(r.Context(), t.ID, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		apiError(w, http.StatusNotFound, "not found")
	case err != nil:
		apiError(w, http.StatusServiceUnavailable, "database unavailable")
	case status == store.StatusCancelled:
		writeJSON(w, http.StatusOK, map[string]string{"id": id.String(), "status": status})
	default:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cannot cancel a notification that is " + status, "status": status})
	}
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("api_key")
	if key != "" && key == s.adminKey {
		s.hub.Serve(w, r, "", r.URL.Query().Get("notification_id"))
		return
	}
	t, ok := s.tenant(r)
	if !ok {
		apiError(w, http.StatusUnauthorized, "missing or invalid API key")
		return
	}
	s.hub.Serve(w, r, t.ID, r.URL.Query().Get("notification_id"))
}

func (s *Server) isAdmin(r *http.Request) bool {
	k := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if k == "" {
		k = r.URL.Query().Get("api_key")
	}
	return s.adminKey != "" && k == s.adminKey
}

// adminChaos forwards fault-injection settings to the mock provider.
func (s *Server) adminChaos(w http.ResponseWriter, r *http.Request) {
	if !s.isAdmin(r) {
		apiError(w, http.StatusUnauthorized, "admin key required")
		return
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, s.cfg.ProviderURL+"/v1/chaos", io.LimitReader(r.Body, 4096))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		apiError(w, http.StatusBadGateway, "provider unreachable")
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Server) adminReset(w http.ResponseWriter, r *http.Request) {
	if !s.isAdmin(r) {
		apiError(w, http.StatusUnauthorized, "admin key required")
		return
	}
	if err := events.Reset(r.Context(), s.rdb); err != nil {
		apiError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.store.Pool.Ping(ctx); err != nil {
		apiError(w, http.StatusServiceUnavailable, "postgres: "+err.Error())
		return
	}
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		apiError(w, http.StatusServiceUnavailable, "redis: "+err.Error())
		return
	}
	w.Write([]byte("ready"))
}

// ProviderStats fetches the mock provider's counters for the dashboard.
func ProviderStats(client *http.Client, base string) func(ctx context.Context) (json.RawMessage, error) {
	return func(ctx context.Context) (json.RawMessage, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/stats", nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return json.RawMessage(b), err
	}
}
