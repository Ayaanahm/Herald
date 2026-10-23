package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/Ayaanahm/Herald/internal/events"
)

var (
	mWSClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "herald_ws_clients", Help: "Connected WebSocket clients.",
	})
	mWSDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "herald_ws_dropped_total", Help: "Messages dropped for slow WebSocket clients.",
	})
)

// client is one WebSocket connection. Its send buffer is bounded: a slow
// client loses messages (and is told how many) instead of slowing the hub.
type client struct {
	tenant         string // "" = admin, sees every tenant
	notificationID string // optional: only events for this notification
	send           chan []byte
	dropped        atomic.Int64
}

// Hub fans events out from Redis Pub/Sub to WebSocket clients. Every API
// replica runs one, so a client sees events from all workers no matter which
// replica it is connected to.
type Hub struct {
	rdb      *redis.Client
	provider func(ctx context.Context) (json.RawMessage, error)
	mu       sync.RWMutex
	clients  map[*client]struct{}
}

func NewHub(rdb *redis.Client, provider func(ctx context.Context) (json.RawMessage, error)) *Hub {
	return &Hub{rdb: rdb, provider: provider, clients: map[*client]struct{}{}}
}

func (h *Hub) Run(ctx context.Context) {
	go h.statsLoop(ctx)
	for ctx.Err() == nil {
		sub := h.rdb.Subscribe(ctx, events.Channel)
		ch := sub.Channel(redis.WithChannelSize(1000))
		stop := context.AfterFunc(ctx, func() { sub.Close() }) // ends the range below on shutdown
		for msg := range ch {
			var evs []events.Event
			if err := json.Unmarshal([]byte(msg.Payload), &evs); err != nil {
				continue
			}
			h.dispatch(evs)
		}
		stop()
		sub.Close()
		if ctx.Err() == nil {
			slog.Warn("hub: redis subscription ended, resubscribing")
			time.Sleep(time.Second)
		}
	}
}

func (h *Hub) dispatch(evs []events.Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		var mine []events.Event
		for _, e := range evs {
			if c.tenant != "" && e.Tenant != c.tenant {
				continue
			}
			if c.notificationID != "" && e.ID != c.notificationID {
				continue
			}
			mine = append(mine, e)
		}
		if len(mine) == 0 {
			continue
		}
		if c.notificationID == "" && len(mine) > 50 {
			mine = mine[len(mine)-50:] // a live feed only needs the latest events
		}
		b, _ := json.Marshal(map[string]any{"type": "events", "events": mine})
		h.offer(c, b)
	}
}

func (h *Hub) offer(c *client, b []byte) {
	select {
	case c.send <- b:
	default:
		c.dropped.Add(1)
		mWSDropped.Inc()
	}
}

// statsLoop pushes aggregate counters to admin clients twice a second.
func (h *Hub) statsLoop(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	var provStats json.RawMessage
	var lastProv time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h.mu.RLock()
		admins := 0
		for c := range h.clients {
			if c.tenant == "" {
				admins++
			}
		}
		h.mu.RUnlock()
		if admins == 0 {
			continue
		}
		snap, err := events.ReadSnapshot(ctx, h.rdb)
		if err != nil {
			continue
		}
		if h.provider != nil && time.Since(lastProv) > time.Second {
			pctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			if p, err := h.provider(pctx); err == nil {
				provStats = p
			}
			cancel()
			lastProv = time.Now()
		}
		b, _ := json.Marshal(map[string]any{"type": "stats", "snapshot": snap, "provider": provStats})
		h.mu.RLock()
		for c := range h.clients {
			if c.tenant == "" {
				h.offer(c, b)
			}
		}
		h.mu.RUnlock()
	}
}

// Serve upgrades the request and streams to the client until it disconnects.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, tenant, notificationID string) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	c := &client{tenant: tenant, notificationID: notificationID, send: make(chan []byte, 256)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	mWSClients.Inc()
	defer func() {
		h.mu.Lock()
		delete(h.clients, c)
		h.mu.Unlock()
		mWSClients.Dec()
	}()

	ctx := conn.CloseRead(r.Context()) // we don't expect client messages; this handles pings/close
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "")
			return
		case b := <-c.send:
			if d := c.dropped.Swap(0); d > 0 {
				note, _ := json.Marshal(map[string]any{"type": "dropped", "count": d})
				if err := write(ctx, conn, note); err != nil {
					return
				}
			}
			if err := write(ctx, conn, b); err != nil {
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func write(ctx context.Context, conn *websocket.Conn, b []byte) error {
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}
