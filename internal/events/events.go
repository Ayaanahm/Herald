// Package events carries status changes and counters from workers and API
// replicas to every dashboard/WebSocket client, through Redis.
//
// Nothing here is per-request: events are buffered and published as one JSON
// array every 100 ms, and counters are summed locally and flushed with one
// pipelined HINCRBY batch, so the cost to Redis stays flat as load grows.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	Channel     = "herald:events"
	StatsKey    = "herald:stats"
	LatencyKey  = "herald:latency"  // histogram of end-to-end delivery latency, ms buckets
	BreakersKey = "herald:breakers" // field "<worker>/<channel>" = closed|open|half-open
	maxBuffered = 5000
	flushEvery  = 100 * time.Millisecond
)

// LatencyBuckets are upper bounds in milliseconds.
var LatencyBuckets = []int64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}

type Event struct {
	ID        string `json:"id"`
	Tenant    string `json:"tenant"`
	Status    string `json:"status"`
	Channel   string `json:"channel,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	Error     string `json:"error,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	At        int64  `json:"at"`
}

type Publisher struct {
	rdb      *redis.Client
	mu       sync.Mutex
	buf      []Event
	dropped  int64
	counters map[string]int64
	latency  map[int64]int64
}

func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb, counters: map[string]int64{}, latency: map[int64]int64{}}
}

// Emit queues an event. If the buffer is full the event is dropped (and
// counted): status events are for observation and must never slow delivery.
func (p *Publisher) Emit(e Event) {
	if e.At == 0 {
		e.At = time.Now().UnixMilli()
	}
	p.mu.Lock()
	if len(p.buf) < maxBuffered {
		p.buf = append(p.buf, e)
	} else {
		p.dropped++
	}
	p.mu.Unlock()
}

func (p *Publisher) Count(name string, n int64) {
	p.mu.Lock()
	p.counters[name] += n
	p.mu.Unlock()
}

func (p *Publisher) ObserveLatency(ms int64) {
	b := LatencyBuckets[len(LatencyBuckets)-1] + 1 // overflow bucket
	for _, ub := range LatencyBuckets {
		if ms <= ub {
			b = ub
			break
		}
	}
	p.mu.Lock()
	p.latency[b]++
	p.mu.Unlock()
}

// Run flushes until ctx is cancelled, then flushes once more.
func (p *Publisher) Run(ctx context.Context) {
	t := time.NewTicker(flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			p.flush(fctx)
			cancel()
			return
		case <-t.C:
			p.flush(ctx)
		}
	}
}

func (p *Publisher) flush(ctx context.Context) {
	p.mu.Lock()
	buf, counters, latency, dropped := p.buf, p.counters, p.latency, p.dropped
	p.buf, p.counters, p.latency, p.dropped = nil, map[string]int64{}, map[int64]int64{}, 0
	p.mu.Unlock()
	if dropped > 0 {
		counters["events_dropped"] += dropped
	}
	if len(buf) == 0 && len(counters) == 0 && len(latency) == 0 {
		return
	}
	pipe := p.rdb.Pipeline()
	if len(buf) > 0 {
		b, _ := json.Marshal(buf)
		pipe.Publish(ctx, Channel, b)
	}
	for k, v := range counters {
		pipe.HIncrBy(ctx, StatsKey, k, v)
	}
	for k, v := range latency {
		pipe.HIncrBy(ctx, LatencyKey, itoa(k), v)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("events flush failed", "err", err)
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// Snapshot is the aggregate state shown on the dashboard.
type Snapshot struct {
	Stats    map[string]int64  `json:"stats"`
	Latency  map[string]int64  `json:"latency"`
	Breakers map[string]string `json:"breakers"`
	At       int64             `json:"at"`
}

func ReadSnapshot(ctx context.Context, rdb *redis.Client) (Snapshot, error) {
	pipe := rdb.Pipeline()
	s := pipe.HGetAll(ctx, StatsKey)
	l := pipe.HGetAll(ctx, LatencyKey)
	b := pipe.HGetAll(ctx, BreakersKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return Snapshot{}, err
	}
	snap := Snapshot{Stats: map[string]int64{}, Latency: map[string]int64{}, Breakers: b.Val(), At: time.Now().UnixMilli()}
	for k, v := range s.Val() {
		var n int64
		_ = json.Unmarshal([]byte(v), &n)
		snap.Stats[k] = n
	}
	for k, v := range l.Val() {
		var n int64
		_ = json.Unmarshal([]byte(v), &n)
		snap.Latency[k] = n
	}
	return snap, nil
}

// Reset clears counters (used by `herald reset` before a benchmark or demo).
func Reset(ctx context.Context, rdb *redis.Client) error {
	return rdb.Del(ctx, StatsKey, LatencyKey, BreakersKey).Err()
}
