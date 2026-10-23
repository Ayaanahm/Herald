package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ayaanahm/Herald/internal/events"
	"github.com/Ayaanahm/Herald/internal/lock"
	"github.com/Ayaanahm/Herald/internal/queue"
	"github.com/Ayaanahm/Herald/internal/store"
)

var mPublish = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "herald_outbox_publish_total", Help: "Kafka publishes from the outbox by path and result.",
}, []string{"path", "result"})

// Outbox moves committed notifications onto Kafka.
//
// The notifications table is the outbox: a row with published_at IS NULL has
// been accepted but is not yet on Kafka. The fast path publishes right after
// the commit. If that fails (Kafka down, process crash), the sweeper finds
// the row a few seconds later and publishes it again. The API keeps accepting
// requests while Kafka is down; the backlog drains when it comes back.
type Outbox struct {
	store    *store.Store
	producer *kgo.Client
	locker   *lock.Locker
	events   *events.Publisher
	node     string
	sweepAge time.Duration

	mu     sync.Mutex
	toMark []uuid.UUID
}

func NewOutbox(st *store.Store, producer *kgo.Client, locker *lock.Locker, ev *events.Publisher, node string, sweepAge time.Duration) *Outbox {
	return &Outbox{store: st, producer: producer, locker: locker, events: ev, node: node, sweepAge: sweepAge}
}

func messageFromInsert(it store.NewNotification, ins store.Inserted) queue.Message {
	return queue.Message{
		ID: ins.ID, TenantID: it.TenantID, RecipientID: it.RecipientID, Channels: it.Channels,
		CollapseKey: it.CollapseKey, Payload: it.Payload, Seq: ins.Seq, CreatedAt: ins.CreatedAt.UnixMilli(),
	}
}

func messageFromRow(n store.Notification) queue.Message {
	m := queue.Message{
		ID: n.ID, TenantID: n.TenantID, RecipientID: n.RecipientID, Channels: n.Channels,
		Payload: n.Payload, Seq: n.Seq, Step: n.Step, ChannelIdx: n.ChannelIdx, CreatedAt: n.CreatedAt.UnixMilli(),
	}
	if n.CollapseKey != nil {
		m.CollapseKey = *n.CollapseKey
	}
	return m
}

// AfterCommit is the fast path, called by the batcher once rows are committed.
// It never blocks the request: if Kafka's buffer is full, the sweeper takes over.
func (o *Outbox) AfterCommit(items []store.NewNotification, res []store.Inserted) {
	accepted := int64(0)
	for i, it := range items {
		if res[i].Replayed {
			continue
		}
		accepted++
		msg := messageFromInsert(it, res[i])
		rec, err := msg.Record(queue.Notifications())
		if err != nil {
			continue
		}
		id := msg.ID
		o.producer.TryProduce(context.Background(), rec, func(_ *kgo.Record, err error) {
			if err != nil {
				mPublish.WithLabelValues("fast", "error").Inc()
				return // left unpublished; the sweeper will retry
			}
			mPublish.WithLabelValues("fast", "ok").Inc()
			o.mu.Lock()
			o.toMark = append(o.toMark, id)
			o.mu.Unlock()
		})
	}
	o.events.Count("accepted", accepted)
}

// Run marks published rows in bulk and, on the elected leader, sweeps the outbox.
func (o *Outbox) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); o.markLoop(ctx) }()
	go func() { defer wg.Done(); o.sweepLoop(ctx) }()
	wg.Wait()
}

func (o *Outbox) markLoop(ctx context.Context) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			o.producer.Flush(fctx)
			o.flushMarks(fctx)
			cancel()
			return
		case <-t.C:
			o.flushMarks(ctx)
		}
	}
}

func (o *Outbox) flushMarks(ctx context.Context) {
	o.mu.Lock()
	ids := o.toMark
	o.toMark = nil
	o.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	if err := o.store.MarkPublished(ctx, ids); err != nil {
		// Harmless: the sweeper will republish them and workers drop the duplicates.
		slog.Warn("mark published failed", "n", len(ids), "err", err)
	}
}

// sweepLoop runs on exactly one API replica at a time, elected with a Redis
// lease. If the leader dies, its lease expires and another replica takes over.
func (o *Outbox) sweepLoop(ctx context.Context) {
	const ttl = 10 * time.Second
	for ctx.Err() == nil {
		lease, err := o.locker.TryAcquire(ctx, "lock:outbox-sweeper", o.node, ttl)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}
		slog.Info("outbox sweeper: became leader", "node", o.node, "fence", lease.Fence)
		o.lead(ctx, lease)
		rctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = lease.Release(rctx)
		cancel()
	}
}

func (o *Outbox) lead(ctx context.Context, lease *lock.Lease) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastExtend := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if time.Since(lastExtend) > 3*time.Second {
			ok, err := lease.Extend(ctx)
			if err != nil || !ok {
				slog.Warn("outbox sweeper: lost leadership", "node", o.node)
				return
			}
			lastExtend = time.Now()
		}
		o.sweepOnce(ctx)
	}
}

func (o *Outbox) sweepOnce(ctx context.Context) {
	for ctx.Err() == nil {
		rows, err := o.store.Unpublished(ctx, o.sweepAge, 1000)
		if err != nil || len(rows) == 0 {
			return
		}
		recs := make([]*kgo.Record, 0, len(rows))
		for _, n := range rows {
			if n.Status != store.StatusQueued {
				continue // already being worked on: only queued work needs publishing
			}
			m := messageFromRow(n)
			rec, err := m.Record(queue.Notifications())
			if err == nil {
				recs = append(recs, rec)
			}
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = o.producer.ProduceSync(pctx, recs...).FirstErr()
		cancel()
		if err != nil {
			mPublish.WithLabelValues("sweeper", "error").Inc()
			slog.Warn("outbox sweeper: kafka unavailable, will retry", "pending", len(rows), "err", err)
			return
		}
		ids := make([]uuid.UUID, len(rows))
		for i, n := range rows {
			ids[i] = n.ID
		}
		if err := o.store.MarkPublished(ctx, ids); err != nil {
			return
		}
		mPublish.WithLabelValues("sweeper", "ok").Add(float64(len(recs)))
		o.events.Count("republished", int64(len(recs)))
		slog.Info("outbox sweeper: republished", "n", len(recs))
		if len(rows) < 1000 {
			return
		}
	}
}
