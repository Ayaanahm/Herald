package api

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Ayaanahm/herald/internal/store"
)

var (
	mBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "herald_insert_batch_size", Help: "Notifications per INSERT.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
	})
	mBatchSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "herald_insert_batch_seconds", Help: "Time to insert and commit one batch.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})
)

var ErrOverloaded = errors.New("ingest queue full")

type result struct {
	ins store.Inserted
	err error
}

type pending struct {
	item store.NewNotification
	done chan result
}

// Batcher turns many concurrent requests into a few multi-row INSERTs.
//
// Every request still waits for its own row to be durably committed before it
// gets a 202, but up to `max` requests share one statement and one WAL flush.
// Several flushers run at once so one slow commit doesn't stall intake.
type Batcher struct {
	in      chan *pending
	store   *store.Store
	max     int
	delay   time.Duration
	onWrite func(items []store.NewNotification, res []store.Inserted)
}

func NewBatcher(st *store.Store, max int, delay time.Duration, flushers int,
	onWrite func([]store.NewNotification, []store.Inserted)) *Batcher {
	b := &Batcher{in: make(chan *pending, max*flushers*4), store: st, max: max, delay: delay, onWrite: onWrite}
	if max == 1 {
		return b // unbatched baseline: each request inserts on its own (see Submit)
	}
	for i := 0; i < flushers; i++ {
		go b.loop()
	}
	return b
}

// Submit blocks until the notification is committed (or ctx ends).
func (b *Batcher) Submit(ctx context.Context, item store.NewNotification) (store.Inserted, error) {
	p := &pending{item: item, done: make(chan result, 1)}
	if b.max == 1 {
		// Baseline mode for benchmarks: one INSERT and one commit per request.
		b.flush([]*pending{p})
		r := <-p.done
		return r.ins, r.err
	}
	select {
	case b.in <- p:
	case <-ctx.Done():
		return store.Inserted{}, ctx.Err()
	default:
		return store.Inserted{}, ErrOverloaded // shed load instead of queueing without bound
	}
	select {
	case r := <-p.done:
		return r.ins, r.err
	case <-ctx.Done():
		// The row may still be committed; the client can retry safely with
		// the same Idempotency-Key.
		return store.Inserted{}, ctx.Err()
	}
}

func (b *Batcher) loop() {
	batch := make([]*pending, 0, b.max)
	for first := range b.in {
		batch = append(batch[:0], first)
		timer := time.NewTimer(b.delay)
	fill:
		for len(batch) < b.max {
			select {
			case p := <-b.in:
				batch = append(batch, p)
			case <-timer.C:
				break fill
			}
		}
		timer.Stop()
		b.flush(batch)
	}
}

func (b *Batcher) flush(batch []*pending) {
	items := make([]store.NewNotification, len(batch))
	for i, p := range batch {
		items[i] = p.item
	}
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	res, err := b.store.InsertBatch(ctx, items)
	cancel()
	mBatchSeconds.Observe(time.Since(t0).Seconds())
	mBatchSize.Observe(float64(len(batch)))
	if err == nil && b.onWrite != nil {
		b.onWrite(items, res)
	}
	for i, p := range batch {
		if err != nil {
			p.done <- result{err: err}
		} else {
			p.done <- result{ins: res[i]}
		}
	}
}
