// Package coalesce merges concurrent calls into batched calls.
//
// Many goroutines call Do at once; each call waits for its own result, but the
// calls are executed together, as one SQL statement or one Redis pipeline.
// Batches form naturally: a flusher takes the first waiting call, then
// everything else already queued (up to max), and runs them. Under light load
// a batch is a single call with no added delay; under heavy load batches grow,
// and the number of network round trips stops growing with the request rate.
package coalesce

import (
	"context"
	"errors"
)

type call[T, R any] struct {
	ctx  context.Context
	in   T
	out  R
	err  error
	done chan struct{}
}

// FlushFunc executes a batch. It returns one result per input, or an error
// for the whole batch. Per-item errors go in R.
type FlushFunc[T, R any] func(ctx context.Context, in []T) ([]R, error)

type Coalescer[T, R any] struct {
	queue chan *call[T, R]
	max   int
	flush FlushFunc[T, R]
	sizes func(int) // optional observer of batch sizes
}

// MaxOverride, when > 0, replaces every coalescer's batch size. Setting it
// to 1 turns coalescing off (one call per round trip), for benchmarks.
var MaxOverride int

// New starts `flushers` goroutines; each can have one batch in flight.
func New[T, R any](max, flushers int, flush FlushFunc[T, R], sizes func(int)) *Coalescer[T, R] {
	if MaxOverride > 0 {
		max = MaxOverride
	}
	if max == 1 {
		flushers = 128 // no batching: allow as many round trips in flight as before
	}
	c := &Coalescer[T, R]{queue: make(chan *call[T, R], max*flushers*4), max: max, flush: flush, sizes: sizes}
	for i := 0; i < flushers; i++ {
		go c.loop()
	}
	return c
}

func (c *Coalescer[T, R]) Do(ctx context.Context, in T) (R, error) {
	cl := &call[T, R]{ctx: ctx, in: in, done: make(chan struct{})}
	var zero R
	select {
	case c.queue <- cl:
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	// Once queued, wait for the result even if ctx ends, so a write is never
	// reported as failed when it actually committed.
	<-cl.done
	return cl.out, cl.err
}

func (c *Coalescer[T, R]) loop() {
	batch := make([]*call[T, R], 0, c.max)
	for first := range c.queue {
		batch = append(batch[:0], first)
	drain:
		for len(batch) < c.max {
			select {
			case cl := <-c.queue:
				batch = append(batch, cl)
			default:
				break drain
			}
		}
		c.run(batch)
	}
}

func (c *Coalescer[T, R]) run(batch []*call[T, R]) {
	// Drop calls whose callers gave up before the batch started.
	live := batch[:0]
	for _, cl := range batch {
		if err := cl.ctx.Err(); err != nil {
			cl.err = err
			close(cl.done)
			continue
		}
		live = append(live, cl)
	}
	if len(live) == 0 {
		return
	}
	if c.sizes != nil {
		c.sizes(len(live))
	}
	in := make([]T, len(live))
	for i, cl := range live {
		in[i] = cl.in
	}
	out, err := c.flush(context.Background(), in)
	if err == nil && len(out) != len(in) {
		err = errors.New("coalesce: flush returned wrong number of results")
	}
	for i, cl := range live {
		if err != nil {
			cl.err = err
		} else {
			cl.out = out[i]
		}
		close(cl.done)
	}
}
