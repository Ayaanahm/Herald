package ratelimit_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ayaanahm/herald/internal/ratelimit"
	"github.com/Ayaanahm/herald/internal/testenv"
)

func key(t testing.TB) string { return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()) }

// Concurrent requests from many "replicas" must never exceed the limit: the
// check and the record happen in one atomic script.
func TestSlidingLogExactUnderConcurrency(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	k := key(t)
	// Four limiters = four API replicas sharing one Redis.
	lims := []ratelimit.Limiter{
		ratelimit.NewSlidingLog(rdb, "a"), ratelimit.NewSlidingLog(rdb, "b"),
		ratelimit.NewSlidingLog(rdb, "c"), ratelimit.NewSlidingLog(rdb, "d"),
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 400; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := lims[i%4].Allow(ctx, k, 100, 10*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			if r.Allowed {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if got := allowed.Load(); got != 100 {
		t.Fatalf("allowed %d of 400, want exactly 100", got)
	}
}

// A fixed-window limiter lets 2x the limit through around a window boundary.
// The sliding log must not: any 1 s span contains at most `limit` requests.
func TestSlidingLogNoBoundaryBurst(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	lim := ratelimit.NewSlidingLog(rdb, "n")
	k := key(t)
	burst := func() (n int) {
		for i := 0; i < 10; i++ {
			r, err := lim.Allow(ctx, k, 10, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if r.Allowed {
				n++
			}
		}
		return n
	}
	if n := burst(); n != 10 {
		t.Fatalf("first burst: %d allowed, want 10", n)
	}
	time.Sleep(600 * time.Millisecond) // a fixed 1 s window could have rolled over here
	if n := burst(); n != 0 {
		t.Fatalf("second burst 600ms later: %d allowed, want 0", n)
	}
	time.Sleep(500 * time.Millisecond) // now the first burst has left the window
	if n := burst(); n != 10 {
		t.Fatalf("third burst after window: %d allowed, want 10", n)
	}
}

func TestRetryAfterIsAccurate(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	for name, lim := range map[string]ratelimit.Limiter{
		"log": ratelimit.NewSlidingLog(rdb, "r"), "counter": ratelimit.NewSlidingCounter(rdb),
	} {
		t.Run(name, func(t *testing.T) {
			k := key(t)
			for i := 0; i < 5; i++ {
				lim.Allow(ctx, k, 5, time.Second)
			}
			r, err := lim.Allow(ctx, k, 5, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if r.Allowed {
				t.Fatal("6th request allowed")
			}
			if r.RetryAfter <= 0 || r.RetryAfter > time.Second {
				t.Fatalf("retry after %v, want (0, 1s]", r.RetryAfter)
			}
			time.Sleep(r.RetryAfter + 20*time.Millisecond)
			r, _ = lim.Allow(ctx, k, 5, time.Second)
			if !r.Allowed {
				t.Fatal("still rejected after Retry-After elapsed")
			}
		})
	}
}

// The counter is approximate but must stay close to the limit at a steady rate.
func TestSlidingCounterSteadyRate(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	lim := ratelimit.NewSlidingCounter(rdb)
	k := key(t)
	// Offer 200/s against a 100/s limit for 3 s.
	allowed := 0
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		<-tick.C
		r, err := lim.Allow(ctx, k, 100, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if r.Allowed {
			allowed++
		}
	}
	if allowed < 270 || allowed > 330 {
		t.Fatalf("allowed %d in 3s at a 100/s limit, want 300 ±10%%", allowed)
	}
}

// Cost per decision when a tenant is at a high steady rate. The sliding log
// keeps one sorted-set entry per request in the window, the counter keeps two
// integers.
func BenchmarkLimiter(b *testing.B) {
	rdb := testenv.Redis(b)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		lim  ratelimit.Limiter
	}{{"sliding-log", ratelimit.NewSlidingLog(rdb, "bench")}, {"sliding-counter", ratelimit.NewSlidingCounter(rdb)}} {
		b.Run(tc.name, func(b *testing.B) {
			k := key(b)
			b.SetParallelism(16)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := tc.lim.Allow(ctx, k, 1_000_000, time.Second); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
