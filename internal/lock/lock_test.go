package lock_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Ayaanahm/herald/internal/lock"
	"github.com/Ayaanahm/herald/internal/testenv"
)

// 40 goroutines each do 25 read-modify-write increments of a Redis value,
// with a deliberate pause between read and write. Without the lock, updates
// are lost; with it, the count is exact.
func TestLockPreventsLostUpdates(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	run := func(useLock bool) int {
		counter := fmt.Sprintf("counter-%s-%d", t.Name(), time.Now().UnixNano())
		lk := lock.New(rdb)
		var wg sync.WaitGroup
		for g := 0; g < 40; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					var lease *lock.Lease
					if useLock {
						var err error
						lease, err = lk.Acquire(ctx, counter+":lock", strconv.Itoa(g), 5*time.Second, 30*time.Second)
						if err != nil {
							t.Error(err)
							return
						}
					}
					v, _ := rdb.Get(ctx, counter).Int()
					time.Sleep(200 * time.Microsecond)
					rdb.Set(ctx, counter, v+1, time.Minute)
					if lease != nil {
						lease.Release(ctx)
					}
				}
			}(g)
		}
		wg.Wait()
		n, _ := rdb.Get(ctx, counter).Int()
		return n
	}
	if n := run(false); n >= 1000 {
		t.Fatalf("control without lock reached %d; the test can't detect races", n)
	} else {
		t.Logf("without lock: %d of 1000 increments survived", n)
	}
	if n := run(true); n != 1000 {
		t.Fatalf("with lock: %d, want 1000", n)
	}
}

func TestFencingTokensIncrease(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	lk := lock.New(rdb)
	k := "k-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var last int64
	for i := 0; i < 5; i++ {
		l, err := lk.TryAcquire(ctx, k, "me", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if l.Fence <= last {
			t.Fatalf("fence %d not greater than previous %d", l.Fence, last)
		}
		last = l.Fence
		l.Release(ctx)
	}
}

// After a lease expires and someone else takes the lock, the old holder must
// not be able to release or extend the new holder's lock.
func TestExpiredHolderCannotReleaseNewHolder(t *testing.T) {
	rdb := testenv.Redis(t)
	ctx := context.Background()
	lk := lock.New(rdb)
	k := "k-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	a, err := lk.TryAcquire(ctx, k, "a", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lk.TryAcquire(ctx, k, "b", time.Second); err != lock.ErrNotAcquired {
		t.Fatalf("second acquire while held: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	b, err := lk.TryAcquire(ctx, k, "b", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
	if b.Fence <= a.Fence {
		t.Fatalf("new fence %d <= old %d", b.Fence, a.Fence)
	}
	if ok, _ := a.Extend(ctx); ok {
		t.Fatal("expired holder extended someone else's lock")
	}
	if ok, _ := a.Release(ctx); ok {
		t.Fatal("expired holder released someone else's lock")
	}
	if ok, _ := b.Release(ctx); !ok {
		t.Fatal("current holder could not release")
	}
}
