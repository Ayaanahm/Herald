package coalesce

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResultsMatchInputs(t *testing.T) {
	var batches atomic.Int64
	c := New(64, 2, func(_ context.Context, in []int) ([]int, error) {
		batches.Add(1)
		time.Sleep(time.Millisecond) // let a queue build up
		out := make([]int, len(in))
		for i, v := range in {
			out[i] = v * 2
		}
		return out, nil
	}, nil)
	var wg sync.WaitGroup
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := c.Do(context.Background(), i)
			if err != nil || got != i*2 {
				t.Errorf("Do(%d) = %d, %v", i, got, err)
			}
		}(i)
	}
	wg.Wait()
	if b := batches.Load(); b >= 2000 {
		t.Fatalf("%d batches for 2000 calls: nothing was coalesced", b)
	} else {
		t.Logf("2000 calls in %d batches", b)
	}
}

func TestBatchErrorReachesEveryCaller(t *testing.T) {
	boom := errors.New("boom")
	c := New(8, 1, func(context.Context, []int) ([]int, error) { return nil, boom }, nil)
	if _, err := c.Do(context.Background(), 1); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}

func TestCancelledBeforeFlushIsSkipped(t *testing.T) {
	var seen atomic.Int64
	block := make(chan struct{})
	c := New(8, 1, func(_ context.Context, in []int) ([]int, error) {
		seen.Add(int64(len(in)))
		<-block
		return make([]int, len(in)), nil
	}, nil)
	go c.Do(context.Background(), 1) // occupies the only flusher
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { _, err := c.Do(ctx, 2); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(block)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if seen.Load() != 1 {
		t.Fatalf("cancelled call was executed")
	}
}
