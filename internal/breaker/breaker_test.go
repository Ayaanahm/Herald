package breaker

import (
	"testing"
	"time"
)

func TestBreakerOpensProbesAndCloses(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(3, 5*time.Second, nil)
	b.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		b.Failure()
	}
	if !b.Allow() || b.State() != Closed {
		t.Fatal("opened before threshold")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("not open after 3 consecutive failures")
	}

	now = now.Add(5 * time.Second)
	if !b.Allow() {
		t.Fatal("no probe after cooldown")
	}
	if b.State() != HalfOpen || b.Allow() {
		t.Fatal("half-open must allow exactly one probe")
	}
	b.Failure()
	if b.State() != Open {
		t.Fatal("failed probe must re-open")
	}

	now = now.Add(5 * time.Second)
	b.Allow()
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatal("successful probe must close")
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	b := New(3, time.Second, nil)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()
	if b.State() != Closed {
		t.Fatal("failures were not consecutive; breaker should stay closed")
	}
}
