// Package breaker is a per-channel circuit breaker.
//
// When a provider is down, retrying every notification against it wastes the
// whole retry budget (and minutes of latency) before falling back. After
// `threshold` consecutive failures the breaker opens and workers skip straight
// to the next channel. After `cooldown` one probe request is let through
// (half-open); success closes the breaker, failure re-opens it.
package breaker

import (
	"sync"
	"time"
)

type State string

const (
	Closed   State = "closed"
	Open     State = "open"
	HalfOpen State = "half-open"
)

type Breaker struct {
	mu        sync.Mutex
	state     State
	failures  int
	openedAt  time.Time
	probing   bool
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(State)
}

func New(threshold int, cooldown time.Duration, onChange func(State)) *Breaker {
	return &Breaker{state: Closed, threshold: threshold, cooldown: cooldown, now: time.Now, onChange: onChange}
}

func (b *Breaker) set(s State) {
	if b.state != s {
		b.state = s
		if b.onChange != nil {
			go b.onChange(s)
		}
	}
}

// Allow reports whether a request may be sent now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.set(HalfOpen)
		b.probing = true
		return true
	default: // half-open: only one probe in flight
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probing = false
	b.set(Closed)
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	b.failures++
	if b.state == HalfOpen || b.failures >= b.threshold {
		b.openedAt = b.now()
		b.set(Open)
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
