// Package lock is a Redis lease lock that hands out fencing tokens.
//
// A lease lock alone cannot guarantee mutual exclusion: a holder that pauses
// (GC, network partition, a slow provider call) past its TTL keeps running
// after someone else has acquired the lock. So every acquisition also returns a
// fencing token from a single, ever-increasing Redis counter, and the resource
// being protected (here, PostgreSQL rows) rejects writes that carry a token
// older than the newest one it has seen.
package lock

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Ayaanahm/herald/internal/redisx"
)

var ErrNotAcquired = errors.New("lock: not acquired")

// acquire: SET NX PX, and on success INCR the global fencing counter, atomically.
var acquire = redis.NewScript(`
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
`)

// release deletes the key only if we still own it.
var release = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// extend renews the lease only if we still own it.
var extend = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

const fenceKey = "herald:fence"

type Locker struct {
	run redisx.Runner
}

// New returns a Locker. With a *redis.Client, concurrent lock operations are
// pipelined together (see package redisx).
func New(rdb redis.Scripter) *Locker { return &Locker{run: redisx.For(rdb)} }

// Lease is a held lock.
type Lease struct {
	l     *Locker
	Key   string
	token string
	Fence int64
	TTL   time.Duration
}

// TryAcquire makes one attempt.
func (l *Locker) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (*Lease, error) {
	fence, err := l.run(ctx, acquire, []string{key, fenceKey}, owner, ttl.Milliseconds()).Int64()
	if err != nil {
		return nil, err
	}
	if fence == 0 {
		return nil, ErrNotAcquired
	}
	return &Lease{l: l, Key: key, token: owner, Fence: fence, TTL: ttl}, nil
}

// Acquire retries with jittered exponential backoff until `wait` elapses.
func (l *Locker) Acquire(ctx context.Context, key, owner string, ttl, wait time.Duration) (*Lease, error) {
	deadline := time.Now().Add(wait)
	backoff := 2 * time.Millisecond
	for {
		lease, err := l.TryAcquire(ctx, key, owner, ttl)
		if err == nil || !errors.Is(err, ErrNotAcquired) {
			return lease, err
		}
		if time.Now().Add(backoff).After(deadline) {
			return nil, ErrNotAcquired
		}
		sleep := backoff/2 + time.Duration(rand.Int64N(int64(backoff)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}

// Release frees the lock if it is still ours. Returns false if it had expired
// and possibly been taken by someone else.
func (s *Lease) Release(ctx context.Context) (bool, error) {
	n, err := s.l.run(ctx, release, []string{s.Key}, s.token).Int64()
	return n == 1, err
}

// Extend renews the lease. Returns false if we no longer hold it.
func (s *Lease) Extend(ctx context.Context) (bool, error) {
	n, err := s.l.run(ctx, extend, []string{s.Key}, s.token, s.TTL.Milliseconds()).Int64()
	return n == 1, err
}
