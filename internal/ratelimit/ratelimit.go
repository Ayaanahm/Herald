// Package ratelimit implements sliding-window rate limiting in Redis Lua scripts.
//
// Both algorithms run as one atomic script per request, so every API replica
// shares the same counters with no race between "check" and "record". Both read
// the clock with Redis TIME inside the script, so replicas with skewed clocks
// still agree on where the window is.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Ayaanahm/Herald/internal/redisx"
)

// Result of one Allow call.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration // when not allowed: how long until a slot frees up
}

type Limiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error)
}

// slidingLog keeps one sorted-set entry per accepted request, scored by time.
// It is exact: no window of length `window` ever contains more than `limit`
// accepted requests. Memory is O(limit) per key.
var slidingLog = redis.NewScript(`
local key    = KEYS[1]
local window = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local member = ARGV[3]
local t      = redis.call('TIME')
local now    = tonumber(t[1]) * 1000000 + tonumber(t[2])      -- microseconds

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now, member)
  redis.call('PEXPIRE', key, math.ceil(window / 1000) + 1)
  return {1, limit - count - 1, 0}
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local retry = tonumber(oldest[2]) + window - now
return {0, 0, retry}
`)

// slidingCounter keeps two fixed-window counters and weights the previous one
// by how much of it still overlaps the sliding window. O(1) memory per key, but
// approximate: it assumes requests in the previous window were evenly spread.
var slidingCounter = redis.NewScript(`
local prefix = KEYS[1]
local window = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local t      = redis.call('TIME')
local now    = tonumber(t[1]) * 1000000 + tonumber(t[2])

local cur_start = now - (now % window)
local cur_key   = prefix .. ':' .. cur_start
local prev_key  = prefix .. ':' .. (cur_start - window)
local cur  = tonumber(redis.call('GET', cur_key) or '0')
local prev = tonumber(redis.call('GET', prev_key) or '0')
local weight = (window - (now - cur_start)) / window
local estimate = prev * weight + cur

if estimate < limit then
  redis.call('INCR', cur_key)
  redis.call('PEXPIRE', cur_key, math.ceil(2 * window / 1000) + 1)
  return {1, math.floor(limit - estimate - 1), 0}
end
-- Time until the weighted estimate drops below the limit.
local retry = window - (now - cur_start)
if prev > 0 then
  local need = (estimate - limit + 1) / prev * window
  if need < retry then retry = need end
end
return {0, 0, math.ceil(retry)}
`)

type redisLimiter struct {
	run    redisx.Runner
	script *redis.Script
	prefix string
	seq    atomic.Uint64
	node   string
}

// NewSlidingLog returns the exact sliding-log limiter.
func NewSlidingLog(rdb redis.Scripter, node string) Limiter {
	return &redisLimiter{run: redisx.For(rdb), script: slidingLog, prefix: "rl:log:", node: node}
}

// NewSlidingCounter returns the approximate, constant-memory limiter.
func NewSlidingCounter(rdb redis.Scripter) Limiter {
	return &redisLimiter{run: redisx.For(rdb), script: slidingCounter, prefix: "rl:ctr:"}
}

func (l *redisLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	args := []any{window.Microseconds(), limit}
	if l.script == slidingLog {
		// Sorted-set members must be unique, or two requests in the same
		// microsecond would collapse into one entry.
		args = append(args, l.node+":"+strconv.FormatUint(l.seq.Add(1), 36))
	}
	v, err := l.run(ctx, l.script, []string{l.prefix + key}, args...).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: %w", err)
	}
	return Result{
		Allowed:    v[0] == 1,
		Limit:      limit,
		Remaining:  int(v[1]),
		RetryAfter: time.Duration(v[2]) * time.Microsecond,
	}, nil
}
