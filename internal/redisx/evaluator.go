// Package redisx runs Lua scripts for many concurrent callers through one
// Redis pipeline, so N concurrent script calls cost one network round trip
// instead of N. Each script still runs atomically on the Redis server.
package redisx

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/Ayaanahm/Herald/internal/coalesce"
)

var mPipeline = promauto.NewHistogram(prometheus.HistogramOpts{
	Name: "herald_redis_pipeline_size", Help: "Lua script calls per Redis pipeline.",
	Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
})

type evalReq struct {
	script *redis.Script
	keys   []string
	args   []any
}

type evalRes struct {
	val any
	err error
}

// Evaluator is safe for concurrent use.
type Evaluator struct {
	rdb *redis.Client
	c   *coalesce.Coalescer[evalReq, evalRes]
}

func NewEvaluator(rdb *redis.Client) *Evaluator {
	e := &Evaluator{rdb: rdb}
	e.c = coalesce.New(256, 4, e.flush, func(n int) { mPipeline.Observe(float64(n)) })
	return e
}

// Eval runs script and returns its reply, like (*redis.Script).Run.
func (e *Evaluator) Eval(ctx context.Context, script *redis.Script, keys []string, args ...any) *redis.Cmd {
	res, err := e.c.Do(ctx, evalReq{script: script, keys: keys, args: args})
	cmd := redis.NewCmd(ctx)
	if err == nil {
		err = res.err
	}
	if err != nil {
		cmd.SetErr(err)
	} else {
		cmd.SetVal(res.val)
	}
	return cmd
}

func (e *Evaluator) flush(ctx context.Context, reqs []evalReq) ([]evalRes, error) {
	if len(reqs) == 1 {
		v, err := reqs[0].script.Run(ctx, e.rdb, reqs[0].keys, reqs[0].args...).Result()
		return []evalRes{{v, err}}, nil
	}
	pipe := e.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(reqs))
	for i, r := range reqs {
		cmds[i] = r.script.EvalSha(ctx, pipe, r.keys, r.args...)
	}
	_, _ = pipe.Exec(ctx) // per-command errors are read below
	out := make([]evalRes, len(reqs))
	for i, cmd := range cmds {
		v, err := cmd.Result()
		if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
			// Script cache was flushed (e.g. Redis restarted): Run loads it again.
			v, err = reqs[i].script.Run(ctx, e.rdb, reqs[i].keys, reqs[i].args...).Result()
		}
		out[i] = evalRes{v, err}
	}
	return out, nil
}

// Runner runs a Lua script. Use For to get one.
type Runner func(ctx context.Context, script *redis.Script, keys []string, args ...any) *redis.Cmd

// For returns a pipelined runner for a *redis.Client, and a direct one for
// anything else (a cluster client, a transaction pipeline).
func For(rdb redis.Scripter) Runner {
	if c, ok := rdb.(*redis.Client); ok {
		return NewEvaluator(c).Eval
	}
	return func(ctx context.Context, script *redis.Script, keys []string, args ...any) *redis.Cmd {
		return script.Run(ctx, rdb, keys, args...)
	}
}
