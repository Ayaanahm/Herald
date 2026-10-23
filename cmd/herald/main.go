// Command herald runs the Herald notification platform.
//
//	herald migrate    create tables and Kafka topics
//	herald api        HTTP + WebSocket API (run several behind a load balancer)
//	herald worker     Kafka consumer that delivers notifications (run several)
//	herald provider   mock push/email/SMS provider with fault injection
//	herald all        everything in one process, for local development
//	herald report     delivery statistics from PostgreSQL, as JSON
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ayaanahm/Herald/internal/api"
	"github.com/Ayaanahm/Herald/internal/coalesce"
	"github.com/Ayaanahm/Herald/internal/config"
	"github.com/Ayaanahm/Herald/internal/events"
	"github.com/Ayaanahm/Herald/internal/lock"
	"github.com/Ayaanahm/Herald/internal/provider"
	"github.com/Ayaanahm/Herald/internal/queue"
	"github.com/Ayaanahm/Herald/internal/ratelimit"
	"github.com/Ayaanahm/Herald/internal/store"
	"github.com/Ayaanahm/Herald/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: herald migrate|api|worker|provider|all|report")
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err != nil {
		fatal(err)
	}
	queue.SetPrefix(os.Getenv("HERALD_TOPIC_PREFIX"))
	if v, err := strconv.Atoi(os.Getenv("HERALD_COALESCE_MAX")); err == nil {
		coalesce.MaxOverride = v
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "migrate":
		err = migrate(ctx, cfg)
	case "api":
		err = runAPI(ctx, cfg)
	case "worker":
		err = runWorker(ctx, cfg)
	case "provider":
		err = runProvider(ctx, cfg)
	case "all":
		err = runAll(ctx, cfg)
	case "report":
		err = report(ctx, cfg)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fatal(err)
	}
}

func fatal(err error) {
	slog.Error("fatal", "err", err)
	os.Exit(1)
}

func nodeName(role string) string {
	if n := os.Getenv("HERALD_NODE"); n != "" {
		return n
	}
	h, _ := os.Hostname()
	return fmt.Sprintf("%s-%s-%d", role, h, os.Getpid())
}

func migrate(ctx context.Context, cfg config.Config) error {
	st, err := store.Open(ctx, cfg.PostgresURL, 2)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	if err := queue.EnsureTopics(ctx, cfg.KafkaBrokers, cfg.Partitions, cfg.RetryDelays); err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	slog.Info("migrated", "partitions", cfg.Partitions, "retry_delays", cfg.RetryDelays)
	return nil
}

func newRedis(cfg config.Config, poolSize int) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: poolSize, MinIdleConns: 8})
}

func serve(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	}
}

func runAPI(ctx context.Context, cfg config.Config) error {
	node := nodeName("api")
	st, err := store.Open(ctx, cfg.PostgresURL, 32)
	if err != nil {
		return err
	}
	defer st.Close()
	rdb := newRedis(cfg, 256)
	defer rdb.Close()
	prod, err := queue.NewProducer(cfg.KafkaBrokers,
		kgo.MaxBufferedRecords(200_000),
		kgo.RecordDeliveryTimeout(30*time.Second))
	if err != nil {
		return err
	}
	defer prod.Close()

	ev := events.NewPublisher(rdb)
	var lim ratelimit.Limiter
	if cfg.RateLimitAlgo == "counter" {
		lim = ratelimit.NewSlidingCounter(rdb)
	} else {
		lim = ratelimit.NewSlidingLog(rdb, node)
	}
	ob := api.NewOutbox(st, prod, lock.New(rdb), ev, node, cfg.SweepAfter)
	b := api.NewBatcher(st, cfg.BatchMax, cfg.BatchDelay, 6, ob.AfterCommit)
	hub := api.NewHub(rdb, api.ProviderStats(&http.Client{}, cfg.ProviderURL))
	adminKey := os.Getenv("HERALD_ADMIN_KEY")
	if adminKey == "" {
		adminKey = "admin-key"
	}
	srv := api.NewServer(cfg, st, rdb, lim, b, ob, hub, ev, adminKey)

	var wg sync.WaitGroup
	bg, cancel := context.WithCancel(context.Background())
	for _, f := range []func(context.Context){ev.Run, ob.Run, hub.Run} {
		wg.Add(1)
		go func() { defer wg.Done(); f(bg) }()
	}
	slog.Info("api listening", "addr", cfg.HTTPAddr, "node", node, "batch_max", cfg.BatchMax, "ratelimit", cfg.RateLimitAlgo)
	err = serve(ctx, cfg.HTTPAddr, withPprof(srv.Routes()))
	cancel() // stop background loops only after in-flight requests have finished
	wg.Wait()
	return err
}

func runWorker(ctx context.Context, cfg config.Config) error {
	node := nodeName("worker")
	st, err := store.Open(ctx, cfg.PostgresURL, 48)
	if err != nil {
		return err
	}
	defer st.Close()
	rdb := newRedis(cfg, 512)
	defer rdb.Close()
	prod, err := queue.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer prod.Close()
	ev := events.NewPublisher(rdb)
	evCtx, evCancel := context.WithCancel(context.Background())
	evDone := make(chan struct{})
	go func() { ev.Run(evCtx); close(evDone) }()

	w := worker.New(cfg, node, st, rdb, prod, ev)
	metricsAddr := os.Getenv("HERALD_METRICS_ADDR")
	if metricsAddr != "" {
		go serve(ctx, metricsAddr, withPprof(metricsMux()))
	}
	slog.Info("worker started", "node", node, "concurrency", cfg.WorkerConcurrency)
	err = w.Run(ctx)
	evCancel()
	<-evDone
	return err
}

func runProvider(ctx context.Context, cfg config.Config) error {
	p := provider.NewServer(provider.DefaultConfig())
	slog.Info("mock provider listening", "addr", cfg.ProviderAddr)
	return serve(ctx, cfg.ProviderAddr, p.Handler())
}

func runAll(ctx context.Context, cfg config.Config) error {
	if err := migrate(ctx, cfg); err != nil {
		return err
	}
	var wg sync.WaitGroup
	errc := make(chan error, 3)
	for _, f := range []func(context.Context, config.Config) error{runProvider, runAPI, runWorker} {
		wg.Add(1)
		go func() { defer wg.Done(); errc <- f(ctx, cfg) }()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}

func metricsMux() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promHandler())
	return mux
}

// report prints delivery statistics computed in PostgreSQL. Used by the benchmarks.
func report(ctx context.Context, cfg config.Config) error {
	st, err := store.Open(ctx, cfg.PostgresURL, 2)
	if err != nil {
		return err
	}
	defer st.Close()
	counts, err := st.StatusCounts(ctx)
	if err != nil {
		return err
	}
	var p50, p95, p99, max *float64
	var n int64
	err = st.Pool.QueryRow(ctx, `
		SELECT count(*),
		       percentile_cont(0.50) WITHIN GROUP (ORDER BY ms),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY ms),
		       percentile_cont(0.99) WITHIN GROUP (ORDER BY ms),
		       max(ms)
		FROM (SELECT extract(epoch FROM delivered_at - created_at) * 1000 AS ms
		      FROM notifications WHERE status = 'delivered') d`).Scan(&n, &p50, &p95, &p99, &max)
	if err != nil {
		return err
	}
	byChannel := map[string]int64{}
	rows, err := st.Pool.Query(ctx, `SELECT channel, count(*) FROM notifications WHERE status = 'delivered' GROUP BY channel`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var ch string
		var c int64
		if err := rows.Scan(&ch, &c); err != nil {
			return err
		}
		byChannel[ch] = c
	}
	rows.Close()
	var unpublished int64
	_ = st.Pool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE published_at IS NULL`).Scan(&unpublished)
	out := map[string]any{
		"status_counts":        counts,
		"delivered":            n,
		"delivered_by_channel": byChannel,
		"unpublished":          unpublished,
		"e2e_latency_ms":       map[string]any{"p50": p50, "p95": p95, "p99": p99, "max": max},
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
