// Package config loads Herald settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Tenant is an API client. Each tenant has its own API key and rate limit.
type Tenant struct {
	ID         string
	APIKey     string
	RateLimit  int // requests allowed per RateWindow
	RateWindow time.Duration
}

type Config struct {
	PostgresURL  string
	RedisAddr    string
	KafkaBrokers []string

	HTTPAddr     string
	ProviderAddr string // mock provider listen address
	ProviderURL  string // where workers reach the provider

	Tenants       map[string]Tenant // keyed by API key
	RateLimitAlgo string            // "log" (exact sliding log) or "counter" (sliding window counter)

	BatchMax   int           // max notifications per INSERT
	BatchDelay time.Duration // max wait to fill a batch

	Partitions        int
	RetryDelays       []time.Duration // one Kafka retry topic per delay
	WorkerConcurrency int
	LockTTL           time.Duration
	ProviderTimeout   time.Duration
	SweepAfter        time.Duration // outbox sweeper republishes rows unpublished for this long
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", key, err))
	}
	return n
}

func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", key, err))
	}
	return d
}

// ParseTenants parses "key:tenant:limit/window,..." e.g. "demo-key:demo:100/1s".
func ParseTenants(s string) (map[string]Tenant, error) {
	out := map[string]Tenant{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		f := strings.Split(part, ":")
		if len(f) != 3 {
			return nil, fmt.Errorf("tenant %q: want key:tenant:limit/window", part)
		}
		lw := strings.SplitN(f[2], "/", 2)
		if len(lw) != 2 {
			return nil, fmt.Errorf("tenant %q: want limit/window", part)
		}
		limit, err := strconv.Atoi(lw[0])
		if err != nil || limit <= 0 {
			return nil, fmt.Errorf("tenant %q: bad limit", part)
		}
		win, err := time.ParseDuration(lw[1])
		if err != nil || win <= 0 {
			return nil, fmt.Errorf("tenant %q: bad window", part)
		}
		out[f[0]] = Tenant{ID: f[1], APIKey: f[0], RateLimit: limit, RateWindow: win}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no tenants configured")
	}
	return out, nil
}

func Load() (Config, error) {
	tenants, err := ParseTenants(env("HERALD_TENANTS", "demo-key:demo:50/1s,load-key:load:100000/1s"))
	if err != nil {
		return Config{}, err
	}
	var delays []time.Duration
	for _, d := range strings.Split(env("HERALD_RETRY_DELAYS", "1s,5s,15s"), ",") {
		dd, err := time.ParseDuration(strings.TrimSpace(d))
		if err != nil {
			return Config{}, fmt.Errorf("HERALD_RETRY_DELAYS: %w", err)
		}
		delays = append(delays, dd)
	}
	c := Config{
		PostgresURL:       env("HERALD_POSTGRES_URL", "postgres://herald:herald@localhost:5432/herald?sslmode=disable"),
		RedisAddr:         env("HERALD_REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:      strings.Split(env("HERALD_KAFKA_BROKERS", "localhost:9092"), ","),
		HTTPAddr:          env("HERALD_HTTP_ADDR", ":8080"),
		ProviderAddr:      env("HERALD_PROVIDER_ADDR", ":8090"),
		ProviderURL:       env("HERALD_PROVIDER_URL", "http://localhost:8090"),
		Tenants:           tenants,
		RateLimitAlgo:     env("HERALD_RATELIMIT_ALGO", "log"),
		BatchMax:          envInt("HERALD_BATCH_MAX", 500),
		BatchDelay:        envDur("HERALD_BATCH_DELAY", 2*time.Millisecond),
		Partitions:        envInt("HERALD_PARTITIONS", 12),
		RetryDelays:       delays,
		WorkerConcurrency: envInt("HERALD_WORKER_CONCURRENCY", 256),
		LockTTL:           envDur("HERALD_LOCK_TTL", 10*time.Second),
		ProviderTimeout:   envDur("HERALD_PROVIDER_TIMEOUT", 2*time.Second),
		SweepAfter:        envDur("HERALD_SWEEP_AFTER", 5*time.Second),
	}
	if c.RateLimitAlgo != "log" && c.RateLimitAlgo != "counter" {
		return Config{}, fmt.Errorf("HERALD_RATELIMIT_ALGO must be log or counter")
	}
	if c.BatchMax < 1 {
		return Config{}, fmt.Errorf("HERALD_BATCH_MAX must be >= 1")
	}
	return c, nil
}
