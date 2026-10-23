// Package testenv gives integration tests real PostgreSQL, Redis and Kafka.
//
// Tests skip (not fail) when a service is unreachable, so `go test ./...`
// works on a laptop with nothing running. CI starts all three services.
// Each test gets its own database and Kafka topic prefix.
package testenv

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ayaanahm/herald/internal/queue"
	"github.com/Ayaanahm/herald/internal/store"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func PostgresURL() string {
	return env("HERALD_TEST_POSTGRES_URL", "postgres://herald:herald@localhost:5432/herald?sslmode=disable")
}
func RedisAddr() string { return env("HERALD_TEST_REDIS_ADDR", "localhost:6379") }
func KafkaBrokers() []string {
	return strings.Split(env("HERALD_TEST_KAFKA_BROKERS", "localhost:9092"), ",")
}

func Redis(t testing.TB) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: RedisAddr(), PoolSize: 128})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s: %v", RedisAddr(), err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// Store creates a fresh, migrated database for this test and drops it afterwards.
func Store(t testing.TB) *store.Store {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, PostgresURL())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	name := fmt.Sprintf("herald_test_%d_%d", time.Now().UnixNano()%1e9, rand.IntN(1e6))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close(ctx)
		t.Fatalf("create database: %v", err)
	}
	cfg, _ := pgx.ParseConfig(PostgresURL())
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, name)
	st, err := store.Open(ctx, url, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.Close()
		admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	return st
}

// Kafka sets a unique topic prefix, creates the topics and returns a producer.
func Kafka(t testing.TB, retryDelays []time.Duration) *kgo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	probe, err := kgo.NewClient(kgo.SeedBrokers(KafkaBrokers()...))
	if err == nil {
		err = probe.Ping(ctx)
		probe.Close()
	}
	if err != nil {
		t.Skipf("kafka unavailable: %v", err)
	}
	queue.SetPrefix(fmt.Sprintf("test%d", time.Now().UnixNano()))
	if err := queue.EnsureTopics(ctx, KafkaBrokers(), 4, retryDelays); err != nil {
		t.Fatal(err)
	}
	prod, err := queue.NewProducer(KafkaBrokers())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	return prod
}
