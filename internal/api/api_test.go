package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ayaanahm/herald/internal/store"
	"github.com/Ayaanahm/herald/internal/testenv"
)

func TestValidate(t *testing.T) {
	ok := json.RawMessage(`{"title":"x"}`)
	cases := []struct {
		name string
		req  createRequest
		want string
	}{
		{"valid", createRequest{RecipientID: "u", Payload: ok}, ""},
		{"no recipient", createRequest{Payload: ok}, "recipient_id"},
		{"bad channel", createRequest{RecipientID: "u", Channels: []string{"fax"}, Payload: ok}, "unknown channel"},
		{"dup channel", createRequest{RecipientID: "u", Channels: []string{"sms", "sms"}, Payload: ok}, "duplicate"},
		{"array payload", createRequest{RecipientID: "u", Payload: json.RawMessage(`[1]`)}, "JSON object"},
		{"huge payload", createRequest{RecipientID: "u", Payload: json.RawMessage(`{"x":"` + strings.Repeat("a", 17000) + `"}`)}, "16 KiB"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.req.validate()
			if c.want == "" && err != nil {
				t.Fatal(err)
			}
			if c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
				t.Fatalf("got %v, want error containing %q", err, c.want)
			}
		})
	}
	r := createRequest{RecipientID: "u", Payload: ok}
	r.validate()
	if strings.Join(r.Channels, ",") != "push,email,sms" {
		t.Fatalf("default channels: %v", r.Channels)
	}
}

// 1,000 concurrent requests through the batcher, where 200 of them are retries
// sharing 40 idempotency keys. Each key must create exactly one row, and every
// retry must get back the ID of that row.
func TestBatcherIdempotencyUnderConcurrency(t *testing.T) {
	st := testenv.Store(t)
	var mu sync.Mutex
	var batches []int
	b := NewBatcher(st, 100, 2*time.Millisecond, 4, func(items []store.NewNotification, _ []store.Inserted) {
		mu.Lock()
		batches = append(batches, len(items))
		mu.Unlock()
	})

	ids := make([]uuid.UUID, 1000)
	keys := make([]string, 1000)
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		if i < 200 {
			keys[i] = fmt.Sprintf("key-%d", i%40)
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, _ := uuid.NewV7()
			res, err := b.Submit(context.Background(), store.NewNotification{
				ID: id, TenantID: "t", IdempotencyKey: keys[i], RecipientID: "u",
				Channels: []string{"push"}, Payload: json.RawMessage(`{}`),
			})
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = res.ID
		}(i)
	}
	wg.Wait()

	var rows int
	st.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&rows)
	if rows != 800+40 {
		t.Fatalf("%d rows, want 840", rows)
	}
	byKey := map[string]uuid.UUID{}
	for i := 0; i < 200; i++ {
		if prev, ok := byKey[keys[i]]; ok && prev != ids[i] {
			t.Fatalf("key %s returned two IDs", keys[i])
		}
		byKey[keys[i]] = ids[i]
	}
	if len(batches) >= 1000 {
		t.Fatalf("%d batches for 1000 requests: batching isn't happening", len(batches))
	}
	t.Logf("1000 requests written in %d INSERT statements", len(batches))
}
