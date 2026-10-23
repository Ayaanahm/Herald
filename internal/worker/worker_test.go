package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ayaanahm/Herald/internal/config"
	"github.com/Ayaanahm/Herald/internal/events"
	"github.com/Ayaanahm/Herald/internal/provider"
	"github.com/Ayaanahm/Herald/internal/queue"
	"github.com/Ayaanahm/Herald/internal/store"
	"github.com/Ayaanahm/Herald/internal/testenv"
	"github.com/Ayaanahm/Herald/internal/worker"
)

type harness struct {
	t     *testing.T
	cfg   config.Config
	st    *store.Store
	prov  *provider.Server
	prod  *kgo.Client
	ev    *events.Publisher
	rdbOK bool
}

func newHarness(t *testing.T, provCfg map[string]provider.ChannelConfig) *harness {
	t.Helper()
	rdb := testenv.Redis(t)
	st := testenv.Store(t)
	delays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	prod := testenv.Kafka(t, delays)
	prov := provider.NewServer(provCfg)
	srv := httptest.NewServer(prov.Handler())
	t.Cleanup(srv.Close)
	cfg := config.Config{
		KafkaBrokers: testenv.KafkaBrokers(), ProviderURL: srv.URL, RetryDelays: delays,
		WorkerConcurrency: 64, LockTTL: 2 * time.Second, ProviderTimeout: time.Second,
	}
	return &harness{t: t, cfg: cfg, st: st, prov: prov, prod: prod, ev: events.NewPublisher(rdb), rdbOK: true}
}

func (h *harness) worker(name string) *worker.Worker {
	return worker.New(h.cfg, name, h.st, testenv.Redis(h.t), h.prod, h.ev)
}

// insert stores notifications and returns the messages the outbox would publish.
func (h *harness) insert(items ...store.NewNotification) []queue.Message {
	h.t.Helper()
	for i := range items {
		items[i].ID, _ = uuid.NewV7()
		items[i].TenantID = "t"
		items[i].Payload = json.RawMessage(`{"title":"hi"}`)
	}
	res, err := h.st.InsertBatch(context.Background(), items)
	if err != nil {
		h.t.Fatal(err)
	}
	out := make([]queue.Message, len(items))
	for i, it := range items {
		out[i] = queue.Message{ID: res[i].ID, TenantID: it.TenantID, RecipientID: it.RecipientID, Channels: it.Channels,
			CollapseKey: it.CollapseKey, Payload: it.Payload, Seq: res[i].Seq, CreatedAt: res[i].CreatedAt.UnixMilli()}
	}
	return out
}

func (h *harness) row(id uuid.UUID) store.Notification {
	h.t.Helper()
	n, err := h.st.Get(context.Background(), "t", id)
	if err != nil {
		h.t.Fatal(err)
	}
	return n
}

func reliable() map[string]provider.ChannelConfig {
	return map[string]provider.ChannelConfig{"push": {}, "email": {}, "sms": {}}
}

// A worker claims a notification, then stalls (think: a long GC pause) past its
// lock lease. A second worker takes over and delivers. When the first worker
// wakes up and tries to send, its old fencing token is rejected: one send total.
func TestPausedWorkerIsFenced(t *testing.T) {
	h := newHarness(t, reliable())
	h.cfg.LockTTL = 200 * time.Millisecond
	msg := h.insert(store.NewNotification{RecipientID: "u1", Channels: []string{"push"}})[0]

	claimed := make(chan struct{})
	a := h.worker("a")
	a.Hooks.AfterClaim = func(queue.Message, int64) {
		close(claimed)
		time.Sleep(700 * time.Millisecond) // lease is 200 ms
	}
	b := h.worker("b")

	var wg sync.WaitGroup
	var errA error
	wg.Add(1)
	go func() { defer wg.Done(); errA = a.Handle(context.Background(), msg) }()
	<-claimed
	time.Sleep(250 * time.Millisecond) // a's lease has now expired
	if err := b.Handle(context.Background(), msg); err != nil {
		t.Fatalf("worker b: %v", err)
	}
	wg.Wait()
	if errA != nil {
		t.Fatalf("worker a: %v", errA)
	}

	s := h.prov.Stats()["push"]
	if s.Sent != 1 || s.StaleFenced != 1 || s.Duplicates != 0 {
		t.Fatalf("provider saw sent=%d stale_fenced=%d duplicates=%d, want 1/1/0", s.Sent, s.StaleFenced, s.Duplicates)
	}
	if n := h.row(msg.ID); n.Status != store.StatusDelivered {
		t.Fatalf("status %s, want delivered", n.Status)
	}
}

// The outbox and Kafka are at-least-once, so the same message can arrive twice.
func TestDuplicateMessageIsDropped(t *testing.T) {
	h := newHarness(t, reliable())
	msg := h.insert(store.NewNotification{RecipientID: "u1", Channels: []string{"email"}})[0]
	w := h.worker("w")
	for i := 0; i < 3; i++ {
		if err := w.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	s := h.prov.Stats()["email"]
	if s.Sent != 1 || s.Duplicates != 0 {
		t.Fatalf("sent=%d duplicates=%d, want 1 and 0 (copies must be dropped before the provider)", s.Sent, s.Duplicates)
	}
}

// A message from before a retry was scheduled is stale and must not resend.
func TestStaleStepIsDropped(t *testing.T) {
	h := newHarness(t, map[string]provider.ChannelConfig{"push": {Down: true}, "email": {}, "sms": {}})
	msg := h.insert(store.NewNotification{RecipientID: "u1", Channels: []string{"push"}})[0]
	w := h.worker("w")
	if err := w.Handle(context.Background(), msg); err != nil { // fails, schedules retry at step 1
		t.Fatal(err)
	}
	if n := h.row(msg.ID); n.Status != store.StatusRetrying || n.Step != 1 {
		t.Fatalf("status=%s step=%d, want retrying/1", n.Status, n.Step)
	}
	before := h.prov.Stats()["push"].Failed
	if err := w.Handle(context.Background(), msg); err != nil { // the step-0 copy again
		t.Fatal(err)
	}
	if after := h.prov.Stats()["push"].Failed; after != before {
		t.Fatalf("stale step-0 message reached the provider (%d -> %d calls)", before, after)
	}
}

// Two updates share a collapse key ("order 42 status"). If the newer one is
// delivered first, the older one must be superseded, never shown after it.
func TestOlderUpdateIsSupersededWhenItArrivesLate(t *testing.T) {
	h := newHarness(t, reliable())
	msgs := h.insert(
		store.NewNotification{RecipientID: "u1", Channels: []string{"push"}, CollapseKey: "order-42"}, // "shipped"
		store.NewNotification{RecipientID: "u1", Channels: []string{"push"}, CollapseKey: "order-42"}, // "out for delivery"
	)
	older, newer := msgs[0], msgs[1]
	w := h.worker("w")
	if err := w.Handle(context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	if s := h.row(newer.ID).Status; s != store.StatusDelivered {
		t.Fatalf("newer: %s", s)
	}
	if s := h.row(older.ID).Status; s != store.StatusSuperseded {
		t.Fatalf("older: %s, want superseded", s)
	}
	if sent := h.prov.Stats()["push"].Sent; sent != 1 {
		t.Fatalf("sent %d, want 1", sent)
	}
}

func TestInOrderUpdatesAreBothDelivered(t *testing.T) {
	h := newHarness(t, reliable())
	msgs := h.insert(
		store.NewNotification{RecipientID: "u1", Channels: []string{"push"}, CollapseKey: "order-42"},
		store.NewNotification{RecipientID: "u1", Channels: []string{"push"}, CollapseKey: "order-42"},
	)
	w := h.worker("w")
	for _, m := range msgs {
		if err := w.Handle(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range msgs {
		if s := h.row(m.ID).Status; s != store.StatusDelivered {
			t.Fatalf("%s: %s", m.ID, s)
		}
	}
}

// Full pipeline through Kafka with two competing workers, flaky providers, a
// dead SMS provider, collapse keys, and 10% of messages published twice.
// Every notification must end in exactly one terminal state, every failure
// must be in the DLQ, and no duplicate copy may reach a provider.
func TestEndToEndUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	h := newHarness(t, map[string]provider.ChannelConfig{
		"push":  {LatencyMs: 2, FailRate: 0.3},
		"email": {LatencyMs: 4, FailRate: 0.1},
		"sms":   {Down: true},
	})
	const n = 600
	var items []store.NewNotification
	rnd := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < n; i++ {
		it := store.NewNotification{RecipientID: fmt.Sprintf("user-%d", rnd.IntN(150))}
		switch r := rnd.IntN(10); {
		case r < 6:
			it.Channels = []string{"push", "email", "sms"}
		case r < 8:
			it.Channels = []string{"push"}
		default:
			it.Channels = []string{"sms"} // provider is down: must end in the DLQ
		}
		if rnd.IntN(5) == 0 {
			it.CollapseKey = fmt.Sprintf("order-%d", rnd.IntN(10))
		}
		items = append(items, it)
	}
	msgs := h.insert(items...)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, name := range []string{"w1", "w2"} {
		w := h.worker(name)
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(ctx) }()
	}
	dupes := 0
	for i, m := range msgs {
		rec, _ := m.Record(queue.Notifications())
		recs := []*kgo.Record{rec}
		if i%10 == 0 {
			dup, _ := m.Record(queue.Notifications())
			recs = append(recs, dup)
			dupes++
		}
		if err := h.prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(90 * time.Second)
	var counts map[string]int64
	for {
		var err error
		counts, err = h.st.StatusCounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts[store.StatusDelivered]+counts[store.StatusFailed]+counts[store.StatusSuperseded] == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all notifications finished: %v", counts)
		}
		time.Sleep(200 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	t.Logf("statuses: %v, duplicated messages: %d", counts, dupes)

	// Every failure is in the DLQ.
	adm := kadm.NewClient(h.prod)
	ends, err := adm.ListEndOffsets(context.Background(), queue.DLQ())
	if err != nil {
		t.Fatal(err)
	}
	var dlq int64
	ends.Each(func(o kadm.ListedOffset) { dlq += o.Offset })
	if dlq != counts[store.StatusFailed] {
		t.Fatalf("DLQ has %d messages, %d notifications failed", dlq, counts[store.StatusFailed])
	}

	// One provider send per delivered notification, and no duplicates got through.
	var sent, dup int64
	for _, s := range h.prov.Stats() {
		sent += s.Sent
		dup += s.Duplicates
	}
	if sent != counts[store.StatusDelivered] {
		t.Fatalf("provider sent %d, delivered %d", sent, counts[store.StatusDelivered])
	}
	if dup != 0 {
		t.Fatalf("%d duplicate sends reached the provider", dup)
	}
	if h.prov.Stats()["sms"].Sent != 0 {
		t.Fatal("sms is down but something was sent")
	}

	// Within each collapse key, delivery order matches creation order.
	rows, err := h.st.Pool.Query(context.Background(), `
		SELECT count(*) FROM (
		  SELECT seq, lag(seq) OVER (PARTITION BY recipient_id, collapse_key ORDER BY delivered_at) AS prev
		  FROM notifications WHERE collapse_key IS NOT NULL AND status = 'delivered') x
		WHERE prev > seq`)
	if err != nil {
		t.Fatal(err)
	}
	var inversions int
	for rows.Next() {
		rows.Scan(&inversions)
	}
	rows.Close()
	if inversions != 0 {
		t.Fatalf("%d collapse-key updates delivered out of order", inversions)
	}
}
