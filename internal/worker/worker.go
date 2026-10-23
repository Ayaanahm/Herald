// Package worker consumes delivery work from Kafka and sends it through the
// provider, with retries, channel fallback, a circuit breaker per channel and
// a dead-letter queue.
//
// Delivery is at-least-once end to end: offsets are committed only after a
// batch is fully handled, so a crash means the batch is redelivered. Every
// step is idempotent, so redelivery is safe:
//
//   - each message carries a step number; Claim only succeeds if the row is
//     still at that step, so duplicate and stale messages are dropped;
//   - writes are fenced with the lock's token, so a worker that paused past
//     its lease cannot overwrite the work of the worker that replaced it;
//   - the provider is called with an idempotency key and the fencing token.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ayaanahm/herald/internal/breaker"
	"github.com/Ayaanahm/herald/internal/coalesce"
	"github.com/Ayaanahm/herald/internal/config"
	"github.com/Ayaanahm/herald/internal/events"
	"github.com/Ayaanahm/herald/internal/lock"
	"github.com/Ayaanahm/herald/internal/provider"
	"github.com/Ayaanahm/herald/internal/queue"
	"github.com/Ayaanahm/herald/internal/store"
)

var (
	mOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herald_worker_outcomes_total", Help: "Message outcomes by result and channel.",
	}, []string{"outcome", "channel"})
	mE2E = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "herald_delivery_latency_seconds", Help: "Accepted-to-delivered latency.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	})
	mProvider = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herald_provider_request_seconds", Help: "Provider call latency.",
		Buckets: prometheus.ExponentialBuckets(0.002, 2, 12),
	}, []string{"channel"})
	mSQLBatch = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "herald_worker_sql_batch_size", Help: "Rows per coalesced worker statement.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
	}, []string{"statement"})
	mLockWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "herald_lock_wait_seconds", Help: "Time to acquire the delivery lock.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})
)

// Hooks let tests inject faults between steps.
type Hooks struct {
	AfterClaim func(m queue.Message, fence int64) // e.g. simulate a GC pause past the lease
}

type Worker struct {
	cfg      config.Config
	name     string
	store    *store.Store
	rdb      *redis.Client
	locker   *lock.Locker
	producer *kgo.Client
	events   *events.Publisher
	http     *http.Client
	breakers map[string]*breaker.Breaker
	Hooks    Hooks

	// Concurrent deliveries share statements: one UPDATE claims (or marks
	// delivered) every notification that is ready at that moment.
	claims    *coalesce.Coalescer[store.ClaimReq, store.ClaimRes]
	delivered *coalesce.Coalescer[store.DeliveredReq, error]
}

func New(cfg config.Config, name string, st *store.Store, rdb *redis.Client, producer *kgo.Client, ev *events.Publisher) *Worker {
	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        2048,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
	}
	w := &Worker{
		cfg: cfg, name: name, store: st, rdb: rdb, locker: lock.New(rdb), producer: producer, events: ev,
		http:     &http.Client{Transport: tr, Timeout: cfg.ProviderTimeout},
		breakers: map[string]*breaker.Breaker{},
	}
	for _, ch := range []string{"push", "email", "sms"} {
		w.breakers[ch] = breaker.New(5, 5*time.Second, nil)
	}
	w.claims = coalesce.New(256, 4, st.ClaimMany, func(n int) { mSQLBatch.WithLabelValues("claim").Observe(float64(n)) })
	w.delivered = coalesce.New(256, 4, st.MarkDeliveredMany, func(n int) { mSQLBatch.WithLabelValues("delivered").Observe(float64(n)) })
	return w
}

// Run consumes the main topic and every retry topic until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(w.cfg.RetryDelays)+1)
	start := func(topic string, delayed bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.consume(ctx, topic, delayed); err != nil {
				errs <- fmt.Errorf("%s: %w", topic, err)
			}
		}()
	}
	start(queue.Notifications(), false)
	for _, d := range w.cfg.RetryDelays {
		start(queue.RetryTopic(d), true)
	}
	wg.Add(1)
	go func() { defer wg.Done(); w.syncBreakers(ctx) }()
	wg.Wait()
	close(errs)
	return <-errs
}

func (w *Worker) consume(ctx context.Context, topic string, delayed bool) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(w.cfg.KafkaBrokers...),
		kgo.ConsumerGroup("herald-workers."+topic),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = cl.CommitMarkedOffsets(cctx)
		cancel()
		// With BlockRebalanceOnPoll, leaving the group waits for permission to
		// rebalance; without this, shutdown hangs forever.
		cl.AllowRebalance()
		cl.Close()
	}()

	for {
		fetches := cl.PollRecords(ctx, 2000)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Warn("fetch error", "topic", t, "partition", p, "err", err)
		})
		recs := fetches.Records()
		if len(recs) > 0 && w.processBatch(ctx, recs, delayed) {
			cl.MarkCommitRecords(recs...)
		}
		cl.AllowRebalance()
	}
}

// processBatch handles records with the same key in order and different keys
// concurrently. It returns false if it was interrupted; the batch is then not
// committed and will be redelivered, which is safe because handling is idempotent.
func (w *Worker) processBatch(ctx context.Context, recs []*kgo.Record, delayed bool) bool {
	groups := map[string][]*kgo.Record{}
	var order []string
	for _, r := range recs {
		k := string(r.Key)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	sem := make(chan struct{}, w.cfg.WorkerConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	complete := true
	for _, k := range order {
		group := groups[k]
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			for _, r := range group {
				if !w.processRecord(ctx, r, delayed) {
					mu.Lock()
					complete = false
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return complete && ctx.Err() == nil
}

// processRecord returns false only if ctx was cancelled before the record was handled.
func (w *Worker) processRecord(ctx context.Context, r *kgo.Record, delayed bool) bool {
	msg, err := queue.Decode(r.Value)
	if err != nil {
		w.poison(ctx, r, err)
		return true
	}
	if delayed && msg.NotBefore > 0 {
		if wait := time.Until(time.UnixMilli(msg.NotBefore)); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return false
			}
		}
	}
	// Infrastructure errors (Postgres, Redis or Kafka unavailable) are retried
	// in place with backoff: the message is never dropped or dead-lettered for them.
	backoff := 100 * time.Millisecond
	for {
		err := w.Handle(ctx, msg)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		slog.Warn("handle failed, retrying", "id", msg.ID, "err", err, "backoff", backoff)
		mOutcomes.WithLabelValues("infra_error", "").Inc()
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return false
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (w *Worker) poison(ctx context.Context, r *kgo.Record, cause error) {
	slog.Error("poison message sent to DLQ", "topic", r.Topic, "offset", r.Offset, "err", cause)
	rec := &kgo.Record{Topic: queue.DLQ(), Key: r.Key, Value: r.Value, Headers: []kgo.RecordHeader{
		{Key: "error", Value: []byte("decode: " + cause.Error())},
		{Key: "source", Value: []byte(fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset))},
	}}
	for ctx.Err() == nil {
		if err := w.producer.ProduceSync(ctx, rec).FirstErr(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	w.events.Count("poison", 1)
	mOutcomes.WithLabelValues("poison", "").Inc()
}

func lockKey(m queue.Message) string {
	if m.CollapseKey != "" {
		// Serialize every notification that shares a collapse key, so an older
		// one can never be delivered after a newer one.
		return "lock:ck:" + m.TenantID + ":" + m.RecipientID + ":" + m.CollapseKey
	}
	return "lock:n:" + m.ID.String()
}

var errLockBusy = errors.New("delivery lock busy")

// Handle processes one message. A nil error means the message is fully handled
// (delivered, retry scheduled, dead-lettered, or dropped as stale).
func (w *Worker) Handle(ctx context.Context, m queue.Message) error {
	t0 := time.Now()
	lease, err := w.locker.Acquire(ctx, lockKey(m), w.name+"/"+uuid.NewString(), w.cfg.LockTTL, w.cfg.LockTTL)
	mLockWait.Observe(time.Since(t0).Seconds())
	if errors.Is(err, lock.ErrNotAcquired) {
		return errLockBusy
	}
	if err != nil {
		return err
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = lease.Release(rctx)
		cancel()
	}()

	claim, err := w.claims.Do(ctx, store.ClaimReq{ID: m.ID, Step: m.Step, Fence: lease.Fence})
	if err != nil {
		return err
	}
	seq := claim.Seq
	if !claim.OK {
		// Already delivered, cancelled, moved to a later step, or claimed by a
		// newer lock holder: this copy of the message is stale.
		w.events.Count("stale_dropped", 1)
		mOutcomes.WithLabelValues("stale_dropped", "").Inc()
		return nil
	}
	if w.Hooks.AfterClaim != nil {
		w.Hooks.AfterClaim(m, lease.Fence)
	}

	if m.CollapseKey != "" {
		last, err := w.store.LastDeliveredSeq(ctx, m.TenantID, m.RecipientID, m.CollapseKey)
		if err != nil {
			return err
		}
		if last > seq {
			// A newer notification with this collapse key already reached the
			// user; delivering this one now would show stale information.
			if err := w.store.MarkTerminal(ctx, m.ID, lease.Fence, store.StatusSuperseded, ""); err != nil {
				return w.fencedOK(err)
			}
			w.emit(m, store.StatusSuperseded, "", "")
			w.events.Count("superseded", 1)
			mOutcomes.WithLabelValues("superseded", "").Inc()
			return nil
		}
	}

	for idx := m.ChannelIdx; idx < len(m.Channels); idx++ {
		ch := m.Channels[idx]
		attempt := 0
		if idx == m.ChannelIdx {
			attempt = m.Attempt
		}
		br := w.breakers[ch]
		if br == nil {
			m.Errors = append(m.Errors, ch+": unknown channel")
			continue
		}
		if !br.Allow() {
			m.Errors = append(m.Errors, ch+": circuit open")
			w.events.Count("breaker_skip", 1)
			mOutcomes.WithLabelValues("breaker_skip", ch).Inc()
			continue
		}
		res, sendErr := w.send(ctx, ch, m, lease.Fence)
		switch res {
		case sendOK:
			br.Success()
			if err := w.markDelivered(ctx, m, lease.Fence, ch, seq); err != nil {
				return w.fencedOK(err)
			}
			lat := time.Since(time.UnixMilli(m.CreatedAt))
			mE2E.Observe(lat.Seconds())
			w.events.ObserveLatency(lat.Milliseconds())
			w.events.Count("delivered", 1)
			w.events.Count("delivered:"+ch, 1)
			if idx > 0 {
				w.events.Count("delivered_fallback", 1)
			}
			mOutcomes.WithLabelValues("delivered", ch).Inc()
			w.emitLatency(m, ch, attempt, lat.Milliseconds())
			return nil
		case sendStale:
			// The provider has seen a newer fencing token for this message: we
			// lost the lock while paused, and a newer holder owns the delivery.
			w.events.Count("fenced", 1)
			mOutcomes.WithLabelValues("fenced", ch).Inc()
			return nil
		case sendPermanent:
			br.Success() // the provider is healthy; this recipient just can't use this channel
			m.Errors = append(m.Errors, ch+": "+sendErr)
			mOutcomes.WithLabelValues("rejected", ch).Inc()
			continue
		case sendRetryable:
			br.Failure()
			m.Errors = append(m.Errors, fmt.Sprintf("%s attempt %d: %s", ch, attempt+1, sendErr))
			mOutcomes.WithLabelValues("failed_attempt", ch).Inc()
			if attempt < len(w.cfg.RetryDelays) {
				return w.scheduleRetry(ctx, m, lease.Fence, idx, attempt, sendErr)
			}
			// Retries on this channel are used up: fall back to the next one.
		}
	}
	return w.deadLetter(ctx, m, lease.Fence)
}

func (w *Worker) markDelivered(ctx context.Context, m queue.Message, fence int64, ch string, seq int64) error {
	if m.CollapseKey != "" {
		// Must advance collapse_state in the same transaction.
		return w.store.MarkDelivered(ctx, m.ID, fence, ch, m.TenantID, m.RecipientID, m.CollapseKey, seq)
	}
	rowErr, err := w.delivered.Do(ctx, store.DeliveredReq{ID: m.ID, Fence: fence, Channel: ch})
	if err != nil {
		return err
	}
	return rowErr
}

// fencedOK turns a fencing rejection into a handled outcome: a newer lock
// holder owns this notification, so there is nothing left for us to do.
func (w *Worker) fencedOK(err error) error {
	if errors.Is(err, store.ErrFenced) {
		w.events.Count("fenced", 1)
		mOutcomes.WithLabelValues("fenced", "").Inc()
		return nil
	}
	return err
}

func (w *Worker) scheduleRetry(ctx context.Context, m queue.Message, fence int64, idx, attempt int, cause string) error {
	delay := w.cfg.RetryDelays[attempt]
	next := m
	next.Step = m.Step + 1
	next.ChannelIdx = idx
	next.Attempt = attempt + 1
	next.NotBefore = time.Now().Add(delay).UnixMilli()
	rec, err := next.Record(queue.RetryTopic(delay))
	if err != nil {
		return err
	}
	// Produce first, then record the new step. If we crash in between, the old
	// message is redelivered and simply schedules the retry again; the extra
	// copy is dropped as stale. The opposite order could lose the notification.
	if err := w.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return err
	}
	if err := w.store.MarkRetrying(ctx, m.ID, fence, next.Step, idx, cause); err != nil {
		return w.fencedOK(err)
	}
	w.emit(m, store.StatusRetrying, m.Channels[idx], cause)
	w.events.Count("retried", 1)
	mOutcomes.WithLabelValues("retry_scheduled", m.Channels[idx]).Inc()
	return nil
}

func (w *Worker) deadLetter(ctx context.Context, m queue.Message, fence int64) error {
	rec, err := m.Record(queue.DLQ())
	if err != nil {
		return err
	}
	if err := w.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return err
	}
	lastErr := "all channels failed"
	if len(m.Errors) > 0 {
		lastErr = m.Errors[len(m.Errors)-1]
	}
	if err := w.store.MarkTerminal(ctx, m.ID, fence, store.StatusFailed, lastErr); err != nil {
		return w.fencedOK(err)
	}
	w.emit(m, store.StatusFailed, "", lastErr)
	w.events.Count("dlq", 1)
	mOutcomes.WithLabelValues("dead_lettered", "").Inc()
	return nil
}

type sendResult int

const (
	sendOK sendResult = iota
	sendRetryable
	sendPermanent
	sendStale
)

func (w *Worker) send(ctx context.Context, ch string, m queue.Message, fence int64) (sendResult, string) {
	body, _ := json.Marshal(provider.SendRequest{
		IdempotencyKey: m.ID.String() + "/" + ch, Fence: fence, Recipient: m.RecipientID, Payload: m.Payload,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.ProviderURL+"/v1/send/"+ch, bytes.NewReader(body))
	if err != nil {
		return sendRetryable, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	t0 := time.Now()
	resp, err := w.http.Do(req)
	mProvider.WithLabelValues(ch).Observe(time.Since(t0).Seconds())
	if err != nil {
		return sendRetryable, "provider unreachable or timed out"
	}
	defer resp.Body.Close()
	var pr provider.SendResponse
	_ = json.NewDecoder(resp.Body).Decode(&pr)
	switch {
	case resp.StatusCode == http.StatusOK:
		if pr.Duplicate {
			w.events.Count("provider_duplicate", 1)
		}
		return sendOK, ""
	case resp.StatusCode == http.StatusConflict:
		return sendStale, pr.Error
	case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
		return sendPermanent, fmt.Sprintf("%d %s", resp.StatusCode, pr.Error)
	default:
		return sendRetryable, fmt.Sprintf("%d %s", resp.StatusCode, pr.Error)
	}
}

func (w *Worker) emit(m queue.Message, status, ch, errMsg string) {
	w.events.Emit(events.Event{ID: m.ID.String(), Tenant: m.TenantID, Status: status, Channel: ch, Attempt: m.Attempt, Error: errMsg})
}

func (w *Worker) emitLatency(m queue.Message, ch string, attempt int, ms int64) {
	w.events.Emit(events.Event{ID: m.ID.String(), Tenant: m.TenantID, Status: store.StatusDelivered, Channel: ch, Attempt: attempt, LatencyMs: ms})
}

// syncBreakers publishes this worker's breaker states for the dashboard.
func (w *Worker) syncBreakers(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	chans := make([]string, 0, len(w.breakers))
	for ch := range w.breakers {
		chans = append(chans, ch)
	}
	sort.Strings(chans)
	for {
		select {
		case <-ctx.Done():
			dctx, cancel := context.WithTimeout(context.Background(), time.Second)
			for _, ch := range chans {
				w.rdb.HDel(dctx, events.BreakersKey, w.name+"/"+ch)
			}
			cancel()
			return
		case <-t.C:
			vals := make([]any, 0, 2*len(chans))
			for _, ch := range chans {
				vals = append(vals, w.name+"/"+ch, string(w.breakers[ch].State()))
			}
			w.rdb.HSet(ctx, events.BreakersKey, vals...)
		}
	}
}
