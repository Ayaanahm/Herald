// Package store is Herald's PostgreSQL layer. PostgreSQL is the source of truth:
// a notification is accepted only once its row is committed, and every state
// change made by a worker is guarded by a lock fencing token.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

const (
	StatusQueued     = "queued"
	StatusDelivering = "delivering"
	StatusRetrying   = "retrying"
	StatusDelivered  = "delivered"
	StatusFailed     = "failed"
	StatusSuperseded = "superseded"
	StatusCancelled  = "cancelled"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	Pool *pgxpool.Pool
}

func Open(ctx context.Context, url string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, schema)
	return err
}

// NewNotification is an accepted request that has not been written yet.
type NewNotification struct {
	ID             uuid.UUID
	TenantID       string
	IdempotencyKey string // empty = none
	RecipientID    string
	Channels       []string
	CollapseKey    string // empty = none
	Payload        json.RawMessage
}

// Inserted is the outcome of one NewNotification in a batch.
type Inserted struct {
	ID        uuid.UUID
	Seq       int64
	CreatedAt time.Time
	Status    string
	Replayed  bool // idempotency key already existed; ID is the original notification
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// InsertBatch writes many notifications in one statement and one commit.
// Results are returned in input order. Rows whose idempotency key already
// exists are not inserted; their result points at the existing notification.
func (s *Store) InsertBatch(ctx context.Context, items []NewNotification) ([]Inserted, error) {
	n := len(items)
	ids := make([]uuid.UUID, n)
	tenants := make([]string, n)
	idems := make([]*string, n)
	rcpts := make([]string, n)
	chans := make([]string, n)
	cks := make([]*string, n)
	payloads := make([]string, n)
	for i, it := range items {
		ids[i] = it.ID
		tenants[i] = it.TenantID
		idems[i] = nullable(it.IdempotencyKey)
		rcpts[i] = it.RecipientID
		chans[i] = strings.Join(it.Channels, ",")
		cks[i] = nullable(it.CollapseKey)
		payloads[i] = string(it.Payload)
	}

	rows, err := s.Pool.Query(ctx, `
		INSERT INTO notifications (id, tenant_id, idempotency_key, recipient_id, channels, collapse_key, payload)
		SELECT u.id, u.tenant, u.idem, u.rcpt, string_to_array(u.ch, ','), u.ck, u.pl::jsonb
		FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
		     WITH ORDINALITY AS u(id, tenant, idem, rcpt, ch, ck, pl, ord)
		ORDER BY u.ord
		ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id, seq, created_at`,
		ids, tenants, idems, rcpts, chans, cks, payloads)
	if err != nil {
		return nil, err
	}
	got := make(map[uuid.UUID]Inserted, n)
	for rows.Next() {
		var r Inserted
		if err := rows.Scan(&r.ID, &r.Seq, &r.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		r.Status = StatusQueued
		got[r.ID] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]Inserted, n)
	var missT, missK []string
	for i, it := range items {
		if r, ok := got[it.ID]; ok {
			out[i] = r
		} else {
			missT = append(missT, it.TenantID)
			missK = append(missK, it.IdempotencyKey)
		}
	}
	if len(missT) == 0 {
		return out, nil
	}

	// Idempotent replays: look up the notifications that already own those keys.
	type tk struct{ t, k string }
	existing := map[tk]Inserted{}
	rows, err = s.Pool.Query(ctx, `
		SELECT n.tenant_id, n.idempotency_key, n.id, n.seq, n.created_at, n.status
		FROM notifications n
		JOIN unnest($1::text[], $2::text[]) AS u(t, k) ON n.tenant_id = u.t AND n.idempotency_key = u.k`,
		missT, missK)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t, k string
		var r Inserted
		if err := rows.Scan(&t, &k, &r.ID, &r.Seq, &r.CreatedAt, &r.Status); err != nil {
			rows.Close()
			return nil, err
		}
		r.Replayed = true
		existing[tk{t, k}] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, it := range items {
		if _, ok := got[it.ID]; ok {
			continue
		}
		r, ok := existing[tk{it.TenantID, it.IdempotencyKey}]
		if !ok {
			return nil, fmt.Errorf("insert: row %s neither inserted nor found", it.ID)
		}
		out[i] = r
	}
	return out, nil
}

// MarkPublished records that these notifications are on Kafka.
func (s *Store) MarkPublished(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.Pool.Exec(ctx,
		`UPDATE notifications SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`, ids)
	return err
}

// Notification is a full row.
type Notification struct {
	ID             uuid.UUID       `json:"id"`
	Seq            int64           `json:"seq"`
	TenantID       string          `json:"-"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	RecipientID    string          `json:"recipient_id"`
	Channels       []string        `json:"channels"`
	CollapseKey    *string         `json:"collapse_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Step           int             `json:"-"`
	ChannelIdx     int             `json:"-"`
	Channel        *string         `json:"channel,omitempty"`
	Attempts       int             `json:"attempts"`
	LastError      *string         `json:"last_error,omitempty"`
	PublishedAt    *time.Time      `json:"-"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	DeliveredAt    *time.Time      `json:"delivered_at,omitempty"`
}

const selectCols = `id, seq, tenant_id, idempotency_key, recipient_id, channels, collapse_key, payload,
	status, step, channel_idx, channel, attempts, last_error, published_at, created_at, updated_at, delivered_at`

func scanNotification(row pgx.Row) (Notification, error) {
	var n Notification
	err := row.Scan(&n.ID, &n.Seq, &n.TenantID, &n.IdempotencyKey, &n.RecipientID, &n.Channels, &n.CollapseKey,
		&n.Payload, &n.Status, &n.Step, &n.ChannelIdx, &n.Channel, &n.Attempts, &n.LastError, &n.PublishedAt,
		&n.CreatedAt, &n.UpdatedAt, &n.DeliveredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return n, ErrNotFound
	}
	return n, err
}

func (s *Store) Get(ctx context.Context, tenantID string, id uuid.UUID) (Notification, error) {
	return scanNotification(s.Pool.QueryRow(ctx,
		`SELECT `+selectCols+` FROM notifications WHERE id = $1 AND tenant_id = $2`, id, tenantID))
}

// Unpublished returns outbox rows older than `olderThan` that never reached Kafka.
func (s *Store) Unpublished(ctx context.Context, olderThan time.Duration, limit int) ([]Notification, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+selectCols+` FROM notifications
		WHERE published_at IS NULL AND created_at < now() - $1::interval
		ORDER BY created_at LIMIT $2`, olderThan.String(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// Cancel stops a notification that has not started delivering.
func (s *Store) Cancel(ctx context.Context, tenantID string, id uuid.UUID) (string, error) {
	var status string
	err := s.Pool.QueryRow(ctx, `
		WITH upd AS (
			UPDATE notifications SET status = 'cancelled', updated_at = now()
			WHERE id = $1 AND tenant_id = $2 AND status IN ('queued', 'retrying')
			RETURNING status)
		SELECT status FROM upd
		UNION ALL
		SELECT status FROM notifications WHERE id = $1 AND tenant_id = $2 AND NOT EXISTS (SELECT 1 FROM upd)`,
		id, tenantID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return status, err
}

// ---- Worker-side state transitions. Every write is fenced. ----

// Claim moves a notification into "delivering" for the holder of fencing token
// `fence`. It succeeds only if the message's step matches the row (so stale or
// duplicate messages are dropped) and no newer lock holder has written the row.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, step int, fence int64) (seq int64, ok bool, err error) {
	err = s.Pool.QueryRow(ctx, `
		UPDATE notifications SET status = 'delivering', fence = $3, attempts = attempts + 1, updated_at = now()
		WHERE id = $1 AND step = $2 AND fence < $3 AND status IN ('queued', 'retrying', 'delivering')
		RETURNING seq`, id, step, fence).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return seq, err == nil, err
}

// LastDeliveredSeq is the newest delivered sequence for a collapse key (0 if none).
func (s *Store) LastDeliveredSeq(ctx context.Context, tenantID, recipientID, collapseKey string) (int64, error) {
	var seq int64
	err := s.Pool.QueryRow(ctx, `SELECT last_seq FROM collapse_state
		WHERE tenant_id = $1 AND recipient_id = $2 AND collapse_key = $3`,
		tenantID, recipientID, collapseKey).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

// ErrFenced means a newer lock holder owns the row; the caller's write was rejected.
var ErrFenced = errors.New("fenced: a newer lock holder owns this notification")

func fenced(tag int64) error {
	if tag == 0 {
		return ErrFenced
	}
	return nil
}

// MarkDelivered finishes a notification and advances its collapse key, atomically.
func (s *Store) MarkDelivered(ctx context.Context, id uuid.UUID, fence int64, channel string,
	tenantID, recipientID, collapseKey string, seq int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE notifications
		SET status = 'delivered', channel = $3, delivered_at = now(), updated_at = now(), last_error = NULL
		WHERE id = $1 AND fence = $2`, id, fence, channel)
	if err != nil {
		return err
	}
	if err := fenced(tag.RowsAffected()); err != nil {
		return err
	}
	if collapseKey != "" {
		if _, err := tx.Exec(ctx, `INSERT INTO collapse_state (tenant_id, recipient_id, collapse_key, last_seq)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, recipient_id, collapse_key)
			DO UPDATE SET last_seq = GREATEST(collapse_state.last_seq, EXCLUDED.last_seq)`,
			tenantID, recipientID, collapseKey, seq); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// MarkRetrying records that a retry message for `nextStep` is on Kafka.
func (s *Store) MarkRetrying(ctx context.Context, id uuid.UUID, fence int64, nextStep, channelIdx int, lastErr string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE notifications
		SET status = 'retrying', step = $3, channel_idx = $4, last_error = $5, updated_at = now()
		WHERE id = $1 AND fence = $2`, id, fence, nextStep, channelIdx, lastErr)
	if err != nil {
		return err
	}
	return fenced(tag.RowsAffected())
}

// MarkTerminal sets failed or superseded.
func (s *Store) MarkTerminal(ctx context.Context, id uuid.UUID, fence int64, status, lastErr string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE notifications
		SET status = $3, last_error = NULLIF($4, ''), updated_at = now()
		WHERE id = $1 AND fence = $2`, id, fence, status, lastErr)
	if err != nil {
		return err
	}
	return fenced(tag.RowsAffected())
}

// StatusCounts returns how many notifications are in each status.
func (s *Store) StatusCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT status, count(*) FROM notifications GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var st string
		var c int64
		if err := rows.Scan(&st, &c); err != nil {
			return nil, err
		}
		out[st] = c
	}
	return out, rows.Err()
}

// ClaimReq is one Claim inside a batch.
type ClaimReq struct {
	ID    uuid.UUID
	Step  int
	Fence int64
}

// ClaimRes is the outcome of one ClaimReq.
type ClaimRes struct {
	Seq int64
	OK  bool
}

// ClaimMany is Claim for many notifications in one statement. Each row is
// still guarded by its own step and fence.
func (s *Store) ClaimMany(ctx context.Context, reqs []ClaimReq) ([]ClaimRes, error) {
	ids := make([]uuid.UUID, len(reqs))
	steps := make([]int32, len(reqs))
	fences := make([]int64, len(reqs))
	for i, r := range reqs {
		ids[i], steps[i], fences[i] = r.ID, int32(r.Step), r.Fence
	}
	rows, err := s.Pool.Query(ctx, `
		UPDATE notifications n
		SET status = 'delivering', fence = u.fence, attempts = n.attempts + 1, updated_at = now()
		FROM unnest($1::uuid[], $2::int[], $3::bigint[]) AS u(id, step, fence)
		WHERE n.id = u.id AND n.step = u.step AND n.fence < u.fence
		  AND n.status IN ('queued', 'retrying', 'delivering')
		RETURNING n.id, n.seq`, ids, steps, fences)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	got := make(map[uuid.UUID]int64, len(reqs))
	for rows.Next() {
		var id uuid.UUID
		var seq int64
		if err := rows.Scan(&id, &seq); err != nil {
			return nil, err
		}
		got[id] = seq
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]ClaimRes, len(reqs))
	for i, r := range reqs {
		seq, ok := got[r.ID]
		out[i] = ClaimRes{Seq: seq, OK: ok}
	}
	return out, nil
}

// DeliveredReq marks one notification without a collapse key as delivered.
type DeliveredReq struct {
	ID      uuid.UUID
	Fence   int64
	Channel string
}

// MarkDeliveredMany marks many notifications delivered in one statement.
// The result is ErrFenced for rows a newer lock holder owns, nil otherwise.
// Notifications with a collapse key must use MarkDelivered, which also
// advances collapse_state in the same transaction.
func (s *Store) MarkDeliveredMany(ctx context.Context, reqs []DeliveredReq) ([]error, error) {
	ids := make([]uuid.UUID, len(reqs))
	fences := make([]int64, len(reqs))
	chans := make([]string, len(reqs))
	for i, r := range reqs {
		ids[i], fences[i], chans[i] = r.ID, r.Fence, r.Channel
	}
	rows, err := s.Pool.Query(ctx, `
		UPDATE notifications n
		SET status = 'delivered', channel = u.channel, delivered_at = now(), updated_at = now(), last_error = NULL
		FROM unnest($1::uuid[], $2::bigint[], $3::text[]) AS u(id, fence, channel)
		WHERE n.id = u.id AND n.fence = u.fence
		RETURNING n.id`, ids, fences, chans)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ok := make(map[uuid.UUID]bool, len(reqs))
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ok[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]error, len(reqs))
	for i, r := range reqs {
		if !ok[r.ID] {
			out[i] = ErrFenced
		}
	}
	return out, nil
}
