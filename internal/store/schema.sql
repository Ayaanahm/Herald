-- Herald schema. Applied idempotently at startup by `herald migrate`.

CREATE TABLE IF NOT EXISTS notifications (
    id              uuid        PRIMARY KEY,           -- UUIDv7: time-ordered, keeps B-tree inserts append-only
    seq             bigserial   NOT NULL,              -- global ingest order, used to resolve out-of-order delivery
    tenant_id       text        NOT NULL,
    idempotency_key text,
    recipient_id    text        NOT NULL,
    channels        text[]      NOT NULL,              -- fallback order, e.g. {push,email,sms}
    collapse_key    text,                              -- newer notification with same key supersedes older ones
    payload         jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'queued',
        -- queued | delivering | retrying | delivered | failed | superseded | cancelled
    step            int         NOT NULL DEFAULT 0,    -- increments each time a retry is scheduled; stale messages carry an old step
    channel_idx     int         NOT NULL DEFAULT 0,
    channel         text,                              -- channel that finally delivered
    attempts        int         NOT NULL DEFAULT 0,
    fence           bigint      NOT NULL DEFAULT 0,    -- highest lock fencing token that has written this row
    last_error      text,
    published_at    timestamptz,                       -- NULL = still in the outbox, not yet on Kafka
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS notifications_idem
    ON notifications (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- The outbox sweeper only ever scans unpublished rows, so keep that index tiny.
CREATE INDEX IF NOT EXISTS notifications_unpublished
    ON notifications (created_at) WHERE published_at IS NULL;

-- Highest sequence number delivered per (tenant, recipient, collapse_key).
CREATE TABLE IF NOT EXISTS collapse_state (
    tenant_id    text   NOT NULL,
    recipient_id text   NOT NULL,
    collapse_key text   NOT NULL,
    last_seq     bigint NOT NULL,
    PRIMARY KEY (tenant_id, recipient_id, collapse_key)
);
