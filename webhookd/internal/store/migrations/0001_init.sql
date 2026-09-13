-- 0001_init.sql — webhook delivery service schema.
-- The database is the source of truth: events, endpoint subscriptions and
-- pending deliveries are written atomically in one transaction (transactional
-- outbox). Delivery workers only ever read/claim rows from these tables.

CREATE TABLE IF NOT EXISTS endpoints (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- All generations of one logical endpoint share a lineage id. Switching
    -- the receiver domain creates a NEW generation row; the old one is
    -- superseded, never edited in place.
    lineage_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    generation        INT NOT NULL DEFAULT 1,
    url               TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    -- Per-endpoint signing secret (whsec_...).
    secret            TEXT NOT NULL,
    -- Bounded dual-secret window: after rotation the previous secret stays
    -- acceptable only until previous_secret_expires_at, never indefinitely.
    previous_secret   TEXT,
    previous_secret_expires_at TIMESTAMPTZ,
    -- Event types this endpoint subscribes to; '*' matches everything.
    subscribed_events TEXT[] NOT NULL DEFAULT '{}',
    status            TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'disabled', 'superseded')),
    -- Per-endpoint retry policy.
    max_attempts      INT NOT NULL DEFAULT 8   CHECK (max_attempts BETWEEN 1 AND 25),
    backoff_base_ms   INT NOT NULL DEFAULT 1000 CHECK (backoff_base_ms BETWEEN 50 AND 600000),
    backoff_max_ms    INT NOT NULL DEFAULT 300000 CHECK (backoff_max_ms BETWEEN 100 AND 3600000),
    http_timeout_ms   INT NOT NULL DEFAULT 10000 CHECK (http_timeout_ms BETWEEN 500 AND 60000),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS endpoints_lineage_idx ON endpoints (lineage_id, status);

-- Per-business-key sequence counter. Events of the same business key (e.g.
-- one order) get monotonically increasing key_seq values, assigned inside
-- the publish transaction.
CREATE TABLE IF NOT EXISTS key_sequences (
    business_key TEXT PRIMARY KEY,
    next_seq     BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    -- The event identity. Immutable once created; retries and dead-letter
    -- replays always reference this same id, never a new row.
    id              UUID PRIMARY KEY,
    -- Producer-supplied idempotency key: re-publishing with the same key
    -- returns the original event instead of creating a duplicate fact.
    idempotency_key TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    -- Ordering scope: events sharing a business key are delivered in
    -- key_seq order; different keys are delivered in parallel.
    business_key    TEXT NOT NULL,
    key_seq         BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (business_key, key_seq)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    endpoint_id     UUID NOT NULL REFERENCES endpoints(id),
    -- Denormalized ordering/lineage context (copied at creation, kept in
    -- sync when re-pointed across generations).
    business_key    TEXT NOT NULL,
    key_seq         BIGINT NOT NULL,
    lineage_id      UUID NOT NULL,
    -- pending -> delivering -> (pending [retry] | succeeded | dead -> skipped)
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivering', 'succeeded', 'dead', 'skipped')),
    attempt_count   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INT,
    last_error      TEXT,
    -- Human decision trail for skipping a poison message.
    skip_reason     TEXT,
    skipped_by      TEXT,
    skipped_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One delivery per (event, endpoint generation): retries reuse this row,
    -- and a late receipt from an old generation can only confirm its own row.
    UNIQUE (event_id, endpoint_id)
);

CREATE INDEX IF NOT EXISTS deliveries_claim_idx
    ON deliveries (next_attempt_at)
    WHERE status = 'pending';

-- Supports the per-key head-of-line check in the claim query.
CREATE INDEX IF NOT EXISTS deliveries_key_order_idx
    ON deliveries (lineage_id, business_key, key_seq);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id          BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id),
    attempt_no  INT NOT NULL,
    status_code INT,          -- NULL when the request never got a response (timeout, reset, ...)
    error       TEXT NOT NULL DEFAULT '',
    duration_ms INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (delivery_id, attempt_no)
);
