-- 0001_init.sql — original schema (bcc7c845). DO NOT EDIT: later changes
-- live in incremental migrations (0002_*.sql and up) so existing databases
-- upgrade in place without losing data or delivery state.

CREATE TABLE IF NOT EXISTS endpoints (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url               TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    secret            TEXT NOT NULL,
    subscribed_events TEXT[] NOT NULL DEFAULT '{}',
    status            TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    max_attempts      INT NOT NULL DEFAULT 8   CHECK (max_attempts BETWEEN 1 AND 25),
    backoff_base_ms   INT NOT NULL DEFAULT 1000 CHECK (backoff_base_ms BETWEEN 50 AND 600000),
    backoff_max_ms    INT NOT NULL DEFAULT 300000 CHECK (backoff_max_ms BETWEEN 100 AND 3600000),
    http_timeout_ms   INT NOT NULL DEFAULT 10000 CHECK (http_timeout_ms BETWEEN 500 AND 60000),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    endpoint_id     UUID NOT NULL REFERENCES endpoints(id),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivering', 'succeeded', 'dead')),
    attempt_count   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);

CREATE INDEX IF NOT EXISTS deliveries_claim_idx
    ON deliveries (next_attempt_at)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id          BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id),
    attempt_no  INT NOT NULL,
    status_code INT,
    error       TEXT NOT NULL DEFAULT '',
    duration_ms INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (delivery_id, attempt_no)
);
