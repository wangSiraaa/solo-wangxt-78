-- 0002_sequences_generations.sql — per-business-key sequences, endpoint
-- generations, bounded secret rotation, poison-message skips.
-- Idempotent-safe to re-run via the migration runner (applied once, tracked
-- in schema_migrations); upgrades bcc7c845 databases in place and preserves
-- all existing rows and delivery states.

-- ---------- endpoints: generations + rotation window ----------
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS lineage_id UUID;
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS generation INT NOT NULL DEFAULT 1;
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS previous_secret TEXT;
ALTER TABLE endpoints ADD COLUMN IF NOT EXISTS previous_secret_expires_at TIMESTAMPTZ;

-- Every existing endpoint becomes generation 1 of its own lineage.
UPDATE endpoints SET lineage_id = id WHERE lineage_id IS NULL;
ALTER TABLE endpoints ALTER COLUMN lineage_id SET NOT NULL;
ALTER TABLE endpoints ALTER COLUMN lineage_id SET DEFAULT gen_random_uuid();

ALTER TABLE endpoints DROP CONSTRAINT endpoints_status_check;
ALTER TABLE endpoints ADD CONSTRAINT endpoints_status_check
    CHECK (status IN ('active', 'disabled', 'superseded'));

-- A lineage always has AT MOST ONE active generation (backstop against
-- reactivating a superseded one).
CREATE UNIQUE INDEX IF NOT EXISTS endpoints_one_active_per_lineage
    ON endpoints (lineage_id) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS endpoints_lineage_idx ON endpoints (lineage_id, status);

-- ---------- per-key sequence counter ----------
CREATE TABLE IF NOT EXISTS key_sequences (
    business_key TEXT PRIMARY KEY,
    next_seq     BIGINT NOT NULL
);

-- ---------- events: ordering context ----------
ALTER TABLE events ADD COLUMN IF NOT EXISTS business_key TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS key_seq BIGINT;

-- Existing events become their own key at seq 1 (preserves their identity;
-- they were never ordered relative to each other).
UPDATE events SET business_key = id::text, key_seq = 1 WHERE business_key IS NULL;
ALTER TABLE events ALTER COLUMN business_key SET NOT NULL;
ALTER TABLE events ALTER COLUMN key_seq SET NOT NULL;
ALTER TABLE events ADD CONSTRAINT events_business_key_key_seq_key
    UNIQUE (business_key, key_seq);

-- Continue each key's sequence after the highest existing seq.
INSERT INTO key_sequences (business_key, next_seq)
SELECT business_key, max(key_seq) + 1 FROM events GROUP BY business_key
ON CONFLICT (business_key) DO NOTHING;

-- ---------- deliveries: denormalized ordering context + skips ----------
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS business_key TEXT;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS key_seq BIGINT;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS lineage_id UUID;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS skip_reason TEXT;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS skipped_by TEXT;
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS skipped_at TIMESTAMPTZ;

UPDATE deliveries d SET business_key = e.business_key, key_seq = e.key_seq
FROM events e WHERE d.event_id = e.id AND d.business_key IS NULL;
UPDATE deliveries d SET lineage_id = ep.lineage_id
FROM endpoints ep WHERE d.endpoint_id = ep.id AND d.lineage_id IS NULL;

ALTER TABLE deliveries ALTER COLUMN business_key SET NOT NULL;
ALTER TABLE deliveries ALTER COLUMN key_seq SET NOT NULL;
ALTER TABLE deliveries ALTER COLUMN lineage_id SET NOT NULL;

ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'delivering', 'succeeded', 'dead', 'skipped'));

CREATE INDEX IF NOT EXISTS deliveries_key_order_idx
    ON deliveries (lineage_id, business_key, key_seq);
