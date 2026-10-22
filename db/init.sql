-- =============================================================================
-- DDL for Fault-Tolerant ML Telemetry Pipeline
-- =============================================================================

-- Immutable event log — partitioned by month for efficient range queries
CREATE TABLE IF NOT EXISTS feature_events (
    event_id       UUID          NOT NULL,
    entity_id      TEXT          NOT NULL,
    timestamp      TIMESTAMPTZ   NOT NULL,
    feature_vector DOUBLE PRECISION[] NOT NULL,
    schema_version INTEGER       NOT NULL,
    ingested_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions for the current period and near future
CREATE TABLE IF NOT EXISTS feature_events_default PARTITION OF feature_events DEFAULT;

CREATE TABLE IF NOT EXISTS feature_events_2026_10
    PARTITION OF feature_events
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE IF NOT EXISTS feature_events_2026_11
    PARTITION OF feature_events
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE TABLE IF NOT EXISTS feature_events_2026_12
    PARTITION OF feature_events
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Latest feature vector per entity (upserted on each write)
CREATE TABLE IF NOT EXISTS entity_latest_features (
    entity_id      TEXT            PRIMARY KEY,
    event_id       UUID            NOT NULL,
    timestamp      TIMESTAMPTZ     NOT NULL,
    feature_vector DOUBLE PRECISION[] NOT NULL,
    updated_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_feature_events_entity
    ON feature_events (entity_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_feature_events_ingested
    ON feature_events (ingested_at);

CREATE INDEX IF NOT EXISTS idx_entity_latest_entity
    ON entity_latest_features (entity_id);
