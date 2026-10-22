# PLAN.md — Fault-Tolerant ML Telemetry & Feature Streaming Pipeline

## 1. Architecture Overview

```mermaid
flowchart LR
    subgraph Ingestion
        PS[Producer Simulator]
    end

    subgraph Kafka Cluster ["Kafka (KRaft)"]
        RAW[telemetry.raw<br/>12 partitions]
        VAL[telemetry.validated<br/>12 partitions]
        DLQ[telemetry.dlq<br/>3 partitions]
    end

    subgraph Processing
        SV[Schema Validator]
        FW[Feature Writer<br/>consumer group]
    end

    subgraph Storage
        PG[(PostgreSQL 16)]
        RD[(Redis 7)]
    end

    subgraph Serving
        API[FastAPI<br/>Feature Server]
    end

    subgraph Observability
        PROM[Prometheus]
        GRAF[Grafana]
        KE[Kafka Exporter]
        PE[Postgres Exporter]
        RE[Redis Exporter]
    end

    PS -->|keyed by entity_id| RAW
    RAW --> SV
    SV -->|valid| VAL
    SV -->|invalid + reason| DLQ
    VAL --> FW
    FW -->|batch upsert| PG
    FW -->|write-through| RD
    API -->|cache hit| RD
    API -->|fallback| PG

    PROM --- KE & PE & RE
    PROM -.->|scrape /metrics| SV & FW & API
    PROM --> GRAF
```

### Data Flow (happy path)

1. **Producer Simulator** generates telemetry events keyed by `entity_id` → `telemetry.raw`.
2. **Schema Validator** consumes `telemetry.raw`, validates against versioned JSON Schema.
   - Valid → `telemetry.validated`
   - Invalid → `telemetry.dlq` (with error reason in headers)
3. **Feature Writer** (consumer group, horizontally scalable) consumes `telemetry.validated`:
   - Batched insert into PostgreSQL (`ON CONFLICT DO NOTHING` for idempotency).
   - Write-through update to Redis (latest feature vector per entity).
   - Manual offset commit after DB commit (at-least-once delivery).
4. **Feature Serving API** (FastAPI) serves features: Redis first, Postgres fallback, cache repopulation on miss.
5. **Prometheus** scrapes all services + exporters → **Grafana** dashboards.

### Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Delivery guarantee | At-least-once + idempotent writes | Simplest to implement correctly; `ON CONFLICT DO NOTHING` on `event_id` gives effectively-exactly-once results without transactions spanning Kafka + DB |
| Cache strategy | Write-through | Ensures cache is always warm for hot entities; avoids thundering herd on cold start; tradeoff: slightly higher write latency vs cache-aside |
| Partitioning | Key = `entity_id`, 12 partitions default | Ensures per-entity ordering; 12 partitions allows scaling up to 12 consumers |
| Offset management | Manual commit after DB commit | Prevents data loss: if writer crashes before DB commit, events are re-consumed and deduplicated by `event_id` |
| Kafka mode | KRaft (no ZooKeeper) | Simpler operations, fewer containers, production direction for Kafka |

---

## 2. Directory Layout

```
.
├── PLAN.md
├── README.md
├── Makefile
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── schemas/
│   └── telemetry_v1.json          # JSON Schema for telemetry events
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py                # CLI-driven traffic simulator
│   └── config.py                  # Producer Kafka config
│
├── validator/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── validator.py               # Schema validation consumer/producer
│   ├── metrics.py                 # Prometheus metric definitions
│   └── config.py
│
├── writer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── writer.py                  # Feature writer consumer
│   ├── db.py                      # Postgres batch-write logic
│   ├── cache.py                   # Redis write-through logic
│   ├── metrics.py
│   └── config.py
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                     # FastAPI application
│   ├── routes.py                  # /features, /health, /metrics
│   ├── cache.py                   # Redis read + fallback logic
│   ├── metrics.py
│   └── config.py
│
├── db/
│   └── init.sql                   # DDL: tables, indexes, constraints
│
├── observability/
│   ├── prometheus.yml             # Prometheus scrape config
│   ├── alerting_rules.yml         # Alert rules (lag, error rate)
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources.yml
│       │   └── dashboards.yml
│       └── dashboards/
│           └── pipeline.json      # Pre-built Grafana dashboard
│
├── benchmarks/
│   ├── Makefile                   # Targets: throughput, latency, integrity, chaos
│   ├── throughput.py              # Sustained 10K+ eps benchmark
│   ├── partition_tuning.py        # Compare partition/consumer configs
│   ├── read_latency.py            # API load test (Zipfian key dist)
│   ├── data_integrity.py          # End-to-end reconciliation
│   ├── chaos.py                   # Fault injection + reconciliation
│   └── results/                   # Auto-populated benchmark output
│       └── .gitkeep
│
├── tests/
│   ├── conftest.py
│   ├── test_validation.py         # Unit: schema validation logic
│   ├── test_writer_idempotency.py # Unit: ON CONFLICT behavior
│   ├── test_api.py                # Unit: API endpoints
│   └── test_integration.py        # Integration: compose-based E2E
│
├── shared/
│   ├── logging_config.py          # Structured JSON logging setup
│   └── signals.py                 # Graceful shutdown handler
│
└── scripts/
    └── wait_for_kafka.py          # Health-check/readiness script
```

---

## 3. Schemas

### Telemetry Event (JSON Schema v1)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["event_id", "entity_id", "timestamp", "feature_vector", "schema_version"],
  "properties": {
    "event_id":       { "type": "string", "format": "uuid" },
    "entity_id":      { "type": "string", "minLength": 1 },
    "timestamp":      { "type": "string", "format": "date-time" },
    "feature_vector": {
      "type": "array",
      "items": { "type": "number" },
      "minItems": 8,
      "maxItems": 16
    },
    "schema_version": { "type": "integer", "minimum": 1 }
  },
  "additionalProperties": false
}
```

### PostgreSQL DDL (core tables)

```sql
-- Immutable event log (append-only, partitioned by month)
CREATE TABLE feature_events (
    event_id       UUID PRIMARY KEY,
    entity_id      TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,
    feature_vector DOUBLE PRECISION[] NOT NULL,
    schema_version INTEGER NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (timestamp);

-- Latest feature vector per entity (upserted)
CREATE TABLE entity_latest_features (
    entity_id      TEXT PRIMARY KEY,
    event_id       UUID NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,
    feature_vector DOUBLE PRECISION[] NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_feature_events_entity ON feature_events (entity_id, timestamp DESC);
CREATE INDEX idx_feature_events_ingested ON feature_events (ingested_at);
```

### Redis Key Layout

```
entity:features:{entity_id}  →  JSON blob of latest feature vector
                                 TTL: 1 hour (configurable)
```

---

## 4. Phased Build Plan

### Phase 1 — Infrastructure & Producer (containers boot, events flow into Kafka)

**Goal:** `docker compose up` starts Kafka + Postgres + Redis; producer pushes events to `telemetry.raw`.

**Tasks:**
1. Create `docker-compose.yml` with:
   - Kafka (KRaft, single broker) — `confluentinc/cp-kafka:7.6.0`
   - PostgreSQL 16 — `postgres:16-alpine`
   - Redis 7 — `redis:7-alpine`
   - Topic auto-creation via Kafka entrypoint or init container
2. Create `.env.example` with all configurable knobs.
3. Create `db/init.sql` with DDL (tables, partitions, indexes).
4. Create `schemas/telemetry_v1.json`.
5. Build the **Producer Simulator** (`producer/`):
   - CLI: `--rate`, `--duration`, `--burst-multiplier`, `--malformed-pct`, `--entity-cardinality`.
   - `confluent-kafka` producer with `acks=all`, idempotence, snappy compression, batching.
   - Key = `entity_id` → consistent partition assignment.
   - Tracks valid/malformed counts, writes summary to stdout + file.
6. Create `shared/logging_config.py` and `shared/signals.py`.
7. Create `scripts/wait_for_kafka.py`.
8. Create `Makefile` with `up`, `down`, `simulate` targets.
9. Create `.gitignore`.

**Exit criteria:** `make up && make simulate` produces events; `kafka-console-consumer` shows them.

**Commit:** `phase-1: infrastructure, Kafka, producer simulator`

---

### Phase 2 — Schema Validator (bad events go to DLQ, good events to validated topic)

**Goal:** Validator container consumes `telemetry.raw`, routes valid/invalid events.

**Tasks:**
1. Build **Schema Validator** (`validator/`):
   - Consumes `telemetry.raw` (consumer group: `validator-group`).
   - Validates each event against `telemetry_v1.json`.
   - Valid → produce to `telemetry.validated` (preserve key + headers).
   - Invalid → produce to `telemetry.dlq` with headers: `error_reason`, `source_topic`, `source_offset`.
   - Manual offset commit after successful downstream produce.
   - Prometheus metrics: `validated_total`, `rejected_total` (label: `reason`), `validation_latency_seconds` histogram.
   - Graceful shutdown (SIGTERM handler, commit offsets, close consumers).
2. Add Dockerfile, wire into `docker-compose.yml`.
3. Unit tests for validation logic (`tests/test_validation.py`).

**Exit criteria:** produce events with `--malformed-pct 10`; verify counts on `telemetry.validated` + `telemetry.dlq` match expectations.

**Commit:** `phase-2: schema validation service with DLQ routing`

---

### Phase 3 — Feature Writer (events persisted to Postgres + Redis)

**Goal:** Events in `telemetry.validated` are written to Postgres and cached in Redis.

**Tasks:**
1. Build **Feature Writer** (`writer/`):
   - Consumer group `feature-writer-group`, configurable `max.poll.records`.
   - Batch writes to Postgres using `psycopg` `execute_values` / `COPY`:
     - Insert into `feature_events` with `ON CONFLICT (event_id) DO NOTHING`.
     - Upsert into `entity_latest_features`.
   - Write-through Redis: pipeline `SET` for each entity's latest feature vector (with TTL).
   - Manual offset commit AFTER DB commit.
   - Rebalance handler: commit offsets on partition revoke.
   - Retry with exponential backoff on DB/Redis failures; buffer in memory and retry.
   - Prometheus metrics: `writes_total`, `write_batch_size`, `write_latency_seconds`, `errors_total`.
   - Graceful shutdown.
2. Add Dockerfile, wire into `docker-compose.yml`.
3. Unit test for idempotency (`tests/test_writer_idempotency.py`).

**Exit criteria:** full pipeline runs; `SELECT count(*) FROM feature_events` matches valid event count; Redis keys populated.

**Commit:** `phase-3: feature writer with Postgres + Redis write-through`

---

### Phase 4 — Feature Serving API (FastAPI with cache)

**Goal:** Low-latency feature serving via REST API.

**Tasks:**
1. Build **Feature Serving API** (`api/`):
   - `GET /features/{entity_id}` — Redis first, Postgres fallback, cache repopulation on miss.
   - `POST /features/batch` — accepts list of entity IDs, returns map.
   - `GET /health` — checks Redis + Postgres connectivity.
   - `GET /metrics` — Prometheus endpoint.
   - `CACHE_ENABLED` env flag to disable Redis (for benchmarking).
   - Prometheus metrics: `request_latency_seconds` histogram (labels: endpoint, cache_status), `cache_hits_total`, `cache_misses_total`.
2. Add Dockerfile, wire into `docker-compose.yml`.
3. Unit tests (`tests/test_api.py`).

**Exit criteria:** `curl localhost:8000/features/{entity_id}` returns correct feature vector; `/health` returns 200.

**Commit:** `phase-4: FastAPI feature serving with Redis cache`

---

### Phase 5 — Observability (Prometheus + Grafana dashboards)

**Goal:** Full metrics pipeline with pre-provisioned dashboards and alerts.

**Tasks:**
1. Add **Prometheus** to compose with `observability/prometheus.yml`:
   - Scrape targets: validator:8001, writer:8002, api:8000, kafka-exporter:9308, postgres-exporter:9187, redis-exporter:9121.
2. Add **exporters** to compose:
   - `danielqsj/kafka-exporter` — consumer lag, topic throughput.
   - `prometheuscommunity/postgres-exporter` — connections, rows, latency.
   - `oliver006/redis_exporter` — memory, hits/misses, connections.
3. Add **Grafana** to compose with provisioned datasource + dashboards:
   - Dashboard panels: throughput (in/out events/sec), consumer lag per partition, validation reject rate, p50/p95/p99 API latency, cache hit ratio, DB write latency, DLQ depth.
4. Add **alert rules** (`observability/alerting_rules.yml`):
   - Consumer lag > threshold for > 5 min.
   - Error rate > 5% for > 2 min.

**Exit criteria:** Grafana at `localhost:3000` shows live metrics; alerts fire when lag is injected.

**Commit:** `phase-5: Prometheus, Grafana dashboards, alerting rules`

---

### Phase 6 — Benchmarks & Fault-Tolerance Tests

**Goal:** Reproducible, citable benchmark results.

**Tasks:**
1. **Throughput benchmark** (`benchmarks/throughput.py`):
   - Sustain 10K+ eps for 5+ minutes.
   - Record: achieved rate, consumer lag over time, CPU/memory per container.
   - Output CSV + summary to `benchmarks/results/throughput_*.json`.

2. **Partition/consumer tuning** (`benchmarks/partition_tuning.py`):
   - Matrix: partitions ∈ {1, 6, 12}, consumers ∈ {1, 3, 6}, batch sizes ∈ {100, 500, 1000}.
   - Output markdown table to `benchmarks/results/tuning_*.md`.

3. **Read latency** (`benchmarks/read_latency.py`):
   - Async HTTP client, Zipfian key distribution, 10K+ requests.
   - Compare `CACHE_ENABLED=true` vs `false`.
   - Report p50/p95/p99/p99.9 and % improvement.
   - Output to `benchmarks/results/latency_*.json`.

4. **Data integrity** (`benchmarks/data_integrity.py`):
   - Produce N events with known malformed %.
   - Assert: DLQ count == malformed count, Postgres rows == valid count, zero duplicates.
   - Output reconciliation report to `benchmarks/results/integrity_*.json`.

5. **Chaos tests** (`benchmarks/chaos.py`):
   - During peak load: kill/restart feature-writer, Postgres, Redis.
   - After recovery: re-run reconciliation, assert zero data loss.
   - Output to `benchmarks/results/chaos_*.json`.

6. Add Makefile targets: `bench-throughput`, `bench-tuning`, `bench-latency`, `bench-integrity`, `bench-chaos`, `bench-all`.

**Exit criteria:** All benchmarks produce files in `results/`; throughput > 10K eps; reconciliation passes.

**Commit:** `phase-6: benchmarks and chaos testing suite`

---

### Phase 7 — Integration Tests, README & Polish

**Goal:** Production-ready documentation and test coverage.

**Tasks:**
1. **Integration tests** (`tests/test_integration.py`):
   - Compose-based: spin up stack, produce events, assert end-to-end flow.
   - Verify DLQ routing, Postgres counts, Redis cache state, API responses.
2. **README.md**:
   - Architecture diagram (mermaid).
   - Design decisions & trade-offs section.
   - Quick-start instructions.
   - **Results** section populated from real benchmark output.
   - Grafana dashboard screenshots (instructions to capture).
3. Polish:
   - Ensure all services handle SIGTERM gracefully.
   - Verify `make up && make simulate` works from clean clone.
   - Final `.env.example` review.

**Exit criteria:** Clean clone → `make up && make simulate` succeeds; README complete with real numbers.

**Commit:** `phase-7: integration tests, README with benchmark results`

---

## 5. Configuration Matrix

| Variable | Default | Used By |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | all services |
| `KAFKA_PARTITIONS` | `12` | topic creation |
| `POSTGRES_DSN` | `postgresql://pipeline:pipeline@postgres:5432/pipeline` | writer, api |
| `REDIS_URL` | `redis://redis:6379/0` | writer, api |
| `CACHE_ENABLED` | `true` | api |
| `CACHE_TTL_SECONDS` | `3600` | writer, api |
| `PRODUCER_RATE` | `10000` | producer |
| `PRODUCER_DURATION` | `60` | producer |
| `PRODUCER_MALFORMED_PCT` | `0` | producer |
| `PRODUCER_ENTITY_CARDINALITY` | `100000` | producer |
| `PRODUCER_BURST_MULTIPLIER` | `1.0` | producer |
| `WRITER_BATCH_SIZE` | `500` | writer |
| `WRITER_MAX_POLL_RECORDS` | `1000` | writer |
| `VALIDATOR_MAX_POLL_RECORDS` | `1000` | validator |
| `LOG_LEVEL` | `INFO` | all services |
| `LOG_FORMAT` | `json` | all services |

---

## 6. Risk Register

| Risk | Mitigation |
|---|---|
| Kafka single-broker SPOF | Documented; multi-broker variant in compose for chaos tests |
| Producer outpaces consumers → lag | Configurable batch sizes; monitoring + alerting on lag |
| Postgres becomes bottleneck on writes | Batched COPY/execute_values; connection pooling; table partitioning |
| Redis eviction under memory pressure | TTL on keys; `maxmemory-policy allkeys-lru`; Postgres as source of truth |
| Rebalance storms with many consumers | Cooperative sticky assignor; commit on revoke |
| Schema evolution breaks consumers | Versioned schemas; validator rejects unknown versions to DLQ |

---

## 7. Non-Goals (for this scope)

- Multi-datacenter / cloud deployment.
- Authentication / authorization on the API.
- Exactly-once semantics via Kafka transactions (at-least-once + idempotent writes is sufficient).
- Real ML model inference (we simulate feature vectors only).
- Schema registry (Confluent SR) — we use local JSON Schema files for simplicity.
