# =============================================================================
# Fault-Tolerant ML Telemetry Pipeline — Makefile
# =============================================================================

.PHONY: up down logs simulate bench-throughput bench-latency \
        bench-integrity bench-chaos bench-all test test-integration \
        build clean ps help

# Default target
help:
	@echo "Available targets:"
	@echo "  up                 — Start all pipeline services"
	@echo "  down               — Stop all services and remove containers"
	@echo "  logs               — Tail logs from all services"
	@echo "  build              — Build all Docker images"
	@echo "  ps                 — Show running containers"
	@echo "  simulate           — Run the producer simulator (10K eps, 60s)"
	@echo "  simulate-fast      — Quick smoke test (1K eps, 10s)"
	@echo "  test               — Run unit tests"
	@echo "  test-integration   — Run integration tests (requires 'make up' first)"
	@echo "  bench-throughput   — Throughput benchmark (10K+ eps, 5+ min)"
	@echo "  bench-latency      — Read latency benchmark (cache on/off)"
	@echo "  bench-integrity    — Data integrity reconciliation"
	@echo "  bench-chaos        — Chaos / fault-tolerance tests"
	@echo "  bench-all          — Run all benchmarks sequentially"
	@echo "  clean              — Remove volumes and build artifacts"

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@docker compose ps

down:
	docker compose down

build:
	docker compose build

ps:
	docker compose ps

logs:
	docker compose logs -f

# ---------------------------------------------------------------------------
# Producer Simulator
# ---------------------------------------------------------------------------

simulate:
	docker compose --profile simulate run --rm \
	  -e PRODUCER_RATE=10000 \
	  -e PRODUCER_DURATION=60 \
	  -e PRODUCER_MALFORMED_PCT=5 \
	  -e PRODUCER_ENTITY_CARDINALITY=100000 \
	  producer

simulate-fast:
	docker compose --profile simulate run --rm \
	  -e PRODUCER_RATE=1000 \
	  -e PRODUCER_DURATION=10 \
	  -e PRODUCER_MALFORMED_PCT=10 \
	  -e PRODUCER_ENTITY_CARDINALITY=1000 \
	  producer

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test:
	pip install pytest pytest-asyncio httpx jsonschema psycopg[binary] redis fastapi uvicorn pydantic prometheus-client 2>/dev/null || true
	python -m pytest tests/test_validation.py tests/test_writer_idempotency.py tests/test_api.py -v

test-integration:
	INTEGRATION=1 python -m pytest tests/test_integration.py -v --timeout=300

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

bench-throughput:
	@echo "=== Throughput Benchmark (10K+ eps for 5+ min) ==="
	python benchmarks/throughput.py

bench-latency:
	@echo "=== Read Latency Benchmark ==="
	python benchmarks/read_latency.py

bench-integrity:
	@echo "=== Data Integrity Benchmark ==="
	python benchmarks/data_integrity.py

bench-chaos:
	@echo "=== Chaos / Fault-Tolerance Tests ==="
	python benchmarks/chaos.py

bench-all: bench-integrity bench-throughput bench-latency bench-chaos
	@echo "All benchmarks complete. Results in benchmarks/results/"

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

reconcile:
	@echo "Running quick reconciliation check..."
	python benchmarks/data_integrity.py

clean:
	docker compose down -v
	find . -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
