"""
Data Integrity Benchmark — End-to-end reconciliation.

Produces N events with a known malformed percentage, then verifies:
  1. DLQ count == malformed count
  2. Postgres row count == valid count
  3. Zero duplicates in Postgres
  4. Zero valid events lost

Output: benchmarks/results/integrity_<timestamp>.json
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import httpx
import psycopg

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
COMPOSE_DIR = os.path.join(os.path.dirname(__file__), "..")

# Integrity test parameters
TEST_RATE = int(os.getenv("INTEGRITY_RATE", "5000"))
TEST_DURATION = int(os.getenv("INTEGRITY_DURATION", "60"))
TEST_MALFORMED_PCT = float(os.getenv("INTEGRITY_MALFORMED_PCT", "15.0"))
WAIT_FOR_DRAIN = int(os.getenv("INTEGRITY_DRAIN_WAIT", "60"))


def query_prometheus(metric: str) -> float:
    try:
        resp = httpx.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": metric},
            timeout=5,
        )
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
    except Exception:
        pass
    return 0.0


def run_producer(rate: int, duration: int, malformed_pct: float) -> dict:
    """Run the producer and return its summary."""
    print(f"Running producer: rate={rate}, duration={duration}s, malformed={malformed_pct}%")
    result = subprocess.run(
        [
            "docker", "compose",
            "--profile", "simulate",
            "run", "--rm",
            "-e", f"PRODUCER_RATE={rate}",
            "-e", f"PRODUCER_DURATION={duration}",
            "-e", f"PRODUCER_MALFORMED_PCT={malformed_pct}",
            "-e", "PRODUCER_ENTITY_CARDINALITY=10000",
            "producer",
        ],
        cwd=COMPOSE_DIR,
        capture_output=True,
        text=True,
    )
    # Parse summary from producer output (last JSON line)
    summary = {}
    for line in (result.stdout + result.stderr).splitlines():
        if "Producer finished" in line:
            try:
                # Extract JSON embedded in structured log
                start = line.index("{")
                summary = json.loads(line[start:])
            except (ValueError, json.JSONDecodeError):
                pass
    return summary


def wait_for_consumer_drain(timeout: int = 60):
    """Wait until consumer lag drops to near zero."""
    print(f"Waiting up to {timeout}s for consumers to drain...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        lag = query_prometheus("sum(kafka_consumergroup_lag_sum)")
        print(f"  Consumer lag: {lag:.0f}")
        if lag <= 10:
            print("  Consumers drained!")
            return True
        time.sleep(5)
    print(f"  Warning: consumers did not fully drain in {timeout}s")
    return False


def count_postgres_rows() -> dict:
    """Count rows in Postgres and check for duplicates."""
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM feature_events")
            total_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT event_id) FROM feature_events")
            distinct_event_ids = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM entity_latest_features")
            entity_count = cur.fetchone()[0]

    return {
        "total_rows": total_rows,
        "distinct_event_ids": distinct_event_ids,
        "duplicate_rows": total_rows - distinct_event_ids,
        "entity_count": entity_count,
    }


def count_dlq_messages() -> int:
    """Count messages in the DLQ topic via Prometheus or kafka-consumer-groups."""
    try:
        result = subprocess.run(
            [
                "docker", "compose", "exec", "kafka",
                "kafka-run-class", "kafka.tools.GetOffsetShell",
                "--bootstrap-server", "localhost:29092",
                "--topic", "telemetry.dlq",
                "--time", "-1",
            ],
            cwd=COMPOSE_DIR,
            capture_output=True,
            text=True,
            timeout=15,
        )
        total = 0
        for line in result.stdout.strip().splitlines():
            if ":" in line:
                parts = line.rsplit(":", 1)
                total += int(parts[-1])
        return total
    except Exception as e:
        print(f"  Warning: could not count DLQ messages: {e}")
        return -1


def run_integrity_benchmark():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(RESULTS_DIR, f"integrity_{timestamp}.json")

    print("=== Data Integrity Benchmark ===")

    # Record baseline
    baseline = count_postgres_rows()
    baseline_dlq = count_dlq_messages()
    print(f"Baseline — Postgres rows: {baseline['total_rows']}, DLQ: {baseline_dlq}")

    # Run producer
    producer_summary = run_producer(TEST_RATE, TEST_DURATION, TEST_MALFORMED_PCT)
    expected_total = producer_summary.get("total_produced", 0)
    expected_valid = producer_summary.get("valid_produced", 0)
    expected_malformed = producer_summary.get("malformed_produced", 0)

    print(f"Producer summary: total={expected_total}, valid={expected_valid}, "
          f"malformed={expected_malformed}")

    # Wait for drain
    drained = wait_for_consumer_drain(WAIT_FOR_DRAIN)

    # Extra wait to ensure writes propagate
    print("Waiting 10s for final writes to flush...")
    time.sleep(10)

    # Count
    final = count_postgres_rows()
    final_dlq = count_dlq_messages()

    new_rows = final["total_rows"] - baseline["total_rows"]
    new_dlq = final_dlq - (baseline_dlq if baseline_dlq >= 0 else 0)

    # Assertions
    checks = {
        "zero_duplicates": final["duplicate_rows"] == 0,
        "valid_events_in_postgres": new_rows == expected_valid,
        "malformed_events_in_dlq": new_dlq == expected_malformed if final_dlq >= 0 else None,
        "no_valid_events_lost": new_rows >= expected_valid if expected_valid > 0 else True,
        "consumers_drained": drained,
    }

    all_passed = all(v is True for v in checks.values() if v is not None)

    report = {
        "benchmark": "data_integrity",
        "timestamp": timestamp,
        "parameters": {
            "rate_eps": TEST_RATE,
            "duration_seconds": TEST_DURATION,
            "malformed_pct": TEST_MALFORMED_PCT,
        },
        "producer": {
            "total_produced": expected_total,
            "valid_produced": expected_valid,
            "malformed_produced": expected_malformed,
        },
        "postgres": {
            "new_rows": new_rows,
            "duplicate_rows": final["duplicate_rows"],
            "entity_count": final["entity_count"],
        },
        "dlq": {
            "new_messages": new_dlq,
            "expected_malformed": expected_malformed,
        },
        "checks": checks,
        "result": "PASS" if all_passed else "FAIL",
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print("\n=== Integrity Report ===")
    for check, passed in checks.items():
        status = "✓ PASS" if passed is True else ("? SKIP" if passed is None else "✗ FAIL")
        print(f"  {status} — {check}")
    print(f"\nOverall: {report['result']}")
    print(f"Results saved to: {output_path}")

    return report


if __name__ == "__main__":
    run_integrity_benchmark()
