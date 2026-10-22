"""
Partition / Consumer Tuning Benchmark.

Compares configurations:
  - partitions ∈ {1, 6, 12}
  - consumers (writer instances) ∈ {1, 3, 6}
  - batch sizes ∈ {250, 500, 1000}

For each configuration, runs a short load test and measures:
  - Achieved throughput (events/sec consumed)
  - Peak consumer lag
  - DB write latency p99

Output: benchmarks/results/tuning_<timestamp>.md + .json
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from itertools import product

import httpx

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
COMPOSE_DIR = os.path.join(os.path.dirname(__file__), "..")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://pipeline:pipeline@localhost:5432/pipeline")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

RATE = int(os.getenv("TUNING_RATE", "10000"))
RUN_DURATION = int(os.getenv("TUNING_RUN_DURATION", "45"))
WARMUP_DURATION = int(os.getenv("TUNING_WARMUP_DURATION", "5"))
SETTLE_TIME = int(os.getenv("TUNING_SETTLE_TIME", "10"))

# Full sweep by default
PARTITION_OPTIONS = [1, 6, 12]
CONSUMER_OPTIONS = [1, 3, 6]
BATCH_SIZE_OPTIONS = [250, 500, 1000]

# Opt-in quick mode for fast verification
QUICK_MODE = os.getenv("QUICK_MODE", "0") == "1"
if QUICK_MODE:
    PARTITION_OPTIONS = [6, 12]
    CONSUMER_OPTIONS = [1, 3]
    BATCH_SIZE_OPTIONS = [500, 1000]


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


def docker_compose(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose"] + args,
        cwd=COMPOSE_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def recreate_topics(partitions: int):
    """Delete and recreate Kafka topics with given partition count."""
    print(f"    Recreating topics with {partitions} partitions...")
    for topic in ["telemetry.raw", "telemetry.validated", "telemetry.dlq"]:
        docker_compose([
            "exec", "kafka",
            "kafka-topics", "--bootstrap-server", "localhost:29092",
            "--delete", "--topic", topic,
        ])
    time.sleep(3)

    for topic in ["telemetry.raw", "telemetry.validated"]:
        docker_compose([
            "exec", "kafka",
            "kafka-topics", "--bootstrap-server", "localhost:29092",
            "--create", "--topic", topic,
            "--partitions", str(partitions),
            "--replication-factor", "1",
        ])
    dlq_partitions = os.getenv("KAFKA_DLQ_PARTITIONS", "3")
    docker_compose([
        "exec", "kafka",
        "kafka-topics", "--bootstrap-server", "localhost:29092",
        "--create", "--topic", "telemetry.dlq",
        "--partitions", str(dlq_partitions),
        "--replication-factor", "1",
    ])
    time.sleep(2)


def reset_state():
    """Reset PostgreSQL tables and Redis cache between runs."""
    print("    Resetting PostgreSQL and Redis state...")
    try:
        import psycopg
        with psycopg.connect(POSTGRES_DSN, autocommit=True, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE feature_events, entity_latest_features CASCADE;")
    except Exception:
        # Fallback via docker compose exec
        docker_compose([
            "exec", "postgres",
            "psql", "-U", "pipeline", "-d", "pipeline",
            "-c", "TRUNCATE TABLE feature_events, entity_latest_features CASCADE;",
        ])

    try:
        import redis
        client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=3)
        client.flushdb()
        client.close()
    except Exception:
        docker_compose(["exec", "redis", "redis-cli", "flushdb"])


def reset_consumer_offsets():
    """Reset offsets for pipeline consumer groups."""
    for group in ["feature-writer-group", "validator-group"]:
        docker_compose([
            "exec", "kafka",
            "kafka-consumer-groups", "--bootstrap-server", "localhost:29092",
            "--group", group, "--reset-offsets", "--to-earliest", "--all-topics", "--execute",
        ])


def scale_writers(n: int, batch_size: int):
    """Scale feature-writer instances and wait for consumer group to stabilise."""
    print(f"    Scaling feature-writer to {n} instances (batch_size={batch_size})...")
    docker_compose(["stop", "feature-writer"])
    time.sleep(2)
    docker_compose(["rm", "-f", "feature-writer"])
    subprocess.run(
        [
            "docker", "compose",
            "up", "-d",
            "--scale", f"feature-writer={n}",
            "--no-recreate",
        ],
        cwd=COMPOSE_DIR,
        env={**os.environ, "WRITER_BATCH_SIZE": str(batch_size)},
        capture_output=True,
    )
    # Restart validator to ensure fresh subscription metadata
    docker_compose(["restart", "validator"])
    # Wait for rebalance to stabilise
    print("    Waiting for consumer group to stabilise...")
    time.sleep(10)


def run_warmup(rate: int, duration: int = 5):
    """Run a short warmup burst before measurement."""
    print(f"    Running warm-up ({duration}s @ {rate} eps)...")
    proc = subprocess.Popen(
        [
            "docker", "compose",
            "--profile", "simulate",
            "run", "--rm",
            "-e", f"PRODUCER_RATE={rate}",
            "-e", f"PRODUCER_DURATION={duration}",
            "-e", "PRODUCER_MALFORMED_PCT=0",
            "-e", "PRODUCER_ENTITY_CARDINALITY=10000",
            "producer",
        ],
        cwd=COMPOSE_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    proc.wait()
    time.sleep(3)


def run_single_config(partitions: int, consumers: int, batch_size: int) -> dict:
    """Reconfigure the environment, run warm-up and load test, and measure performance."""
    label = f"p{partitions}_c{consumers}_b{batch_size}"
    print(f"\n========================================================")
    print(f"  Config: partitions={partitions}, consumers={consumers}, batch={batch_size}")
    print(f"========================================================")

    # 1. Apply topology changes
    recreate_topics(partitions)
    reset_state()
    scale_writers(consumers, batch_size)
    reset_consumer_offsets()

    # 2. Short warm-up burst
    if WARMUP_DURATION > 0:
        run_warmup(RATE, WARMUP_DURATION)

    # 3. Main measurement run
    print(f"    Starting measurement run: {RATE} eps for {RUN_DURATION}s...")
    proc = subprocess.Popen(
        [
            "docker", "compose",
            "--profile", "simulate",
            "run", "--rm",
            "-e", f"PRODUCER_RATE={RATE}",
            "-e", f"PRODUCER_DURATION={RUN_DURATION}",
            "-e", "PRODUCER_MALFORMED_PCT=0",
            "-e", "PRODUCER_ENTITY_CARDINALITY=100000",
            "producer",
        ],
        cwd=COMPOSE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    # Sample metrics mid-run
    samples = []
    start = time.time()
    while time.time() - start < RUN_DURATION + 10:
        if proc.poll() is not None:
            break
        samples.append({
            "consume_rate": query_prometheus("rate(validator_events_consumed_total[15s])"),
            "write_rate": query_prometheus("rate(writer_writes_total[15s])"),
            "consumer_lag": query_prometheus("sum(kafka_consumergroup_lag_sum)"),
            "write_p99": query_prometheus(
                "histogram_quantile(0.99, rate(writer_write_latency_seconds_bucket[15s]))"
            ),
        })
        time.sleep(10)

    proc.wait()

    if not samples:
        return {"config": label, "error": "no samples"}

    valid_rates = [s["consume_rate"] for s in samples if s["consume_rate"] > 0]
    write_rates = [s["write_rate"] for s in samples if s["write_rate"] > 0]
    lags = [s["consumer_lag"] for s in samples]
    p99s = [s["write_p99"] for s in samples if s["write_p99"] > 0]

    result = {
        "partitions": partitions,
        "consumers": consumers,
        "batch_size": batch_size,
        "avg_consume_rate_eps": round(sum(valid_rates) / len(valid_rates) if valid_rates else 0, 1),
        "peak_consume_rate_eps": round(max(valid_rates or [0]), 1),
        "avg_write_rate_eps": round(sum(write_rates) / len(write_rates) if write_rates else 0, 1),
        "peak_consumer_lag": round(max(lags or [0]), 0),
        "final_consumer_lag": round(lags[-1] if lags else 0, 0),
        "write_p99_ms": round((sum(p99s) / len(p99s) if p99s else 0) * 1000, 1),
    }

    print(f"    Result -> consume={result['avg_consume_rate_eps']} eps, "
          f"lag={result['peak_consumer_lag']}, "
          f"write_p99={result['write_p99_ms']}ms")
    return result


def format_markdown_table(results: list[dict]) -> str:
    header = "| Partitions | Consumers | Batch | Avg Rate (eps) | Peak Lag | Write p99 (ms) |\n"
    sep    = "|-----------|-----------|-------|---------------|----------|----------------|\n"
    rows = []
    for r in results:
        if "error" in r:
            continue
        rows.append(
            f"| {r['partitions']:9} | {r['consumers']:9} | {r['batch_size']:5} "
            f"| {r['avg_consume_rate_eps']:13} | {r['peak_consumer_lag']:8.0f} "
            f"| {r['write_p99_ms']:14} |"
        )
    return header + sep + "\n".join(rows) + "\n"


def run_partition_tuning():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    json_path = os.path.join(RESULTS_DIR, f"tuning_{timestamp}.json")
    md_path = os.path.join(RESULTS_DIR, f"tuning_{timestamp}.md")

    print("=== Partition / Consumer Tuning Benchmark ===")
    print(f"Sweep options:")
    print(f"  Partitions:  {PARTITION_OPTIONS}")
    print(f"  Consumers:   {CONSUMER_OPTIONS}")
    print(f"  Batch sizes: {BATCH_SIZE_OPTIONS}")
    print(f"Each run: {RATE} eps for {RUN_DURATION}s (warmup {WARMUP_DURATION}s)")

    configs = list(product(PARTITION_OPTIONS, CONSUMER_OPTIONS, BATCH_SIZE_OPTIONS))
    print(f"Total configurations to evaluate: {len(configs)}\n")

    all_results = []
    for partitions, consumers, batch_size in configs:
        try:
            result = run_single_config(partitions, consumers, batch_size)
            all_results.append(result)
        except Exception as e:
            print(f"  Config p{partitions}_c{consumers}_b{batch_size} failed: {e}")
            all_results.append({
                "partitions": partitions,
                "consumers": consumers,
                "batch_size": batch_size,
                "error": str(e),
            })
        time.sleep(SETTLE_TIME)

    # Sort by avg consume rate descending
    all_results.sort(key=lambda r: r.get("avg_consume_rate_eps", 0), reverse=True)

    output = {
        "benchmark": "partition_tuning",
        "timestamp": timestamp,
        "rate_eps": RATE,
        "run_duration_seconds": RUN_DURATION,
        "results": all_results,
    }

    with open(json_path, "w") as f:
        json.dump(output, f, indent=2)

    md_content = f"# Partition / Consumer Tuning Results\n\n"
    md_content += f"**Date:** {timestamp}  \n"
    md_content += f"**Target rate:** {RATE} eps per run, {RUN_DURATION}s duration\n\n"
    md_content += format_markdown_table(all_results)

    with open(md_path, "w") as f:
        f.write(md_content)

    print(f"\nResults saved to: {json_path}")
    print(f"Markdown table: {md_path}")
    print("\nTop 3 configurations:")
    for r in all_results[:3]:
        if "error" not in r:
            print(f"  p={r['partitions']} c={r['consumers']} b={r['batch_size']} "
                  f"-> {r['avg_consume_rate_eps']} eps avg, lag={r['peak_consumer_lag']}")

    # Restore default writer scale (1 instance) after benchmark
    print("\nRestoring default feature-writer scale (1 instance)...")
    scale_writers(1, 500)

    return output


if __name__ == "__main__":
    run_partition_tuning()
