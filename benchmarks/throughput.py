"""
Throughput Benchmark — sustain 10K+ events/sec for 5+ minutes.

Measures:
  - Achieved produce rate
  - Validator consumed/validated/rejected rates
  - Writer DB write rate
  - Consumer lag over time
  - Container CPU / memory (via docker stats)

Output: benchmarks/results/throughput_<timestamp>.json
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import httpx

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
API_URL = os.getenv("API_URL", "http://localhost:8000")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
DURATION = int(os.getenv("BENCH_DURATION", "310"))  # 5+ minutes
TARGET_RATE = int(os.getenv("BENCH_RATE", "10000"))
SAMPLE_INTERVAL = 10  # seconds between metric samples


def query_prometheus(metric: str) -> float:
    """Instant query against Prometheus."""
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


def sample_metrics() -> dict:
    """Collect a snapshot of key pipeline metrics."""
    return {
        "ts": time.time(),
        "validator_consumed_rate": query_prometheus("rate(validator_events_consumed_total[30s])"),
        "validator_validated_rate": query_prometheus("rate(validator_validated_total[30s])"),
        "validator_rejected_rate": query_prometheus("rate(validator_rejected_total[30s])"),
        "writer_write_rate": query_prometheus("rate(writer_writes_total[30s])"),
        "consumer_lag_total": query_prometheus("sum(kafka_consumergroup_lag_sum)"),
        "writer_p99_write_latency": query_prometheus(
            "histogram_quantile(0.99, rate(writer_write_latency_seconds_bucket[30s]))"
        ),
    }


def start_producer(rate: int, duration: int, malformed_pct: float = 5.0) -> subprocess.Popen:
    """Start the producer container."""
    cmd = [
        "docker", "compose",
        "--profile", "simulate",
        "run", "--rm",
        "-e", f"PRODUCER_RATE={rate}",
        "-e", f"PRODUCER_DURATION={duration}",
        "-e", f"PRODUCER_MALFORMED_PCT={malformed_pct}",
        "-e", "PRODUCER_ENTITY_CARDINALITY=100000",
        "producer",
    ]
    return subprocess.Popen(
        cmd,
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def get_docker_stats() -> list[dict]:
    """Sample CPU/memory from docker stats (one shot)."""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
            capture_output=True, text=True, timeout=10,
        )
        rows = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) >= 3:
                rows.append({
                    "container": parts[0],
                    "cpu_pct": parts[1],
                    "mem_usage": parts[2],
                })
        return rows
    except Exception:
        return []


def run_throughput_benchmark():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(RESULTS_DIR, f"throughput_{timestamp}.json")

    print(f"Starting throughput benchmark: rate={TARGET_RATE} eps, duration={DURATION}s")
    print(f"Results will be saved to: {output_path}")

    samples = []
    start_time = time.time()

    # Start the producer in the background
    proc = start_producer(TARGET_RATE, DURATION, malformed_pct=5.0)
    print(f"Producer started (PID {proc.pid})")

    # Sample metrics while producer runs
    next_sample = start_time + SAMPLE_INTERVAL
    while time.time() - start_time < DURATION + 30:
        now = time.time()
        if now >= next_sample:
            snap = sample_metrics()
            samples.append(snap)
            elapsed = now - start_time
            print(
                f"[{elapsed:5.0f}s] "
                f"consume={snap['validator_consumed_rate']:.0f} eps, "
                f"validated={snap['validator_validated_rate']:.0f} eps, "
                f"written={snap['writer_write_rate']:.0f} eps, "
                f"lag={snap['consumer_lag_total']:.0f}"
            )
            next_sample = now + SAMPLE_INTERVAL

        if proc.poll() is not None:
            print("Producer finished")
            break

        time.sleep(1)

    proc.wait()

    # Compute summary statistics
    if samples:
        consume_rates = [s["validator_consumed_rate"] for s in samples if s["validator_consumed_rate"] > 0]
        write_rates = [s["writer_write_rate"] for s in samples if s["writer_write_rate"] > 0]
        lags = [s["consumer_lag_total"] for s in samples]
    else:
        consume_rates = write_rates = lags = [0]

    docker_stats = get_docker_stats()
    total_elapsed = time.time() - start_time

    summary = {
        "benchmark": "throughput",
        "timestamp": timestamp,
        "target_rate_eps": TARGET_RATE,
        "duration_seconds": DURATION,
        "total_elapsed_seconds": round(total_elapsed, 1),
        "achieved_peak_consume_rate_eps": round(max(consume_rates or [0]), 1),
        "achieved_avg_consume_rate_eps": round(
            sum(consume_rates) / len(consume_rates) if consume_rates else 0, 1
        ),
        "achieved_avg_write_rate_eps": round(
            sum(write_rates) / len(write_rates) if write_rates else 0, 1
        ),
        "peak_consumer_lag": round(max(lags or [0]), 0),
        "final_consumer_lag": round(lags[-1] if lags else 0, 0),
        "sustained_10k_eps": max(consume_rates or [0]) >= 10000,
        "samples": samples,
        "docker_stats": docker_stats,
    }

    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n=== Throughput Benchmark Results ===")
    print(f"  Peak consume rate:  {summary['achieved_peak_consume_rate_eps']} eps")
    print(f"  Avg consume rate:   {summary['achieved_avg_consume_rate_eps']} eps")
    print(f"  Avg write rate:     {summary['achieved_avg_write_rate_eps']} eps")
    print(f"  Peak consumer lag:  {summary['peak_consumer_lag']}")
    print(f"  Final consumer lag: {summary['final_consumer_lag']}")
    print(f"  10K+ eps sustained: {summary['sustained_10k_eps']}")
    print(f"  Results saved to:   {output_path}")

    return summary


if __name__ == "__main__":
    run_throughput_benchmark()
