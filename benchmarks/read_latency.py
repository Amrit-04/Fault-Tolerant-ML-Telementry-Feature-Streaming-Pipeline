"""
Read Latency Benchmark — API load test with Zipfian key distribution.

Tests:
  - Single-entity GET /features/{entity_id}
  - Cache ON vs Cache OFF comparison
  - Zipfian distribution of entity_ids (hot/cold key pattern)

Reports: p50, p95, p99, p99.9 latencies and % improvement with cache.

Output: benchmarks/results/latency_<timestamp>.json
"""

import asyncio
import json
import math
import os
import random
import statistics
import time
from datetime import datetime, timezone

import httpx

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
API_URL = os.getenv("API_URL", "http://localhost:8000")
TOTAL_REQUESTS = int(os.getenv("BENCH_REQUESTS", "10000"))
CONCURRENCY = int(os.getenv("BENCH_CONCURRENCY", "50"))
ENTITY_CARDINALITY = int(os.getenv("PRODUCER_ENTITY_CARDINALITY", "100000"))
ZIPF_SKEW = float(os.getenv("ZIPF_SKEW", "1.2"))
WARMUP_REQUESTS = int(os.getenv("WARMUP_REQUESTS", "500"))


def generate_zipfian_indices(n: int, k: int, skew: float = 1.2) -> list[int]:
    """Precomputed inverse-power weights for fast Zipfian sampling."""
    weights = [1.0 / (i ** skew) for i in range(1, n + 1)]
    return random.choices(range(n), weights=weights, k=k)


def zipfian_sample(n: int, skew: float = 1.2) -> int:
    """Return a Zipfian-distributed integer in [0, n)."""
    return generate_zipfian_indices(n, 1, skew)[0]


def generate_entity_id(idx: int) -> str:
    return f"entity-{idx:06d}"


def percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = math.ceil(pct / 100.0 * len(sorted_data)) - 1
    return sorted_data[max(0, idx)]


async def run_requests(
    client: httpx.AsyncClient,
    n: int,
    entity_ids: list[str],
    semaphore: asyncio.Semaphore,
    label: str = "",
) -> tuple[list[float], int, int]:
    """Run n async requests and return (latencies, successes, errors)."""
    latencies = []
    success = 0
    errors = 0

    async def single_request():
        nonlocal success, errors
        eid = random.choice(entity_ids)
        try:
            async with semaphore:
                t0 = time.monotonic()
                resp = await client.get(f"{API_URL}/features/{eid}", timeout=10.0)
                latency = time.monotonic() - t0
                latencies.append(latency * 1000)  # convert to ms
                if resp.status_code in (200, 404):
                    success += 1
                else:
                    errors += 1
        except Exception:
            errors += 1

    tasks = [single_request() for _ in range(n)]
    total = len(tasks)
    done = 0
    for coro in asyncio.as_completed(tasks):
        await coro
        done += 1
        if done % 1000 == 0:
            print(f"  {label}: {done}/{total} requests done...")

    return latencies, success, errors


async def benchmark_with_cache_setting(cache_enabled: bool) -> dict:
    """Run the full load test with cache on or off."""
    label = "cache=ON" if cache_enabled else "cache=OFF"
    print(f"\nRunning load test ({label})...")
    print(f"  Requests: {TOTAL_REQUESTS}, Concurrency: {CONCURRENCY}")

    # Build Zipfian-distributed entity IDs via weighted choice
    indices = generate_zipfian_indices(ENTITY_CARDINALITY, TOTAL_REQUESTS + WARMUP_REQUESTS, ZIPF_SKEW)
    zipf_entities = [generate_entity_id(idx) for idx in indices]

    warmup_entities = zipf_entities[:WARMUP_REQUESTS]
    test_entities = zipf_entities[WARMUP_REQUESTS:]

    semaphore = asyncio.Semaphore(CONCURRENCY)

    # Note: cache toggling requires restarting the API with CACHE_ENABLED env var.
    # This script assumes the API is already running with the right setting.
    async with httpx.AsyncClient() as client:
        # Warmup
        print(f"  Warming up with {WARMUP_REQUESTS} requests...")
        await run_requests(client, WARMUP_REQUESTS, warmup_entities, semaphore, label="warmup")

        # Actual benchmark
        start = time.monotonic()
        latencies, success, errors = await run_requests(
            client, TOTAL_REQUESTS, test_entities, semaphore, label=label
        )
        elapsed = time.monotonic() - start

    if not latencies:
        return {"error": "no latencies recorded"}

    rps = TOTAL_REQUESTS / elapsed if elapsed > 0 else 0

    result = {
        "cache_enabled": cache_enabled,
        "requests": TOTAL_REQUESTS,
        "concurrency": CONCURRENCY,
        "elapsed_seconds": round(elapsed, 2),
        "rps": round(rps, 1),
        "success": success,
        "errors": errors,
        "p50_ms": round(percentile(latencies, 50), 2),
        "p95_ms": round(percentile(latencies, 95), 2),
        "p99_ms": round(percentile(latencies, 99), 2),
        "p999_ms": round(percentile(latencies, 99.9), 2),
        "mean_ms": round(statistics.mean(latencies), 2),
        "min_ms": round(min(latencies), 2),
        "max_ms": round(max(latencies), 2),
    }

    print(f"  p50={result['p50_ms']}ms  p95={result['p95_ms']}ms  "
          f"p99={result['p99_ms']}ms  p99.9={result['p999_ms']}ms  "
          f"RPS={result['rps']}")
    return result


async def run_latency_benchmark():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(RESULTS_DIR, f"latency_{timestamp}.json")

    print("=== Read Latency Benchmark ===")
    print(f"API URL: {API_URL}")
    print(f"Zipf skew: {ZIPF_SKEW}, Entity cardinality: {ENTITY_CARDINALITY}")
    print()
    print("NOTE: Run this benchmark twice:")
    print("  1. With CACHE_ENABLED=true (default)")
    print("  2. With CACHE_ENABLED=false (restart API with env var)")
    print()

    # Single run — detect cache status from /health
    try:
        health = httpx.get(f"{API_URL}/health", timeout=5).json()
        cache_status = health.get("redis", "unknown")
        cache_on = cache_status == "ok"
    except Exception:
        cache_on = True  # assume on

    result = await benchmark_with_cache_setting(cache_on)
    result["timestamp"] = timestamp
    result["api_url"] = API_URL
    result["zipf_skew"] = ZIPF_SKEW
    result["entity_cardinality"] = ENTITY_CARDINALITY

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nResults saved to: {output_path}")
    return result


if __name__ == "__main__":
    asyncio.run(run_latency_benchmark())
