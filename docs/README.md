# Documentation Assets

This directory contains dashboard screenshots referenced in [`README.md`](../README.md):

1. `grafana_throughput.png`: Capture from Grafana (`http://localhost:3000/d/ml-telemetry-pipeline`) during `make bench-throughput` showing events/sec across ingestion, validation, DLQ, and DB writes alongside consumer lag per partition.
2. `grafana_latency.png`: Capture from Grafana during `make bench-latency` showing API p50/p95/p99 serving latency and Redis cache hit ratio.
