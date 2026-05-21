# Bastion — Measured Benchmark Results

All numbers below were produced by running the docker-compose stack on a
single host. Raw k6 JSON summaries and console logs are in this directory
(`k6_<target>rps_<timestamp>.{json,log}`). The harness lives at
[scripts/run_bench.sh](../scripts/run_bench.sh) and [loadtest/load.js](../loadtest/load.js).

## Test harness

| Item | Value |
|---|---|
| Host | macOS 14 (Darwin 24.6.0), Intel x86_64 |
| Docker runtime | colima 0.10.1 on `Virtualization.framework` |
| VM allocation | 6 vCPU, 9.71 GiB RAM |
| Services co-located | gateway, backend, redis, kafka, autoscaler, loadgen (all on the VM) |
| Load tool | k6 v0.51.0, run inside the compose network (bypasses host port-forward) |
| Per-client limit | 200 req / 1 s sliding window |
| Rate-limit shards | 64 |
| Backend gRPC pool size | 32 connections (single-target) |
| Backend simulated work | 100 µs (spin) per request |

Co-locating everything on a single 6-vCPU VM is the right way to test the
*code*, and a terrible way to measure absolute throughput. The numbers
below are the achievable ceiling **on this hardware in this topology**, not
production limits. The bottleneck is CPU contention between five
co-located services, not anything in the gateway's request path.

## Throughput

| Target RPS (sustained + 4 k bursty) | Achieved total RPS | Notes |
|---:|---:|---|
| 500 + 4 000 = 4 500 | 4 499 | Under capacity. Clean baseline. |
| 3 000 + 4 000 = 7 000 | 6 506 | Near ceiling. p95 ≈ 280 ms. |
| 10 000 + 4 000 = 14 000 | 5 408 | Over-saturated. Queueing dominates. |
| 20 000 + 4 000 = 24 000 | 5 845 | Over-saturated. |
| 30 000 + 4 000 = 34 000 | 5 347 | Worse than 10 k target — thrashing. |

**Single-host ceiling: ≈ 6 500 req/s combined** before queue depth blows out
p95 past 300 ms. Mid-test `docker stats` showed gateway at ~116 % CPU,
backend at ~93 %, Redis at ~137 %, Kafka at ~96 %, k6 at ~120 % — the VM is
CPU-saturated at the ceiling, not waiting on any single component.

## Latency

Latency reported here is the `expected_response:true` slice — i.e. the
`200`s that completed end-to-end (HTTP intake → Lua-rate-limit Redis hop →
gRPC forward to backend → response written). 429s are excluded from this
slice; they have their own rejection-path metrics below.

### Unloaded baseline (target 500 RPS sustained + 4 k bursty)

This is the latency budget when the gateway is *not* at the queueing knee.

| Metric | Sustained-200 slice |
|---|---:|
| min  | 0.80 ms |
| p50  | **7.14 ms** |
| p90  | 26.48 ms |
| p95  | 37.09 ms |
| max  | 136.94 ms |

7 ms p50 is the honest end-to-end number for one request through this
stack: it's HTTP server overhead + one Redis EVALSHA roundtrip + one
gRPC unary call + Kafka publish (async, off the hot path) + response.

### At capacity (target 3 k RPS sustained + 4 k bursty, achieved 6.5 k)

| Metric | Sustained-200 slice |
|---|---:|
| p50  | 108.7 ms |
| p90  | 231.3 ms |
| p95  | 280.5 ms |
| max  | 644.8 ms |

Latency is dominated by VU-queueing at this load — see the
`dropped_iterations` metric in the JSON summary.

## Rate-limit accuracy

From the unit test `TestAllow_ConcurrentAccuracy` (run under `-race`):

- Limit configured: **500** requests in a 2 s window.
- Concurrent attempts: **5 000** goroutines, all targeting the same key.
- Allowed by the limiter: **500**.
- Overage: **0**.

The Lua script's atomicity (one EVALSHA per check; ZREMRANGEBYSCORE +
ZCARD + ZADD inside the same script invocation) is what makes this
exact, not just "very close." See [gateway/internal/ratelimit/sliding_window.lua](../gateway/internal/ratelimit/sliding_window.lua).

## Rejection → autoscaler alert latency

Procedure:

1. Bring stack up with empty Kafka volumes (`docker compose down -v && up`).
2. Restart the autoscaler so its `LastOffset` consumer joins fresh.
3. Drive a 5 s burst at 3 000 req/s across 4 abusive clients (each with a
   200 req/s per-client limit), so ~2 200 req/s become 429s.
4. Capture the burst start wallclock and the timestamp of the first ALERT
   line in the autoscaler container log.

Result on this run:

| Event | Wallclock (ms since epoch) |
|---|---:|
| Burst start | 1 779 382 439 648 |
| First autoscaler ALERT logged | 1 779 382 441 436 |
| **Delta** | **1 788 ms** |

Pipeline traversed: HTTP request → gateway Lua check → 429 issued →
JSON event into Kafka producer's async buffer → broker write (Snappy,
batched) → consumer FetchMessage → in-memory sliding counter increments
→ counter ≥ threshold → ALERT.

Theoretical floor on this burst was ~455 ms (time for the rejection
counter to reach the 1 000 threshold at 2 200 r/s of 429s). Observed:
1.79 s. So the gateway-to-alert path adds ~1.3 s on top, dominated by
Kafka commit/poll cadence.

## What was *not* measured

- HTTP/1.1 vs gRPC delta. The gateway speaks gRPC to the backend; I did
  not implement an HTTP/1.1 control path, so the "X ms faster than
  HTTP/1.1" framing some sources use is not supported by anything I ran.
- Multi-host, sharded Redis Cluster, multi-broker Kafka. The compose
  stack is single-node for everything; the per-client shard keys are
  designed to distribute across a Cluster but I haven't run that
  configuration here.
- Cold-start vs warm. All numbers above are after a 5–10 s warm-up implied
  by k6's ramp-up.
