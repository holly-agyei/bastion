#!/usr/bin/env bash
# Run the k6 load test against the gateway from *inside* the docker network,
# so traffic doesn't bounce through colima's host port-forward (which becomes
# the bottleneck above a few thousand RPS on Mac). Outputs (k6 JSON summary,
# console log, gateway /stats snapshot) land in benchmarks/.
set -euo pipefail

cd "$(dirname "$0")/.."

TARGET_RPS="${TARGET_RPS:-10000}"
DURATION="${DURATION:-30s}"
CLIENTS="${CLIENTS:-2000}"

mkdir -p benchmarks
stamp="$(date +%Y%m%dT%H%M%S)"
out_json="benchmarks/k6_${TARGET_RPS}rps_${stamp}.json"
out_log="benchmarks/k6_${TARGET_RPS}rps_${stamp}.log"

# Build (or rebuild) the load-tester image. It carries load.js and a pinned
# k6 version so runs are reproducible regardless of the host's k6.
docker build -q -t bastion-loadtest -f loadtest/Dockerfile . >/dev/null

# Wait for the gateway to answer health checks on the host port; if k6 will
# run inside the compose network, it talks to the gateway container directly.
echo "[bench] waiting for gateway healthz on host (compose port-forward)"
for i in $(seq 1 60); do
  if curl -fsS "http://localhost:8080/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done

# Snapshot stats before so we can attribute the delta.
curl -fsS "http://localhost:8080/stats" > "benchmarks/gateway_stats_before_${stamp}.txt" || true

echo "[bench] running k6 inside compose network: target=${TARGET_RPS}rps dur=${DURATION}"
docker run --rm --network bastion_default \
  -e TARGET_RPS="${TARGET_RPS}" \
  -e DURATION="${DURATION}" \
  -e CLIENTS="${CLIENTS}" \
  -e BASE_URL="http://gateway:8080" \
  -v "$(pwd)/benchmarks:/out" \
  bastion-loadtest \
    run --summary-export "/out/$(basename "$out_json")" \
    /loadtest/load.js 2>&1 | tee "${out_log}"

curl -fsS "http://localhost:8080/stats" > "benchmarks/gateway_stats_after_${stamp}.txt" || true

echo "[bench] outputs:"
echo "  ${out_json}"
echo "  ${out_log}"
echo "  benchmarks/gateway_stats_before_${stamp}.txt"
echo "  benchmarks/gateway_stats_after_${stamp}.txt"
