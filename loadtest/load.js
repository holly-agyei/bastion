// k6 load test for Bastion gateway.
//
// Two scenarios run concurrently:
//
//   sustained — drives constant throughput at TARGET_RPS to measure
//   sustained-throughput latency. Most clients stay under the per-client
//   limit, so this scenario also exercises the allowed (forwarded-to-gRPC)
//   path.
//
//   bursty   — small number of "abusive" clients sending way above their
//   quota to deliberately trigger 429s and exercise the rejection +
//   Kafka-publish path that the autoscaler-monitor consumes.
//
// Pass parameters via env, e.g.:
//   k6 run -e TARGET_RPS=20000 -e DURATION=60s -e CLIENTS=2000 load.js
//
// Output is captured as a JSON summary by the runner script and parsed
// into the markdown report under benchmarks/.

import http from 'k6/http';
import { check } from 'k6';

const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '10000', 10);
const DURATION  = __ENV.DURATION || '30s';
const CLIENTS   = parseInt(__ENV.CLIENTS || '1000', 10);
const ABUSERS   = parseInt(__ENV.ABUSERS || '5', 10);
const BASE      = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    sustained: {
      executor: 'constant-arrival-rate',
      rate: TARGET_RPS,
      timeUnit: '1s',
      duration: DURATION,
      // VU sizing matters: max in-flight = VUs, so VUs must comfortably
      // exceed RPS × p99-latency. We allocate aggressively to avoid the
      // load harness becoming the bottleneck.
      preAllocatedVUs: Math.max(500, Math.ceil(TARGET_RPS / 5)),
      maxVUs:          Math.max(2000, TARGET_RPS),
      exec: 'sustained',
    },
    bursty: {
      executor: 'constant-arrival-rate',
      rate: 4000,                  // intentional 429 firehose
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 400,
      maxVUs: 4000,
      exec: 'bursty',
    },
  },
  thresholds: {
    'http_req_duration{scenario:sustained,expected_response:true}': ['p(99)<200'],
  },
};

export function sustained() {
  const id = `cust-${Math.floor(Math.random() * CLIENTS)}`;
  const res = http.get(`${BASE}/process`, {
    headers: { 'X-Client-Id': id },
    tags: { scenario: 'sustained' },
  });
  check(res, { 'sustained ok or 429': (r) => r.status === 200 || r.status === 429 });
}

export function bursty() {
  const id = `abuser-${Math.floor(Math.random() * ABUSERS)}`;
  const res = http.get(`${BASE}/process`, {
    headers: { 'X-Client-Id': id },
    tags: { scenario: 'bursty' },
  });
  check(res, { 'bursty 200 or 429': (r) => r.status === 200 || r.status === 429 });
}
