// Quick burst scenario used to measure rejection → autoscaler-alert latency.
// All traffic uses a tiny pool of "abusive" client ids that exceed their
// per-client quota immediately, so >90% of responses are 429s.
//
// Stdout includes a single line "RUN_STARTED_MS=<epoch_ms>" right before the
// scenario starts; the runner script captures that and compares against the
// autoscaler container log timestamp.

import http from 'k6/http';

const ABUSERS = parseInt(__ENV.ABUSERS || '4', 10);
const BASE    = __ENV.BASE_URL || 'http://gateway:8080';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 3000,
      timeUnit: '1s',
      duration: '5s',
      preAllocatedVUs: 400,
      maxVUs: 1500,
      exec: 'burst',
    },
  },
};

export function setup() {
  console.log(`RUN_STARTED_MS=${Date.now()}`);
}

export function burst() {
  const id = `abuser-${Math.floor(Math.random() * ABUSERS)}`;
  http.get(`${BASE}/process`, { headers: { 'X-Client-Id': id } });
}
