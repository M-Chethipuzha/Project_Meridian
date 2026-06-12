import { check } from 'k6';
import http from 'k6/http';

export const options = {
  scenarios: {
    throughput: { executor: 'ramping-arrival-rate', startRate: 100, timeUnit: '1s', preAllocatedVUs: 10, maxVUs: 100, stages: [{ target: 1000, duration: '30s' }] },
  },
};

export default function () { http.get('http://localhost:8081/healthz'); }
