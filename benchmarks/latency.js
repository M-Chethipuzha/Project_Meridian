import { check } from 'k6';
import http from 'k6/http';

export const options = {
  scenarios: { latency: { executor: 'constant-arrival-rate', rate: 100, timeUnit: '1s', duration: '60s', preAllocatedVUs: 10 } },
};

export default function () {
  let res = http.get('http://localhost:8081/healthz');
  check(res, { 'status 200': (r) => r.status === 200 });
}
