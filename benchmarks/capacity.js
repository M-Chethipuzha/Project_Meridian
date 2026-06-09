import { check } from 'k6';
import http from 'k6/http';

export const options = {
  scenarios: { capacity: { executor: 'ramping-vus', startVUs: 0, stages: [{ target: 200, duration: '2m' }] } },
};

export default function () { http.get('http://localhost:8082/healthz'); }
