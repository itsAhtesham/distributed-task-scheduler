import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const taskCompletionRate = new Rate('task_completion_rate');

export const options = {
  stages: [
    { duration: '30s', target: 20 },   // Ramp up
    { duration: '1m', target: 50 },     // Sustained load
    { duration: '30s', target: 100 },   // Peak load
    { duration: '30s', target: 0 },     // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'],
    task_completion_rate: ['rate>0.995'],  // 99.5% target
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const API_KEY = __ENV.API_KEY || 'test-api-key-12345';
const headers = { 'Content-Type': 'application/json', 'X-API-Key': API_KEY };

export default function () {
  // Create a one-time job
  const createRes = http.post(`${BASE_URL}/api/v1/jobs`, JSON.stringify({
    name: `load-test-job-${Date.now()}`,
    type: 'once',
    handler: 'http-request',
    payload: { url: 'https://httpbin.org/post', options: { method: 'POST' } },
    max_retries: 2,
  }), { headers });

  check(createRes, { 'job created': (r) => r.status === 201 });

  if (createRes.status === 201) {
    const jobId = JSON.parse(createRes.body).data.id;

    // Wait and check completion
    sleep(5);

    const statusRes = http.get(`${BASE_URL}/api/v1/jobs/${jobId}`, { headers });
    const job = JSON.parse(statusRes.body).data;

    taskCompletionRate.add(job.status === 'completed');
  }

  sleep(1);
}
