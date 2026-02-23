# PRD: Distributed Task Scheduler

## 1. Overview

Build a **production-grade distributed task scheduling system** in Node.js/TypeScript that supports cron-based and asynchronous job execution with fault tolerance, guaranteed delivery, and horizontal scaling across multiple worker nodes.

**Tech Stack:** Node.js, TypeScript, Redis (Streams + Distributed Locks), PostgreSQL, Docker, Express.js

**GitHub Repo Name:** `distributed-task-scheduler`

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT / USER                         │
│              (REST API calls + Dashboard UI)                 │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      API SERVER (Express)                     │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌────────────┐  │
│  │  Jobs    │  │ Rate     │  │ Webhook   │  │ Auth       │  │
│  │  CRUD    │  │ Limiter  │  │ Manager   │  │ Middleware │  │
│  └─────────┘  └──────────┘  └───────────┘  └────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┼────────────────┐
              ▼            ▼                ▼
┌──────────────────┐ ┌──────────┐ ┌─────────────────────┐
│   PostgreSQL     │ │  Redis   │ │   Redis Streams      │
│  ┌────────────┐  │ │          │ │                       │
│  │ jobs       │  │ │ - Locks  │ │ - task_queue (main)   │
│  │ job_runs   │  │ │ - Cache  │ │ - task_dlq (dead      │
│  │ webhooks   │  │ │ - Rate   │ │   letter queue)       │
│  └────────────┘  │ │   limits │ │                       │
└──────────────────┘ └──────────┘ └───────────┬───────────┘
                                               │
                    ┌──────────────────────────┤
                    ▼              ▼            ▼
            ┌─────────────┐┌─────────────┐┌─────────────┐
            │  WORKER 1   ││  WORKER 2   ││  WORKER 3   │
            │  (Leader)   ││  (Follower) ││  (Follower) │
            │             ││             ││             │
            │ - Cron      ││ - Execute   ││ - Execute   │
            │   Scheduler ││   tasks     ││   tasks     │
            │ - Execute   ││ - Heartbeat ││ - Heartbeat │
            │   tasks     ││             ││             │
            │ - Heartbeat ││             ││             │
            └─────────────┘└─────────────┘└─────────────┘
```

---

## 3. Project Structure

```
distributed-task-scheduler/
├── docker-compose.yml          # PostgreSQL + Redis + 3 workers + API
├── Dockerfile                  # Multi-stage build
├── package.json
├── tsconfig.json
├── .env.example
├── k6/
│   └── load-test.js            # k6 load testing script
├── src/
│   ├── index.ts                # API server entry point
│   ├── worker.ts               # Worker process entry point
│   ├── config/
│   │   ├── database.ts         # PostgreSQL connection (pg pool)
│   │   ├── redis.ts            # Redis client setup
│   │   └── env.ts              # Environment variable validation (Zod)
│   ├── api/
│   │   ├── routes/
│   │   │   ├── jobs.routes.ts       # Job CRUD + execution routes
│   │   │   ├── webhooks.routes.ts   # Webhook registration routes
│   │   │   └── health.routes.ts     # Health check endpoint
│   │   ├── middlewares/
│   │   │   ├── rateLimiter.ts       # Redis-based rate limiting
│   │   │   ├── pagination.ts        # Cursor-based pagination
│   │   │   ├── auth.ts              # API key authentication
│   │   │   ├── validate.ts          # Zod request validation
│   │   │   └── errorHandler.ts      # Global error handler
│   │   └── controllers/
│   │       ├── jobs.controller.ts
│   │       └── webhooks.controller.ts
│   ├── services/
│   │   ├── jobService.ts            # Job business logic
│   │   ├── schedulerService.ts      # Cron scheduling logic
│   │   ├── queueService.ts          # Redis Streams producer
│   │   ├── webhookService.ts        # Webhook dispatch + retry
│   │   └── leaderElection.ts        # Redis distributed lock leader election
│   ├── workers/
│   │   ├── taskConsumer.ts          # Redis Streams consumer (job executor)
│   │   ├── dlqProcessor.ts         # Dead letter queue processor
│   │   └── heartbeat.ts            # Worker heartbeat & health
│   ├── db/
│   │   ├── migrations/
│   │   │   └── 001_initial.sql      # Create tables
│   │   └── queries/
│   │       ├── jobs.queries.ts      # Raw SQL queries for jobs
│   │       └── webhooks.queries.ts
│   ├── types/
│   │   ├── job.types.ts
│   │   ├── webhook.types.ts
│   │   └── common.types.ts
│   └── utils/
│       ├── logger.ts                # Structured logging (pino)
│       ├── cronParser.ts            # Cron expression validator
│       └── idGenerator.ts           # ULID/nanoid generator
└── README.md
```

---

## 4. Database Schema (PostgreSQL)

### 4.1 `jobs` table

```sql
CREATE TABLE jobs (
    id              VARCHAR(26) PRIMARY KEY,        -- ULID
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    type            VARCHAR(20) NOT NULL CHECK (type IN ('cron', 'once', 'delayed')),
    cron_expression VARCHAR(100),                    -- Only for type='cron'
    scheduled_at    TIMESTAMPTZ,                     -- Only for type='once' or 'delayed'
    payload         JSONB NOT NULL DEFAULT '{}',     -- Arbitrary JSON data sent to handler
    handler         VARCHAR(255) NOT NULL,           -- Handler function identifier
    status          VARCHAR(20) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'paused', 'completed', 'failed', 'cancelled')),
    max_retries     INT NOT NULL DEFAULT 3,
    retry_count     INT NOT NULL DEFAULT 0,
    timeout_ms      INT NOT NULL DEFAULT 30000,      -- 30s default timeout
    priority        INT NOT NULL DEFAULT 0,           -- Higher = more priority
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    next_run_at     TIMESTAMPTZ,
    last_run_at     TIMESTAMPTZ
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_next_run ON jobs(next_run_at) WHERE status = 'active';
CREATE INDEX idx_jobs_type ON jobs(type);
```

### 4.2 `job_runs` table

```sql
CREATE TABLE job_runs (
    id              VARCHAR(26) PRIMARY KEY,
    job_id          VARCHAR(26) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id       VARCHAR(50) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'timed_out', 'retrying')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    duration_ms     INT,
    attempt         INT NOT NULL DEFAULT 1,
    error_message   TEXT,
    result          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_runs_job_id ON job_runs(job_id);
CREATE INDEX idx_job_runs_status ON job_runs(status);
CREATE INDEX idx_job_runs_worker ON job_runs(worker_id);
```

### 4.3 `webhooks` table

```sql
CREATE TABLE webhooks (
    id              VARCHAR(26) PRIMARY KEY,
    job_id          VARCHAR(26) REFERENCES jobs(id) ON DELETE CASCADE,
    url             VARCHAR(2048) NOT NULL,
    events          VARCHAR(50)[] NOT NULL DEFAULT '{}',  -- ['completed', 'failed', 'retrying']
    secret          VARCHAR(255),                          -- For HMAC signature verification
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 5. Component Specifications

### 5.1 API Server (`src/index.ts`)

The main Express.js HTTP server that exposes the REST API.

**Responsibilities:**
- Serve REST endpoints for job management
- Apply rate limiting, authentication, pagination
- Validate all incoming requests with Zod schemas
- Return consistent JSON responses with proper HTTP status codes

**Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/jobs` | Create a new job |
| `GET` | `/api/v1/jobs` | List jobs (paginated, filterable) |
| `GET` | `/api/v1/jobs/:id` | Get job details |
| `PATCH` | `/api/v1/jobs/:id` | Update a job (pause/resume/modify) |
| `DELETE` | `/api/v1/jobs/:id` | Cancel and delete a job |
| `POST` | `/api/v1/jobs/:id/trigger` | Manually trigger a job immediately |
| `GET` | `/api/v1/jobs/:id/runs` | Get execution history for a job |
| `POST` | `/api/v1/webhooks` | Register a webhook |
| `GET` | `/api/v1/webhooks` | List webhooks |
| `DELETE` | `/api/v1/webhooks/:id` | Remove a webhook |
| `GET` | `/api/v1/health` | Health check (API + Redis + PostgreSQL) |

---

### 5.2 Rate Limiter (`src/api/middlewares/rateLimiter.ts`)

**What it does:** Limits API requests per client using a Redis-backed sliding window algorithm.

**How it works:**
1. Extract client identifier from API key or IP address
2. Use Redis `MULTI/EXEC` to atomically increment a counter key with TTL
3. Return `429 Too Many Requests` with `Retry-After` header when limit exceeded
4. Use sliding window: key pattern `ratelimit:{client_id}:{window_timestamp}`

**Config:**
- Default: 100 requests per minute per client
- Configurable per-route overrides

---

### 5.3 Pagination (`src/api/middlewares/pagination.ts`)

**What it does:** Provides cursor-based pagination for list endpoints.

**How it works:**
1. Accept `?cursor=<ulid>&limit=<n>` query params
2. Use ULID as cursor (naturally sortable by time)
3. Query: `WHERE id > $cursor ORDER BY id ASC LIMIT $limit + 1`
4. If results > limit, pop last item and use its ID as `next_cursor`
5. Return response shape:
```json
{
  "data": [...],
  "pagination": {
    "next_cursor": "01HX...",
    "has_more": true,
    "limit": 20
  }
}
```

---

### 5.4 Job Service (`src/services/jobService.ts`)

**What it does:** Core business logic for job CRUD operations.

**Responsibilities:**
- Validate cron expressions using `cron-parser` library
- Calculate `next_run_at` for cron jobs
- Insert jobs into PostgreSQL
- Push immediate/delayed jobs to Redis Streams via `queueService`
- Update job status on completion/failure

**Key methods:**
```typescript
createJob(input: CreateJobInput): Promise<Job>
getJob(id: string): Promise<Job>
listJobs(filters: JobFilters, pagination: PaginationParams): Promise<PaginatedResult<Job>>
updateJob(id: string, updates: UpdateJobInput): Promise<Job>
deleteJob(id: string): Promise<void>
triggerJob(id: string): Promise<JobRun>
getJobRuns(jobId: string, pagination: PaginationParams): Promise<PaginatedResult<JobRun>>
```

---

### 5.5 Scheduler Service (`src/services/schedulerService.ts`)

**What it does:** The cron scheduling engine. Runs ONLY on the leader node.

**How it works:**
1. Every 10 seconds, query PostgreSQL for jobs where `type = 'cron'` AND `status = 'active'` AND `next_run_at <= NOW()`
2. For each due job:
   - Push a task message to Redis Streams (`task_queue`)
   - Update `next_run_at` based on cron expression
   - Update `last_run_at` to NOW()
3. Uses a PostgreSQL advisory lock per job to prevent double-scheduling in race conditions

**Why only on leader:** If all workers scheduled cron jobs, every job would execute N times (once per worker). Leader election ensures exactly-once scheduling.

---

### 5.6 Queue Service (`src/services/queueService.ts`)

**What it does:** Produces messages to Redis Streams.

**How it works:**
1. Use `XADD task_queue * job_id <id> payload <json> attempt <n> priority <n>`
2. Messages are persistent in Redis Streams until acknowledged
3. Each message contains:
```json
{
  "job_id": "01HX...",
  "payload": "{...}",
  "attempt": 1,
  "priority": 0,
  "enqueued_at": "2024-06-15T10:30:00Z"
}
```

**Dead Letter Queue:**
- If a message fails after max_retries, move it to `task_dlq` stream
- DLQ messages can be inspected and replayed via API

---

### 5.7 Leader Election (`src/services/leaderElection.ts`)

**What it does:** Ensures only ONE worker node runs the cron scheduler at any time.

**How it works:**
1. Worker tries to acquire Redis lock: `SET leader_lock <worker_id> NX PX 15000`
   - `NX` = only set if key doesn't exist
   - `PX 15000` = auto-expire in 15 seconds
2. If acquired → this worker is the leader
3. Leader refreshes lock every 5 seconds (before 15s expiry)
4. If leader crashes → lock expires in 15s → another worker acquires it
5. Use Redlock pattern for correctness:
   - Read lock value before releasing to prevent deleting another leader's lock
   - Use Lua script for atomic check-and-delete

**Failover guarantee:** New leader elected within 15 seconds of leader crash.

**Key methods:**
```typescript
tryAcquireLeadership(): Promise<boolean>
renewLeadership(): Promise<boolean>
releaseLeadership(): Promise<void>
isLeader(): boolean
```

---

### 5.8 Task Consumer (`src/workers/taskConsumer.ts`)

**What it does:** Each worker runs a consumer that pulls tasks from Redis Streams and executes them.

**How it works:**
1. Create a consumer group on `task_queue`: `XGROUP CREATE task_queue workers $ MKSTREAM`
2. Each worker reads with: `XREADGROUP GROUP workers <worker_id> COUNT 5 BLOCK 2000 STREAMS task_queue >`
3. For each message:
   a. Create a `job_run` record in PostgreSQL (status: running)
   b. Execute the job handler with a timeout wrapper (`Promise.race` with setTimeout)
   c. On success:
      - `XACK task_queue workers <message_id>` (acknowledge)
      - Update job_run status to `completed`
      - Trigger webhook callbacks for `completed` event
   d. On failure:
      - If `attempt < max_retries`:
        - Re-enqueue to `task_queue` with incremented attempt
        - Update job_run status to `retrying`
        - Trigger webhook for `retrying` event
      - If `attempt >= max_retries`:
        - Move to `task_dlq` (dead letter queue)
        - Update job_run and job status to `failed`
        - Trigger webhook for `failed` event
      - `XACK` the original message in both cases

**Job handlers:** A registry of named functions:
```typescript
const handlers: Record<string, (payload: any) => Promise<any>> = {
  'send-email': async (payload) => { /* ... */ },
  'process-image': async (payload) => { /* ... */ },
  'generate-report': async (payload) => { /* ... */ },
  'http-request': async (payload) => {
    // Generic HTTP handler - makes a request to payload.url
    const res = await fetch(payload.url, payload.options);
    return { status: res.status, body: await res.json() };
  },
};
```

---

### 5.9 DLQ Processor (`src/workers/dlqProcessor.ts`)

**What it does:** Processes failed tasks that ended up in the dead letter queue.

**How it works:**
1. Periodically reads from `task_dlq` stream
2. Logs failed tasks with full context (job_id, error, attempts)
3. Exposes API endpoints to:
   - List DLQ messages
   - Replay a specific message (re-enqueue to `task_queue`)
   - Purge old DLQ entries

---

### 5.10 Worker Heartbeat (`src/workers/heartbeat.ts`)

**What it does:** Each worker sends periodic heartbeats so the system knows which workers are alive.

**How it works:**
1. Every 5 seconds, set Redis key: `SET worker:{worker_id}:heartbeat <timestamp> PX 15000`
2. The API health endpoint reads all `worker:*:heartbeat` keys to report cluster status
3. If a worker stops heartbeating, its key expires and it's considered dead

---

### 5.11 Webhook Service (`src/services/webhookService.ts`)

**What it does:** Sends HTTP POST callbacks to registered webhook URLs when job events occur.

**How it works:**
1. On job event (completed/failed/retrying), look up webhooks for that job_id
2. For each matching webhook:
   - Build payload: `{ event, job_id, job_run_id, timestamp, data }`
   - Sign with HMAC-SHA256 using webhook secret → add `X-Signature-256` header
   - Send HTTP POST with 5-second timeout
   - Retry up to 3 times with exponential backoff (1s, 2s, 4s) on failure
3. Fire-and-forget (don't block job execution)

---

### 5.12 Worker Entry Point (`src/worker.ts`)

**What it does:** The main process for each worker node.

**Startup sequence:**
1. Generate unique `worker_id` using hostname + random suffix
2. Connect to Redis and PostgreSQL
3. Start heartbeat loop
4. Start leader election loop
5. If leader → start scheduler service
6. Start task consumer (all workers consume tasks)
7. Start DLQ processor (leader only)
8. Graceful shutdown: release leadership, stop consuming, drain in-progress tasks

---

## 6. Docker Compose Setup

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: task_scheduler
      POSTGRES_USER: scheduler
      POSTGRES_PASSWORD: scheduler_pass
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes

  api:
    build: .
    command: npm run start:api
    ports:
      - "3000:3000"
    environment:
      DATABASE_URL: postgresql://scheduler:scheduler_pass@postgres:5432/task_scheduler
      REDIS_URL: redis://redis:6379
      PORT: 3000
      API_KEY: test-api-key-12345
    depends_on:
      - postgres
      - redis

  worker-1:
    build: .
    command: npm run start:worker
    environment:
      DATABASE_URL: postgresql://scheduler:scheduler_pass@postgres:5432/task_scheduler
      REDIS_URL: redis://redis:6379
      WORKER_ID: worker-1
    depends_on:
      - postgres
      - redis

  worker-2:
    build: .
    command: npm run start:worker
    environment:
      DATABASE_URL: postgresql://scheduler:scheduler_pass@postgres:5432/task_scheduler
      REDIS_URL: redis://redis:6379
      WORKER_ID: worker-2
    depends_on:
      - postgres
      - redis

  worker-3:
    build: .
    command: npm run start:worker
    environment:
      DATABASE_URL: postgresql://scheduler:scheduler_pass@postgres:5432/task_scheduler
      REDIS_URL: redis://redis:6379
      WORKER_ID: worker-3
    depends_on:
      - postgres
      - redis

volumes:
  pgdata:
```

---

## 7. k6 Load Test Script

Create `k6/load-test.js`:

```javascript
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
```

---

## 8. Key NPM Dependencies

```json
{
  "dependencies": {
    "express": "^4.18.2",
    "pg": "^8.12.0",
    "ioredis": "^5.3.2",
    "cron-parser": "^4.9.0",
    "zod": "^3.23.0",
    "pino": "^9.0.0",
    "pino-pretty": "^11.0.0",
    "nanoid": "^3.3.7",
    "ulid": "^2.3.0",
    "node-cron": "^3.0.3",
    "helmet": "^7.1.0",
    "cors": "^2.8.5"
  },
  "devDependencies": {
    "typescript": "^5.4.0",
    "@types/express": "^4.17.21",
    "@types/pg": "^8.11.0",
    "@types/node": "^20.12.0",
    "tsx": "^4.7.0",
    "nodemon": "^3.1.0"
  }
}
```

---

## 9. Environment Variables

```env
# Database
DATABASE_URL=postgresql://scheduler:scheduler_pass@localhost:5432/task_scheduler

# Redis
REDIS_URL=redis://localhost:6379

# API
PORT=3000
API_KEY=your-secret-api-key
RATE_LIMIT_MAX=100
RATE_LIMIT_WINDOW_MS=60000

# Worker
WORKER_ID=worker-1
LEADER_LOCK_TTL_MS=15000
LEADER_RENEW_INTERVAL_MS=5000
HEARTBEAT_INTERVAL_MS=5000
SCHEDULER_POLL_INTERVAL_MS=10000

# Webhook
WEBHOOK_TIMEOUT_MS=5000
WEBHOOK_MAX_RETRIES=3
```

---

## 10. API Response Format

All endpoints follow this consistent format:

**Success:**
```json
{
  "success": true,
  "data": { ... },
  "pagination": { "next_cursor": "...", "has_more": true, "limit": 20 }
}
```

**Error:**
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid cron expression",
    "details": [{ "field": "cron_expression", "message": "..." }]
  }
}
```

---

## 11. README Template

The README.md should include:
1. Project title with a one-line description
2. Architecture diagram (paste the ASCII diagram from section 2)
3. Features list (bullet points)
4. Quick start with `docker-compose up`
5. API documentation with curl examples for each endpoint
6. Load testing instructions with k6
7. Configuration section listing all env vars
8. How leader election works (brief explanation)
9. License: MIT

---

## 12. Build Instructions for Claude Code

Run these commands in order:

```bash
# 1. Initialize the project
mkdir distributed-task-scheduler && cd distributed-task-scheduler
npm init -y
npm install express pg ioredis cron-parser zod pino pino-pretty nanoid ulid node-cron helmet cors
npm install -D typescript @types/express @types/pg @types/node tsx nodemon

# 2. Create tsconfig.json with strict mode, ES2022 target, NodeNext module

# 3. Create the full folder structure as specified in Section 3

# 4. Build components in this order:
#    a. Config files (database.ts, redis.ts, env.ts)
#    b. Types (job.types.ts, webhook.types.ts, common.types.ts)
#    c. Database migrations (001_initial.sql) + run migration on startup
#    d. Utility files (logger.ts, cronParser.ts, idGenerator.ts)
#    e. Queue service (Redis Streams producer)
#    f. Leader election service
#    g. Job service (CRUD logic)
#    h. Webhook service
#    i. Scheduler service (cron polling)
#    j. Task consumer (Redis Streams consumer)
#    k. DLQ processor
#    l. Heartbeat
#    m. API middlewares (rateLimiter, pagination, auth, validate, errorHandler)
#    n. API routes and controllers
#    o. API server entry point (index.ts)
#    p. Worker entry point (worker.ts)
#    q. Docker setup (Dockerfile + docker-compose.yml)
#    r. k6 load test script
#    s. README.md

# 5. Add scripts to package.json:
#    "start:api": "tsx src/index.ts"
#    "start:worker": "tsx src/worker.ts"
#    "dev:api": "nodemon --exec tsx src/index.ts"
#    "dev:worker": "nodemon --exec tsx src/worker.ts"
#    "migrate": "tsx src/db/migrate.ts"
#    "test:load": "k6 run k6/load-test.js"

# 6. Test by running: docker-compose up --build
# 7. Verify all 3 workers start, one becomes leader
# 8. Create a job via curl and verify execution
```

---

## 13. Success Criteria

- [ ] API server starts and all endpoints respond correctly
- [ ] 3 worker nodes start via docker-compose
- [ ] Exactly one worker becomes leader at any time
- [ ] Leader failover happens within 15 seconds when leader container is stopped
- [ ] Cron jobs execute on schedule without duplicates
- [ ] One-time jobs execute exactly once
- [ ] Failed jobs retry up to max_retries then move to DLQ
- [ ] Webhooks fire on job completion/failure
- [ ] Rate limiter returns 429 when exceeded
- [ ] Pagination works correctly with cursor
- [ ] k6 load test achieves 99.5%+ task completion rate
- [ ] No duplicate job execution across workers