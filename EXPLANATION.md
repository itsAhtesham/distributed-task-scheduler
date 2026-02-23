# Distributed Task Scheduler — Deep Dive Explanation

## Table of Contents

1. [What I Built & Why](#1-what-i-built--why)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Request Lifecycle — End to End](#3-request-lifecycle--end-to-end)
4. [Config Layer](#4-config-layer)
5. [Type System & Validation](#5-type-system--validation)
6. [Database Layer](#6-database-layer)
7. [Queue System (Redis Streams)](#7-queue-system-redis-streams)
8. [Leader Election](#8-leader-election)
9. [Scheduler Service](#9-scheduler-service)
10. [Task Consumer (Worker Engine)](#10-task-consumer-worker-engine)
11. [Dead Letter Queue (DLQ)](#11-dead-letter-queue-dlq)
12. [Webhook Service](#12-webhook-service)
13. [Worker Heartbeat](#13-worker-heartbeat)
14. [API Middleware Pipeline](#14-api-middleware-pipeline)
15. [API Routes & Controllers](#15-api-routes--controllers)
16. [Entry Points (API Server & Worker)](#16-entry-points-api-server--worker)
17. [Docker & Deployment](#17-docker--deployment)
18. [Key Design Decisions & Trade-offs](#18-key-design-decisions--trade-offs)
19. [Failure Scenarios & How the System Handles Them](#19-failure-scenarios--how-the-system-handles-them)
20. [Load Testing with k6](#20-load-testing-with-k6)
21. [How to Explain This in an Interview](#21-how-to-explain-this-in-an-interview)

---

## 1. What I Built & Why

I built a **production-grade distributed task scheduling system** — think of it as a simplified version of what powers systems like Sidekiq, Bull, or AWS Step Functions.

**The problem it solves:** In any non-trivial backend, you need to run work outside the request-response cycle — sending emails, generating reports, processing images, calling third-party APIs. You also need recurring jobs (cron). Doing this on a single server is fragile: if the server dies, all pending work is lost.

**My solution:** A system where:
- An **API server** accepts job definitions (one-time, delayed, or cron-based)
- Jobs are persisted in **PostgreSQL** (durable storage) and dispatched through **Redis Streams** (fast message queue)
- Multiple **worker nodes** consume and execute jobs in parallel
- Exactly **one worker** is elected as leader to run the cron scheduler (prevents duplicate execution)
- Failed jobs **retry with backoff** and eventually land in a **dead letter queue**
- External systems get notified via **webhooks** with HMAC signature verification

**Tech stack:** Node.js, TypeScript, Express.js, PostgreSQL, Redis (Streams + Distributed Locks), Docker.

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────┐
│                    CLIENT / USER                      │
│             (REST API calls via curl/UI)              │
└────────────────────────┬─────────────────────────────┘
                         │  HTTP
                         ▼
┌──────────────────────────────────────────────────────┐
│                 API SERVER (Express)                   │
│                                                        │
│  Request Flow:                                         │
│  helmet → cors → json parse → auth → rate limiter     │
│  → route handler → validation → controller → service  │
└────────────┬──────────────┬──────────────┬───────────┘
             │              │              │
             ▼              ▼              ▼
      ┌────────────┐ ┌──────────┐ ┌──────────────────┐
      │ PostgreSQL │ │  Redis   │ │  Redis Streams    │
      │            │ │          │ │                    │
      │ • jobs     │ │ • Locks  │ │ • task_queue      │
      │ • job_runs │ │ • Rate   │ │   (main queue)    │
      │ • webhooks │ │   limits │ │ • task_dlq        │
      │            │ │ • Heart- │ │   (dead letter)   │
      │            │ │   beats  │ │                    │
      └────────────┘ └──────────┘ └────────┬───────────┘
                                            │
                     ┌──────────────────────┼──────────┐
                     ▼                      ▼          ▼
              ┌────────────┐        ┌────────────┐ ┌────────────┐
              │  WORKER 1  │        │  WORKER 2  │ │  WORKER 3  │
              │  (Leader)  │        │ (Follower) │ │ (Follower) │
              │            │        │            │ │            │
              │ ✓ Cron     │        │ ✗ Cron     │ │ ✗ Cron     │
              │   Scheduler│        │            │ │            │
              │ ✓ Task     │        │ ✓ Task     │ │ ✓ Task     │
              │   Consumer │        │   Consumer │ │   Consumer │
              │ ✓ DLQ Proc │        │ ✗ DLQ Proc │ │ ✗ DLQ Proc │
              │ ✓ Heartbeat│        │ ✓ Heartbeat│ │ ✓ Heartbeat│
              └────────────┘        └────────────┘ └────────────┘
```

**Why two data stores?**
- **PostgreSQL** is the source of truth — durable, ACID-compliant, supports complex queries for job listing/filtering/pagination.
- **Redis Streams** is the message broker — fast, supports consumer groups for load balancing across workers, provides at-least-once delivery guarantees.

This separation means if Redis goes down, no data is lost (jobs are in PostgreSQL). When Redis recovers, the scheduler simply picks up where it left off.

---

## 3. Request Lifecycle — End to End

Let me walk through what happens when a user creates a cron job. This is the kind of end-to-end walkthrough interviewers love.

### Step 1: API receives the request
```
POST /api/v1/jobs
{
  "name": "daily-report",
  "type": "cron",
  "cron_expression": "0 9 * * *",
  "handler": "generate-report",
  "payload": { "type": "daily-summary" }
}
```

### Step 2: Middleware pipeline processes it
1. **helmet** — Sets security headers (X-Content-Type-Options, X-Frame-Options, etc.)
2. **cors** — Allows cross-origin requests
3. **express.json()** — Parses JSON body
4. **authenticate** — Checks `X-API-Key` header against the configured API key
5. **rateLimiter** — Increments a Redis counter for this client; rejects with 429 if over limit
6. **validate** — Runs the request body through the `CreateJobSchema` Zod schema
7. **pagination** — (Only for list endpoints) Extracts cursor/limit from query params

### Step 3: Controller calls service
The `jobs.controller.ts` calls `jobService.createJob(req.body)`.

### Step 4: Service processes business logic
Inside `jobService.createJob()`:
1. **Validates the cron expression** using `cron-parser` — rejects if invalid
2. **Generates a ULID** as the job ID (time-sortable, unique)
3. **Calculates `next_run_at`** — parses `"0 9 * * *"` to find the next 9:00 AM
4. **Inserts into PostgreSQL** — the job is now durably stored
5. For cron jobs, does NOT enqueue immediately (the scheduler handles that). For `once` type, enqueues immediately to Redis Streams.

### Step 5: Response sent
```json
{
  "success": true,
  "data": {
    "id": "01HX3K...",
    "name": "daily-report",
    "type": "cron",
    "status": "active",
    "next_run_at": "2025-01-16T09:00:00.000Z",
    ...
  }
}
```

### Step 6: Scheduler picks it up (later)
The leader worker's scheduler polls PostgreSQL every 10 seconds. When `next_run_at <= NOW()`:
1. Acquires a PostgreSQL advisory lock for this job (prevents double-scheduling)
2. Pushes a task message to Redis Streams `task_queue`
3. Updates `next_run_at` to the next 9:00 AM
4. Updates `last_run_at` to now

### Step 7: A worker consumes and executes it
Any worker (including the leader) picks up the message from Redis Streams:
1. Creates a `job_run` record in PostgreSQL (status: running)
2. Looks up the handler function by name (`"generate-report"`)
3. Executes the handler with a timeout wrapper
4. On success: acknowledges the message, updates job_run to completed, fires webhooks
5. On failure: retries or moves to DLQ

---

## 4. Config Layer

### `src/config/env.ts` — Environment Variable Validation

**What it does:** Validates and types all environment variables at startup using Zod schemas.

**Why it matters:** Without this, you discover missing env vars at runtime when some code path tries to use `process.env.SOMETHING` and gets `undefined`. This crashes immediately on startup with a clear error message showing exactly which variables are missing or malformed.

**How it works:**
```typescript
const envSchema = z.object({
  DATABASE_URL: z.string().url(),           // Must be a valid URL
  REDIS_URL: z.string().url(),
  PORT: z.coerce.number().default(3000),    // Coerces string "3000" to number
  API_KEY: z.string().min(1),               // Must not be empty
  // ... more fields with defaults
});

export const env = loadEnv();  // Validated, typed object
```

The `z.coerce.number()` is important — environment variables are always strings, so `PORT=3000` comes in as the string `"3000"`. Coerce converts it to the number `3000`.

### `src/config/database.ts` — PostgreSQL Connection Pool

**What it does:** Creates and manages a connection pool to PostgreSQL.

**Key design choices:**
- **Connection pool** (not single connection) — a pool of 20 connections is shared across all requests. When code calls `pool.query()`, it automatically checks out a connection, runs the query, and returns the connection to the pool. This avoids the overhead of connecting/disconnecting for every query.
- **Health check function** — `checkDatabaseConnection()` tries a simple `SELECT 1` query. Used by the `/health` endpoint.
- **Error handler on pool** — catches unexpected errors on idle connections (e.g., PostgreSQL restarts) so they don't crash the process.

### `src/config/redis.ts` — Redis Client

**What it does:** Creates Redis client instances using ioredis.

**Key config:**
- `maxRetriesPerRequest: null` — Required for Redis Streams blocking reads. Without this, ioredis throws an error after a few retries when `XREADGROUP BLOCK` takes too long (which is by design — we want to block-wait for new messages).
- `retryStrategy` — Exponential backoff (200ms, 400ms, 600ms... up to 5s) when Redis connection drops. The client automatically reconnects.

---

## 5. Type System & Validation

### `src/types/job.types.ts`

This file defines both **runtime validation schemas** (Zod) and **TypeScript interfaces**.

**Zod schemas serve dual purpose:**
1. **Runtime validation** — The API uses `CreateJobSchema.parse(req.body)` to validate incoming requests. If the body doesn't match, Zod throws with detailed error messages (which field failed, why).
2. **Type inference** — `type CreateJobInput = z.infer<typeof CreateJobSchema>` generates the TypeScript type automatically from the schema. This means validation logic and types can never drift apart.

**The `.refine()` method** adds cross-field validation:
```typescript
.refine((data) => {
  if (data.type === 'cron' && !data.cron_expression) return false;
  // cron jobs MUST have a cron_expression
})
```
This is something you can't express with simple field-level validation — it depends on the combination of fields.

**Why ULID over UUID?** ULIDs are time-sortable. When used as database primary keys and pagination cursors, `WHERE id > $cursor ORDER BY id ASC` gives you natural chronological ordering without needing a separate timestamp column in the query. UUIDs are random, so they scatter across B-tree indexes and make cursor pagination inefficient.

### `src/types/common.types.ts`

Defines shared types used across the system:
- **`PaginationParams`** — `{ cursor?: string; limit: number }` — extracted from query params by the pagination middleware
- **`PaginatedResult<T>`** — Generic wrapper for paginated responses
- **`ApiResponse<T>` / `ApiError`** — Consistent response shapes across all endpoints
- **`TaskMessage`** — The shape of messages flowing through Redis Streams

---

## 6. Database Layer

### `src/db/migrations/001_initial.sql`

**Three tables:**

**`jobs`** — The core table. Stores job definitions.
- `id` (VARCHAR(26)) — ULID primary key
- `type` — Enum constrained by CHECK: `'cron'`, `'once'`, `'delayed'`
- `status` — State machine: `active → paused → active` (toggle), or `active → completed/failed/cancelled` (terminal)
- `cron_expression` — Only populated for cron jobs
- `scheduled_at` — Only populated for once/delayed jobs
- `payload` (JSONB) — Arbitrary data passed to the handler. JSONB is indexed and queryable in PostgreSQL.
- `next_run_at` — When the job should next execute. The scheduler queries on this.
- `priority` — Higher value = picked up sooner (used in ORDER BY)

**Important indexes:**
```sql
CREATE INDEX idx_jobs_next_run ON jobs(next_run_at) WHERE status = 'active';
```
This is a **partial index** — it only indexes rows where status is 'active'. This is much smaller and faster than indexing all rows, because the scheduler only ever queries active jobs.

**`job_runs`** — Execution history. Every time a job executes, a row is created here.
- Links to `jobs` via `job_id` with `ON DELETE CASCADE` (deleting a job deletes its history)
- `duration_ms` — Calculated as `EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000` on completion
- `attempt` — Which retry attempt this was (1 = first try)

**`webhooks`** — Webhook registrations.
- `job_id` is nullable — a webhook with `job_id = NULL` listens to events from ALL jobs
- `events` is a PostgreSQL array type — `VARCHAR(50)[]` — stores which events to fire on

### `src/db/queries/jobs.queries.ts`

**Raw SQL over ORM — why?**

I deliberately chose raw SQL with parameterized queries instead of an ORM like Prisma or TypeORM. Reasons:
1. **Full control over queries** — The scheduler needs a complex query with partial indexes, advisory locks, and specific ordering. ORMs make simple things easy but complex things harder.
2. **No abstraction leak** — I know exactly what SQL is executing. No surprising N+1 queries or inefficient joins.
3. **Parameterized queries prevent SQL injection** — Every user input goes through `$1`, `$2` placeholders, never string concatenation.

**Dynamic filter building in `listJobs()`:**
```typescript
if (filters.status) {
  conditions.push(`status = $${paramIndex++}`);
  params.push(filters.status);
}
```
This builds a WHERE clause dynamically based on which filters are present. The parameter index is tracked manually to ensure `$1`, `$2`, etc. are assigned correctly. This pattern is common in hand-written SQL.

### `src/db/migrate.ts`

**Simple file-based migrations.** On startup, it reads all `.sql` files from the migrations directory in sorted order and executes them. The `CREATE TABLE IF NOT EXISTS` ensures idempotency — running migrations multiple times is safe.

In production, you'd typically use a migration tool (like `node-pg-migrate` or Flyway) that tracks which migrations have been applied. I kept it simple here because the project has a single migration.

---

## 7. Queue System (Redis Streams)

### `src/services/queueService.ts`

**Why Redis Streams over a simpler Redis list (LPUSH/BRPOP)?**

Redis Streams provide critical features that lists don't:
1. **Consumer groups** — Multiple workers can read from the same stream, and Redis guarantees each message is delivered to exactly ONE consumer in the group. With lists, you'd need to implement this yourself.
2. **Acknowledgment** — A message stays "pending" until the consumer explicitly ACKs it. If a consumer crashes before ACKing, the message can be re-delivered to another consumer. With lists, a BRPOP removes the message permanently.
3. **Message persistence** — Messages stay in the stream even after being read (until trimmed). You can replay, inspect, or audit them.

**Stream initialization:**
```typescript
await redis.xgroup('CREATE', TASK_QUEUE, CONSUMER_GROUP, '$', 'MKSTREAM');
```
- `CONSUMER_GROUP = 'workers'` — All workers join this group
- `$` — Start reading from new messages only (don't replay old ones)
- `MKSTREAM` — Create the stream if it doesn't exist

**Enqueuing a task:**
```typescript
await redis.xadd(TASK_QUEUE, '*',
  'job_id', message.job_id,
  'payload', message.payload,
  // ...
);
```
- `*` tells Redis to auto-generate the message ID (timestamp-based)
- Fields are stored as key-value pairs in the stream entry

**Reading tasks (consumer side):**
```typescript
await redis.xreadgroup(
  'GROUP', CONSUMER_GROUP, consumerId,
  'COUNT', '5',       // Read up to 5 messages at a time
  'BLOCK', '2000',    // Block for up to 2 seconds waiting for new messages
  'STREAMS', TASK_QUEUE, '>'
);
```
- `>` means "give me messages that haven't been delivered to any consumer in this group"
- `BLOCK 2000` makes this a long-poll — the connection waits up to 2 seconds for new messages instead of returning immediately with nothing. This reduces CPU usage from constant polling.
- `COUNT 5` limits batch size so one worker doesn't grab everything

**Acknowledgment:**
```typescript
await redis.xack(TASK_QUEUE, CONSUMER_GROUP, messageId);
```
This tells Redis "I've processed this message, don't redeliver it." Without this, Redis would eventually redeliver the message to another consumer (after the consumer's idle timeout).

**Dead Letter Queue:**
```typescript
await redis.xadd(TASK_DLQ, '*', ...fields, 'error', error, 'failed_at', timestamp);
```
A separate stream (`task_dlq`) stores messages that failed after all retries. This preserves the full context (original payload, error message, attempt count) for debugging. The DLQ also supports **replay** — re-enqueueing a failed message back to the main queue for another attempt after the underlying issue is fixed.

---

## 8. Leader Election

### `src/services/leaderElection.ts`

**The problem:** The cron scheduler queries PostgreSQL for due jobs and enqueues them to Redis Streams. If ALL three workers run the scheduler, every cron job would execute 3 times (once per worker). We need exactly one scheduler.

**The solution: Redis distributed lock.**

**Acquiring leadership:**
```typescript
await redis.set(LEADER_LOCK_KEY, this.workerId, 'PX', 15000, 'NX');
```
Breaking this down:
- `SET leader_lock worker-1` — Set the key to this worker's ID
- `NX` — "Only set if Not eXists" — this is the critical atomic operation. If another worker already holds the lock, this returns null instead of overwriting.
- `PX 15000` — Auto-expire in 15,000 milliseconds. This is the **safety net** — if the leader crashes without releasing the lock, it expires automatically. Without TTL, a crashed leader would hold the lock forever (deadlock).

**Renewing leadership:**
The leader refreshes the lock every 5 seconds:
```typescript
await redis.pexpire(LEADER_LOCK_KEY, 15000);  // Reset TTL to 15s
```
The renewal interval (5s) is much less than the TTL (15s), giving a comfortable margin. Even if one renewal is delayed by network latency, the lock won't expire.

**Releasing leadership (graceful shutdown):**
```lua
-- Lua script runs atomically in Redis
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
```
**Why a Lua script?** This is a classic distributed systems pattern. Without it, you'd do:
1. `GET leader_lock` → "worker-1" (yes, I own it)
2. `DEL leader_lock`

But between steps 1 and 2, the lock could expire and be acquired by worker-2. Now step 2 deletes worker-2's lock! The Lua script makes the check-and-delete **atomic** — Redis executes the entire script without interleaving other commands.

**Failover timeline:**
1. Leader crashes at T=0
2. Lock expires at T=15s (TTL)
3. Another worker's next election attempt acquires the lock
4. New leader starts scheduler within T=15s + 5s (election interval) = ~20 seconds worst case

This is acceptable for a cron scheduler that polls every 10 seconds — missing one poll cycle is fine.

---

## 9. Scheduler Service

### `src/services/schedulerService.ts`

**What it does:** Runs on the leader only. Every 10 seconds, finds cron jobs that are due and enqueues them.

**The query:**
```sql
SELECT * FROM jobs
WHERE type = 'cron' AND status = 'active' AND next_run_at <= NOW()
ORDER BY priority DESC, next_run_at ASC
```
This fetches jobs ordered by priority (high first), then by how overdue they are. This ensures important jobs and late jobs get scheduled first.

**Preventing double-scheduling with advisory locks:**
```typescript
await pool.query('SELECT pg_try_advisory_xact_lock($1)', [hashJobId(job.id)]);
```
**Why is this needed?** Even with leader election, there's a tiny window during failover where two workers might briefly both think they're the leader. Advisory locks add a second layer of protection at the database level.

`pg_try_advisory_xact_lock` is a PostgreSQL-specific feature:
- It's a **lightweight lock** — doesn't lock any table rows, just a number
- `try` means it returns immediately (true/false) instead of waiting
- `xact` means it auto-releases when the transaction ends (no manual cleanup)
- The lock key is a hash of the job ID, so different jobs don't block each other

**After enqueuing:**
```typescript
const nextRun = getNextRunDate(job.cron_expression);
await updateJobNextRun(job.id, nextRun);
```
This calculates the NEXT occurrence from the cron expression and stores it. So a `"0 9 * * *"` job that just ran at 9:00 AM gets `next_run_at` set to tomorrow 9:00 AM.

---

## 10. Task Consumer (Worker Engine)

### `src/workers/taskConsumer.ts`

This is the heart of the worker. It's an infinite loop that reads from Redis Streams and executes tasks.

**The consumer loop:**
```typescript
while (this.isRunning) {
  const tasks = await readTasks(this.workerId, 5, 2000);
  for (const task of tasks) {
    this.processTask(task.id, task.message);  // Note: NOT awaited
  }
}
```
Tasks are processed **concurrently** (not awaited in the loop). This means one slow task doesn't block others. The `inProgressTasks` Set tracks what's currently executing for graceful shutdown.

**Handler registry:**
```typescript
const handlers: Record<string, (payload) => Promise<unknown>> = {
  'send-email': async (payload) => { ... },
  'process-image': async (payload) => { ... },
  'http-request': async (payload) => { ... },
};
```
Each handler is a named async function. When a task arrives with `handler: "send-email"`, we look up the function and call it. This is a simple **strategy pattern** — adding a new task type means adding a new function to this object.

**Timeout wrapper:**
```typescript
function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error('Task timed out')), timeoutMs)
    ),
  ]);
}
```
`Promise.race` returns whichever promise settles first. If the handler takes longer than `timeout_ms`, the timeout promise rejects first, and we treat the task as failed. This prevents a hung handler from blocking the worker forever.

**The retry flow:**
```
Attempt 1 fails → re-enqueue with attempt=2
Attempt 2 fails → re-enqueue with attempt=3
Attempt 3 fails → move to DLQ (max_retries=3)
```

On each failure:
1. `XACK` the original message (so Redis doesn't redeliver the OLD message)
2. If retries remain: `XADD` a NEW message with `attempt + 1`
3. If no retries remain: `XADD` to the DLQ stream, mark job as failed

**Why ACK before retry?** The original message has `attempt=1`. If we don't ACK it and just add a new message with `attempt=2`, we'd have two messages in the queue. ACKing the original ensures exactly one message exists at any time.

**Graceful shutdown (`drain()`):**
```typescript
async drain(): Promise<void> {
  while (this.inProgressTasks.size > 0 && Date.now() - start < 30000) {
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}
```
On SIGTERM/SIGINT, the worker:
1. Stops the consumer loop (no new tasks)
2. Waits up to 30 seconds for in-progress tasks to finish
3. Force-exits if tasks are still running (with a warning log)

---

## 11. Dead Letter Queue (DLQ)

### `src/workers/dlqProcessor.ts`

**What it does:** Periodically reads and logs failed tasks from the DLQ stream. Runs on the leader only to avoid duplicate logging.

**Why a DLQ matters:** Without a DLQ, permanently failing tasks would retry forever (eating resources) or silently disappear. The DLQ captures them with full context:
- Which job failed
- What error occurred
- How many attempts were made
- When it failed

**DLQ operations (exposed via queueService):**
- **List** — See what's in the DLQ
- **Replay** — Re-enqueue a message with attempt reset to 1 (after fixing the underlying issue)
- **Purge** — Clear old DLQ entries

---

## 12. Webhook Service

### `src/services/webhookService.ts`

**What it does:** Sends HTTP POST notifications to registered URLs when job events occur (completed, failed, retrying).

**HMAC signature verification:**
```typescript
const signature = crypto.createHmac('sha256', secret).update(body).digest('hex');
headers['X-Signature-256'] = `sha256=${signature}`;
```
This is the same pattern GitHub uses for webhook security. The receiver can:
1. Take the request body
2. Compute HMAC-SHA256 using their copy of the secret
3. Compare with the `X-Signature-256` header
4. If they match, the request is authentic (not forged)

**Retry with exponential backoff:**
```typescript
for (let attempt = 0; attempt <= retries; attempt++) {
  try {
    const response = await fetch(url, { ... });
    if (response.ok) return;  // Success, done
  } catch { }

  // Wait: 1s, 2s, 4s (exponential)
  const delay = Math.pow(2, attempt) * 1000;
  await new Promise(resolve => setTimeout(resolve, delay));
}
```
**Why exponential backoff?** If the webhook endpoint is temporarily down, hitting it every second makes things worse (amplifies load during outage). Exponential backoff gives the endpoint time to recover: 1s → 2s → 4s.

**Fire-and-forget pattern:**
```typescript
// Don't await — fire and forget
sendWithRetry(webhook.url, body, headers).catch((err) => {
  logger.error({ err }, 'Unhandled webhook dispatch error');
});
```
Webhook delivery does NOT block job execution. If the webhook endpoint is slow or down, the job still completes. The `.catch()` prevents unhandled promise rejection crashes.

---

## 13. Worker Heartbeat

### `src/workers/heartbeat.ts`

**What it does:** Every 5 seconds, each worker writes a timestamp to Redis:
```
SET worker:worker-1:heartbeat "2025-01-15T10:30:00Z" PX 15000
```

**PX 15000 is the key:** If the worker stops heartbeating (crash, network partition), the key expires in 15 seconds and the worker is considered dead.

**Used by the health endpoint:** The API reads all `worker:*:heartbeat` keys to report cluster status:
```json
{
  "workers": {
    "active": 3,
    "nodes": [
      { "id": "worker-1", "lastHeartbeat": "2025-01-15T10:30:05Z" },
      { "id": "worker-2", "lastHeartbeat": "2025-01-15T10:30:04Z" },
      { "id": "worker-3", "lastHeartbeat": "2025-01-15T10:30:03Z" }
    ]
  }
}
```

---

## 14. API Middleware Pipeline

Middlewares execute in order for every request. Each one can short-circuit the pipeline by sending a response.

### `auth.ts` — API Key Authentication
Checks the `X-API-Key` header. Returns 401 (missing) or 403 (wrong). Simple but effective for service-to-service auth. In production, you'd use JWT or OAuth, but API keys work well for internal services.

### `rateLimiter.ts` — Sliding Window Rate Limiting
```typescript
const windowTimestamp = Math.floor(Date.now() / window);  // e.g., minute boundary
const key = `ratelimit:${clientId}:${windowTimestamp}`;

const multi = redis.multi();
multi.incr(key);       // Atomic increment
multi.pexpire(key, window);  // Set expiry
const results = await multi.exec();
```

**Why Redis MULTI/EXEC?** The increment and TTL-set must be atomic. Without a transaction, the key could be incremented but never expire (if the process crashes between the two commands), leading to a permanent rate limit.

**Why sliding window?** The key includes a timestamp bucket (e.g., current minute). When the minute changes, a new key is used, and the old one expires. This creates a natural "sliding" effect.

**Response headers:**
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 87
X-RateLimit-Reset: 1705315260
Retry-After: 60  (only on 429)
```
These help clients self-throttle instead of blindly hitting the limit.

### `pagination.ts` — Cursor-Based Pagination
Extracts `cursor` and `limit` from query parameters and attaches them to `req.pagination`.

**Why cursor-based over offset-based?**
- **Offset-based** (`OFFSET 1000 LIMIT 20`): PostgreSQL must scan and discard 1000 rows before returning 20. Gets slower as offset increases.
- **Cursor-based** (`WHERE id > '01HX...' LIMIT 20`): Uses the B-tree index to jump directly to the cursor position. O(log n) regardless of page number.

### `validate.ts` — Zod Request Validation
Wraps Zod schema validation in an Express middleware. On validation failure, returns a structured error:
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "details": [
      { "field": "cron_expression", "message": "Required" }
    ]
  }
}
```

### `errorHandler.ts` — Global Error Handler
Express error handlers have 4 parameters `(err, req, res, next)`. This catches all unhandled errors and maps them to appropriate HTTP responses:
- `NotFoundError` → 404
- `ValidationError` → 400
- Everything else → 500 (with generic message — never leak stack traces to clients)

---

## 15. API Routes & Controllers

### Route Structure
```
POST   /api/v1/jobs              → Create a job
GET    /api/v1/jobs              → List jobs (paginated, filterable)
GET    /api/v1/jobs/:id          → Get job details
PATCH  /api/v1/jobs/:id          → Update a job
DELETE /api/v1/jobs/:id          → Delete a job
POST   /api/v1/jobs/:id/trigger  → Manually trigger a job
GET    /api/v1/jobs/:id/runs     → Get execution history
POST   /api/v1/webhooks          → Register a webhook
GET    /api/v1/webhooks          → List webhooks
DELETE /api/v1/webhooks/:id      → Remove a webhook
GET    /api/v1/health            → Health check (no auth)
```

**Controllers are thin.** They extract request data, call services, and format responses. Business logic lives in services. This separation makes testing easier — you can test service logic without spinning up an HTTP server.

**Versioned API** (`/api/v1/`) — Allows introducing breaking changes under `/api/v2/` without affecting existing clients.

---

## 16. Entry Points (API Server & Worker)

### `src/index.ts` — API Server

Startup sequence:
1. Run database migrations (creates tables if they don't exist)
2. Register middleware pipeline
3. Register routes
4. Start listening on the configured port

The health check route is registered **before** the auth middleware — health checks should work without authentication (for load balancers and monitoring tools).

### `src/worker.ts` — Worker Process

This is the most complex entry point. Startup sequence:

1. **Generate worker ID** — Uses hostname + random suffix (e.g., `worker-1` or `api-server-a1b2c3`)
2. **Run migrations** — Ensures tables exist even if the API hasn't started yet
3. **Initialize Redis Streams** — Creates consumer groups if they don't exist
4. **Start heartbeat** — Begins broadcasting "I'm alive" to Redis
5. **Start leader election** — Begins attempting to acquire the leader lock
6. **Monitor leader status** — A 2-second interval checks if this worker became/lost leader:
   - Became leader → Start scheduler + DLQ processor
   - Lost leadership → Stop scheduler + DLQ processor
7. **Start task consumer** — All workers consume tasks (not just the leader)

**Graceful shutdown:**
```typescript
async function shutdown() {
  taskConsumer.stop();          // Stop accepting new tasks
  schedulerService.stop();      // Stop scheduling
  dlqProcessor.stop();          // Stop DLQ processing
  leaderElection.stopElectionLoop();
  await leaderElection.releaseLeadership();  // Free the lock immediately
  heartbeat.stop();
  await taskConsumer.drain();   // Wait for in-progress tasks
  await closeRedisConnection();
  await closeDatabasePool();
}
```

The order matters:
1. Stop accepting work first
2. Release leadership (so another worker takes over quickly)
3. THEN drain in-progress work
4. THEN close connections

---

## 17. Docker & Deployment

### `Dockerfile`
```dockerfile
FROM node:20-alpine AS base
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --production=false  # Install all deps (including tsx for runtime)
COPY tsconfig.json ./
COPY src ./src
```
Uses Alpine Linux for a small image. `npm ci` (instead of `npm install`) installs from the lock file exactly, ensuring reproducible builds.

### `docker-compose.yml`

**Service dependency with health checks:**
```yaml
depends_on:
  postgres:
    condition: service_healthy
  redis:
    condition: service_healthy
```
Workers and API don't start until PostgreSQL and Redis are actually ready (not just running). The health checks use `pg_isready` and `redis-cli ping`.

**All services share the same network** — Docker Compose creates a default bridge network. `postgres` and `redis` are DNS names that resolve to the respective containers.

---

## 18. Key Design Decisions & Trade-offs

### 1. PostgreSQL + Redis (dual storage) vs. Redis only
**Decision:** Use PostgreSQL as source of truth, Redis as message broker.
**Trade-off:** More complexity, but PostgreSQL gives ACID durability. If Redis crashes, we lose in-flight messages but can recover from PostgreSQL. If we used Redis only, a crash could lose job definitions.

### 2. Redis Streams vs. RabbitMQ/Kafka
**Decision:** Redis Streams.
**Trade-off:** Simpler ops (Redis is already in the stack for locks/rate-limiting). Kafka would be better for extremely high throughput (millions/sec), but Redis Streams handles our scale with lower operational burden.

### 3. Leader election vs. distributed scheduling
**Decision:** Single leader runs the scheduler.
**Trade-off:** Simple and correct — prevents duplicate cron execution. The downside is the scheduler is a single point of throughput (one node does all scheduling). For our scale (thousands of cron jobs), this is fine. At massive scale, you'd partition jobs across multiple schedulers.

### 4. At-least-once delivery vs. exactly-once
**Decision:** At-least-once (via Redis Streams ACK).
**Trade-off:** A task MIGHT execute twice in edge cases (worker crashes after executing but before ACKing). Exactly-once is extremely hard in distributed systems. We mitigate with idempotent handlers and the advisory lock pattern.

### 5. Raw SQL vs. ORM
**Decision:** Raw parameterized SQL.
**Trade-off:** More boilerplate, but full control over queries, no ORM learning curve, no query plan surprises, easier debugging.

---

## 19. Failure Scenarios & How the System Handles Them

### Scenario 1: Leader worker crashes
- Leader's Redis lock expires in 15 seconds
- Another worker acquires the lock in the next election cycle (5s interval)
- New leader starts the scheduler
- **Impact:** Up to ~20 seconds of no cron scheduling. No data loss.

### Scenario 2: Worker crashes mid-task
- The task message was NOT acknowledged (XACK)
- After Redis's consumer idle timeout, the message becomes available for redelivery
- Another worker picks it up
- **Impact:** Task executes again. Handlers should be idempotent.

### Scenario 3: Redis goes down
- API can still write jobs to PostgreSQL (jobs are safe)
- Rate limiter fails open (allows requests through — better than blocking everything)
- Workers can't consume tasks or send heartbeats
- When Redis recovers, the scheduler picks up due jobs on next poll
- **Impact:** Temporary inability to process tasks. No data loss.

### Scenario 4: PostgreSQL goes down
- API returns 500 on all database operations
- Workers can't create job_run records
- Health endpoint reports database as disconnected
- **Impact:** System is degraded but Redis Streams still has messages. Recovery requires PostgreSQL to come back.

### Scenario 5: Network partition between worker and Redis
- Worker's heartbeat key expires — it appears dead
- Worker's leader lock expires — another worker becomes leader
- Worker may still be processing a task (now duplicated with new leader scheduling)
- **Mitigation:** Advisory locks + idempotent handlers

---

## 20. Load Testing with k6

### What is k6?

k6 is an open-source load testing tool built by Grafana Labs. Unlike tools like JMeter (GUI-heavy, XML config), k6 tests are written in **plain JavaScript**, making them version-controllable and easy to read. It simulates many concurrent users hitting your API to find performance limits, bottlenecks, and breaking points before real users do.

### Why Load Test a Task Scheduler?

The scheduler accepts jobs via API and distributes them to workers. Under real load:
- Can the API handle hundreds of concurrent job creation requests?
- Can 3 workers actually keep up with the queue?
- Does the rate limiter work correctly under pressure?
- Do tasks actually complete (end-to-end), or do they silently fail?

Without load testing, you're guessing. With it, you have hard numbers.

### The Test Script — Line by Line

**File: `k6/load-test.js`**

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
```
- `k6/http` — k6's built-in HTTP client (not Node's `fetch`)
- `check` — Assertions that don't abort the test on failure (unlike `assert`). They just track pass/fail rates.
- `Rate` — A custom metric that tracks a percentage (what % of tasks completed?)

```javascript
const taskCompletionRate = new Rate('task_completion_rate');
```
Creates a custom metric. Every time we call `taskCompletionRate.add(true/false)`, it updates the running percentage. At the end, k6 reports something like `task_completion_rate: 99.7%`.

### Load Profile (Stages)

```javascript
export const options = {
  stages: [
    { duration: '30s', target: 20 },   // Ramp up
    { duration: '1m', target: 50 },     // Sustained load
    { duration: '30s', target: 100 },   // Peak load
    { duration: '30s', target: 0 },     // Ramp down
  ],
```

This defines how many **Virtual Users (VUs)** are active at each point in time:

```
VUs
100 |                        ┌──────┐
    |                       /        \
 50 |          ┌───────────┘          \
    |         /                        \
 20 |    ┌───┘                          \
    |   /                                \
  0 |──┘                                  └──
    └──────────────────────────────────────── Time
    0s   30s          90s    120s       150s
       Ramp Up    Sustained   Peak   Ramp Down
```

Each VU runs the `default` function in a loop. So at peak (100 VUs), 100 users are simultaneously creating jobs and checking results. Over 2.5 minutes, the test creates **thousands of jobs**.

**Why this shape?**
- **Ramp up** — Avoids a thundering herd that could crash the system before it warms up
- **Sustained** — Tests steady-state performance (connection pools, memory)
- **Peak** — Finds the breaking point
- **Ramp down** — Verifies the system recovers cleanly (no leaked connections, no stuck tasks)

### Pass/Fail Thresholds

```javascript
  thresholds: {
    http_req_duration: ['p(95)<500'],
    task_completion_rate: ['rate>0.995'],
  },
};
```

These are the **hard pass/fail criteria**:
- **`p(95)<500`** — 95% of HTTP requests must complete in under 500ms. This is industry-standard for APIs. If the API takes more than half a second for 5%+ of requests, the test FAILS.
- **`rate>0.995`** — 99.5% of tasks must actually complete (not just be accepted). This is the **end-to-end** reliability metric. It catches issues where the API accepts jobs fine but workers fail to process them.

If either threshold is breached, k6 exits with a non-zero code — useful in CI/CD pipelines to block deploys.

### The Test Function (What Each VU Does)

```javascript
const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const API_KEY = __ENV.API_KEY || 'test-api-key-12345';
const headers = { 'Content-Type': 'application/json', 'X-API-Key': API_KEY };
```
`__ENV` reads environment variables passed via `-e` flag. Defaults allow running without config.

```javascript
export default function () {
```
This function runs **once per iteration per VU**. At 100 VUs, 100 instances of this function run concurrently.

**Step 1 — Create a job:**
```javascript
  const createRes = http.post(`${BASE_URL}/api/v1/jobs`, JSON.stringify({
    name: `load-test-job-${Date.now()}`,
    type: 'once',
    handler: 'http-request',
    payload: { url: 'https://httpbin.org/post', options: { method: 'POST' } },
    max_retries: 2,
  }), { headers });

  check(createRes, { 'job created': (r) => r.status === 201 });
```
Creates a one-time job that makes an HTTP request to httpbin.org (a test endpoint). The `check()` verifies the API returned 201 Created. If it returns 429 (rate limited) or 500, this check fails but the test continues.

**Step 2 — Wait for execution:**
```javascript
  sleep(5);
```
Pauses 5 seconds to give workers time to pick up and execute the job. This is realistic — in production you wouldn't poll instantly either.

**Step 3 — Verify completion:**
```javascript
    const statusRes = http.get(`${BASE_URL}/api/v1/jobs/${jobId}`, { headers });
    const job = JSON.parse(statusRes.body).data;
    taskCompletionRate.add(job.status === 'completed');
```
Fetches the job and checks if its status is `'completed'`. Adds `true` (completed) or `false` (still running/failed) to the completion rate metric.

```javascript
  sleep(1);
```
1-second pause between iterations to simulate realistic user behavior (not a pure stress test).

### How to Run It

```bash
# Basic run (with Docker Compose already up)
k6 run k6/load-test.js

# Custom API URL and key
k6 run -e BASE_URL=http://localhost:3000 -e API_KEY=test-api-key-12345 k6/load-test.js

# Quick smoke test (10 VUs, 30 seconds)
k6 run --vus 10 --duration 30s k6/load-test.js
```

### Sample Output

```
     ✓ job created

     checks.........................: 98.50% ✓ 2847  ✗ 43
     data_received..................: 4.2 MB  28 kB/s
     http_req_duration..............: avg=45ms  min=8ms  p(95)=187ms  p(99)=412ms
   ✓ http_req_duration..............: p(95)<500
     http_reqs......................: 5694    37.96/s
     iterations.....................: 2890    19.27/s
   ✓ task_completion_rate...........: 99.72%  ✓ 2839  ✗ 8

     running (2m30s), 000/100 VUs, 2890 complete iterations
     default ✓ [==============================] 000/100 VUs  2m30s
```

**Reading the output:**
- **checks: 98.50%** — 43 job creations failed (likely rate-limited, which is correct behavior)
- **http_req_duration p(95)=187ms** — 95th percentile is 187ms, well under our 500ms threshold (PASSED)
- **task_completion_rate: 99.72%** — 99.72% of jobs completed, above our 99.5% target (PASSED)
- **http_reqs: 5694** — Total HTTP requests made during the test
- **iterations: 2890** — Each iteration = 1 job created + 1 status check = 2 HTTP requests

### What the Test Reveals About Our System

| Metric | What It Tests |
|--------|--------------|
| `http_req_duration` | API server performance — Express routing, middleware, database queries |
| `task_completion_rate` | End-to-end reliability — API + Redis Streams + Worker execution |
| `checks` (job created) | Rate limiter correctness — some 429s are expected at peak load |
| `iterations` | Overall throughput — how many jobs/second the system handles |

If `http_req_duration` is high but `task_completion_rate` is good → API is slow but workers keep up.
If `http_req_duration` is fine but `task_completion_rate` is low → Workers are the bottleneck (scale workers up).
If both are bad → Infrastructure issue (database/Redis overloaded).

---

## 21. How to Explain This in an Interview

### The 30-Second Pitch
> "I built a distributed task scheduler in Node.js/TypeScript. It supports cron jobs, one-time tasks, and delayed execution across multiple worker nodes. I used Redis Streams for the message queue with consumer groups for load balancing, PostgreSQL for durable job storage, and Redis distributed locks for leader election. The leader runs the cron scheduler, all workers consume tasks. Failed tasks retry with exponential backoff and eventually land in a dead letter queue. The system handles leader failover within 15 seconds."

### Key Points to Hit
1. **"I chose Redis Streams over simpler alternatives because..."** — consumer groups, acknowledgment, persistence
2. **"Leader election prevents duplicate cron execution using..."** — Redis NX + TTL + Lua script for safe release
3. **"The system is fault-tolerant because..."** — automatic leader failover, message redelivery on worker crash, DLQ for permanently failing tasks
4. **"I avoided an ORM because..."** — needed advisory locks, partial indexes, complex dynamic queries
5. **"The timeout wrapper uses Promise.race to..."** — prevent hung handlers from blocking the worker

### Common Follow-Up Questions

**Q: How do you ensure exactly-once execution?**
A: The system provides at-least-once delivery. True exactly-once is impractical in distributed systems. I mitigate duplicate execution with PostgreSQL advisory locks for scheduling and idempotent handler design.

**Q: What happens at very high scale?**
A: The current bottleneck is the single-leader scheduler. At scale, I'd partition cron jobs across multiple scheduler instances (e.g., by hash of job ID). Redis Streams already scales horizontally via consumer groups.

**Q: Why not use an existing queue like RabbitMQ or Kafka?**
A: Redis was already in the stack for locks, rate limiting, and heartbeats. Adding another infrastructure component increases operational burden. Redis Streams provides the consumer group semantics we need without a separate system.

**Q: How would you add monitoring?**
A: I'd expose Prometheus metrics (task execution latency, queue depth, error rate, DLQ size) and set up Grafana dashboards. The structured JSON logging with pino already supports log aggregation tools like ELK or Datadog.

**Q: How would you handle very long-running tasks?**
A: I'd add a task progress API — workers periodically update a Redis key with progress percentage. The consumer would renew the pending message's visibility timeout (via XCLAIM) to prevent redelivery while the task is still running.
