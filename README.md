# Distributed Task Scheduler

A production-grade distributed task scheduling system built with Node.js/TypeScript that supports cron-based and asynchronous job execution with fault tolerance, guaranteed delivery, and horizontal scaling across multiple worker nodes.

## Architecture

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

## Features

- **Cron scheduling** - Recurring jobs with standard cron expressions
- **One-time & delayed jobs** - Execute once immediately or at a scheduled time
- **Distributed execution** - Horizontal scaling across multiple worker nodes
- **Leader election** - Redis-based leader election ensures only one scheduler runs
- **Fault tolerance** - Automatic leader failover within 15 seconds
- **Guaranteed delivery** - Redis Streams with consumer groups for at-least-once delivery
- **Dead letter queue** - Failed tasks after max retries move to DLQ for inspection/replay
- **Webhook notifications** - HTTP callbacks on job completion, failure, or retry with HMAC signing
- **Rate limiting** - Redis-backed sliding window rate limiter
- **Cursor pagination** - ULID-based cursor pagination for all list endpoints
- **API key authentication** - Simple API key auth middleware
- **Health checks** - Full system health with database, Redis, and worker status
- **Structured logging** - JSON logging with pino

## Quick Start

```bash
# Clone and start with Docker Compose
git clone <repo-url>
cd distributed-task-scheduler
docker-compose up --build
```

The API server will be available at `http://localhost:3000`.

## API Documentation

All requests require the `X-API-Key` header (default: `test-api-key-12345`).

### Create a Job

```bash
# One-time job (executes immediately)
curl -X POST http://localhost:3000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-api-key-12345" \
  -d '{
    "name": "my-email-job",
    "type": "once",
    "handler": "send-email",
    "payload": { "to": "user@example.com", "subject": "Hello" }
  }'

# Cron job (runs every 5 minutes)
curl -X POST http://localhost:3000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-api-key-12345" \
  -d '{
    "name": "report-generator",
    "type": "cron",
    "cron_expression": "*/5 * * * *",
    "handler": "generate-report",
    "payload": { "type": "daily-summary" }
  }'

# Delayed job (executes at a specific time)
curl -X POST http://localhost:3000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-api-key-12345" \
  -d '{
    "name": "scheduled-task",
    "type": "delayed",
    "scheduled_at": "2025-01-01T00:00:00Z",
    "handler": "http-request",
    "payload": { "url": "https://httpbin.org/post", "options": { "method": "POST" } }
  }'
```

### List Jobs

```bash
curl http://localhost:3000/api/v1/jobs?limit=10&status=active \
  -H "X-API-Key: test-api-key-12345"
```

### Get Job Details

```bash
curl http://localhost:3000/api/v1/jobs/<job_id> \
  -H "X-API-Key: test-api-key-12345"
```

### Update a Job

```bash
curl -X PATCH http://localhost:3000/api/v1/jobs/<job_id> \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-api-key-12345" \
  -d '{ "status": "paused" }'
```

### Delete a Job

```bash
curl -X DELETE http://localhost:3000/api/v1/jobs/<job_id> \
  -H "X-API-Key: test-api-key-12345"
```

### Trigger a Job Manually

```bash
curl -X POST http://localhost:3000/api/v1/jobs/<job_id>/trigger \
  -H "X-API-Key: test-api-key-12345"
```

### Get Job Execution History

```bash
curl http://localhost:3000/api/v1/jobs/<job_id>/runs \
  -H "X-API-Key: test-api-key-12345"
```

### Register a Webhook

```bash
curl -X POST http://localhost:3000/api/v1/webhooks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-api-key-12345" \
  -d '{
    "job_id": "<job_id>",
    "url": "https://example.com/webhook",
    "events": ["completed", "failed"],
    "secret": "my-webhook-secret"
  }'
```

### Health Check

```bash
curl http://localhost:3000/api/v1/health
```

## Load Testing

Run load tests with k6:

```bash
# Install k6 first: https://k6.io/docs/getting-started/installation/
k6 run k6/load-test.js

# Or with custom config
k6 run -e BASE_URL=http://localhost:3000 -e API_KEY=test-api-key-12345 k6/load-test.js
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL connection string |
| `REDIS_URL` | - | Redis connection string |
| `PORT` | `3000` | API server port |
| `API_KEY` | - | API authentication key |
| `RATE_LIMIT_MAX` | `100` | Max requests per window |
| `RATE_LIMIT_WINDOW_MS` | `60000` | Rate limit window (ms) |
| `WORKER_ID` | auto-generated | Unique worker identifier |
| `LEADER_LOCK_TTL_MS` | `15000` | Leader lock TTL (ms) |
| `LEADER_RENEW_INTERVAL_MS` | `5000` | Leader lock renewal interval |
| `HEARTBEAT_INTERVAL_MS` | `5000` | Worker heartbeat interval |
| `SCHEDULER_POLL_INTERVAL_MS` | `10000` | Cron scheduler poll interval |
| `WEBHOOK_TIMEOUT_MS` | `5000` | Webhook delivery timeout |
| `WEBHOOK_MAX_RETRIES` | `3` | Webhook retry attempts |

## Leader Election

The system uses Redis-based leader election to ensure exactly one worker runs the cron scheduler:

1. Workers attempt to acquire a Redis lock (`SET leader_lock <worker_id> NX PX 15000`)
2. The winner becomes leader and starts the cron scheduler
3. Leader refreshes the lock every 5 seconds (before 15s expiry)
4. If the leader crashes, the lock expires in 15 seconds and another worker takes over
5. Lock release uses a Lua script for atomic check-and-delete to prevent releasing another worker's lock

## License

MIT
