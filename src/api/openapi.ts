import { z } from 'zod';
import {
  extendZodWithOpenApi,
  OpenAPIRegistry,
  OpenApiGeneratorV3,
} from '@asteasolutions/zod-to-openapi';

extendZodWithOpenApi(z);

const registry = new OpenAPIRegistry();

// ── Security scheme ──────────────────────────────────────────────────
const apiKeyScheme = registry.registerComponent('securitySchemes', 'ApiKey', {
  type: 'apiKey',
  in: 'header',
  name: 'X-API-Key',
});

// ── Reusable schemas ─────────────────────────────────────────────────

const JobTypeSchema = z.enum(['cron', 'once', 'delayed']).openapi('JobType');
const JobStatusSchema = z
  .enum(['active', 'paused', 'completed', 'failed', 'cancelled'])
  .openapi('JobStatus');
const JobRunStatusSchema = z
  .enum(['running', 'completed', 'failed', 'timed_out', 'retrying'])
  .openapi('JobRunStatus');
const WebhookEventSchema = z
  .enum(['completed', 'failed', 'retrying'])
  .openapi('WebhookEvent');

const JobSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    description: z.string().nullable(),
    type: JobTypeSchema,
    cron_expression: z.string().nullable(),
    scheduled_at: z.string().nullable(),
    payload: z.record(z.unknown()),
    handler: z.string(),
    status: JobStatusSchema,
    max_retries: z.number().int(),
    retry_count: z.number().int(),
    timeout_ms: z.number().int(),
    priority: z.number().int(),
    metadata: z.record(z.unknown()),
    created_at: z.string(),
    updated_at: z.string(),
    next_run_at: z.string().nullable(),
    last_run_at: z.string().nullable(),
  })
  .openapi('Job');

const JobRunSchema = z
  .object({
    id: z.string(),
    job_id: z.string(),
    worker_id: z.string(),
    status: JobRunStatusSchema,
    started_at: z.string(),
    completed_at: z.string().nullable(),
    duration_ms: z.number().nullable(),
    attempt: z.number().int(),
    error_message: z.string().nullable(),
    result: z.record(z.unknown()).nullable(),
    created_at: z.string(),
  })
  .openapi('JobRun');

const WebhookSchema = z
  .object({
    id: z.string(),
    job_id: z.string().nullable(),
    url: z.string(),
    events: z.array(WebhookEventSchema),
    secret: z.string().nullable(),
    is_active: z.boolean(),
    created_at: z.string(),
  })
  .openapi('Webhook');

const ErrorSchema = z
  .object({
    success: z.literal(false),
    error: z.object({
      code: z.string(),
      message: z.string(),
      details: z
        .array(z.object({ field: z.string(), message: z.string() }))
        .optional(),
    }),
  })
  .openapi('Error');

const PaginationSchema = z
  .object({
    next_cursor: z.string().nullable(),
    has_more: z.boolean(),
    limit: z.number().int(),
  })
  .openapi('Pagination');

// ── Request body schemas ─────────────────────────────────────────────

const CreateJobBodySchema = z
  .object({
    name: z.string().min(1).max(255),
    description: z.string().optional(),
    type: JobTypeSchema,
    cron_expression: z.string().optional(),
    scheduled_at: z.string().datetime().optional(),
    payload: z.record(z.unknown()).default({}).optional(),
    handler: z.string().min(1).max(255),
    max_retries: z.number().int().min(0).max(10).default(3).optional(),
    timeout_ms: z.number().int().min(1000).max(300000).default(30000).optional(),
    priority: z.number().int().min(0).max(100).default(0).optional(),
    metadata: z.record(z.unknown()).default({}).optional(),
  })
  .openapi('CreateJobBody');

const UpdateJobBodySchema = z
  .object({
    name: z.string().min(1).max(255).optional(),
    description: z.string().optional(),
    status: z.enum(['active', 'paused', 'cancelled']).optional(),
    cron_expression: z.string().optional(),
    payload: z.record(z.unknown()).optional(),
    max_retries: z.number().int().min(0).max(10).optional(),
    timeout_ms: z.number().int().min(1000).max(300000).optional(),
    priority: z.number().int().min(0).max(100).optional(),
    metadata: z.record(z.unknown()).optional(),
  })
  .openapi('UpdateJobBody');

const CreateWebhookBodySchema = z
  .object({
    job_id: z.string().optional(),
    url: z.string().url().max(2048),
    events: z.array(WebhookEventSchema).min(1),
    secret: z.string().max(255).optional(),
  })
  .openapi('CreateWebhookBody');

// ── Helper to wrap in ApiResponse envelope ───────────────────────────

function successResponse(dataSchema: z.ZodTypeAny, description: string) {
  return {
    description,
    content: {
      'application/json': {
        schema: z.object({ success: z.literal(true), data: dataSchema }),
      },
    },
  };
}

function paginatedResponse(itemSchema: z.ZodTypeAny, description: string) {
  return {
    description,
    content: {
      'application/json': {
        schema: z.object({
          success: z.literal(true),
          data: z.array(itemSchema),
          pagination: PaginationSchema,
        }),
      },
    },
  };
}

function errorResponse(description: string) {
  return {
    description,
    content: { 'application/json': { schema: ErrorSchema } },
  };
}

// ── Path registrations ───────────────────────────────────────────────

// GET /api/v1/health
registry.registerPath({
  method: 'get',
  path: '/api/v1/health',
  tags: ['Health'],
  summary: 'Health check',
  description: 'Returns the health status of the API, database, Redis, and worker nodes.',
  responses: {
    200: {
      description: 'Service is healthy',
      content: {
        'application/json': {
          schema: z.object({
            success: z.literal(true),
            data: z.object({
              status: z.enum(['healthy', 'degraded']),
              timestamp: z.string(),
              services: z.object({
                database: z.enum(['connected', 'disconnected']),
                redis: z.enum(['connected', 'disconnected']),
              }),
              workers: z.object({
                active: z.number().int(),
                nodes: z.array(
                  z.object({ id: z.string(), lastHeartbeat: z.string() })
                ),
              }),
            }),
          }),
        },
      },
    },
    503: errorResponse('Service is degraded'),
  },
});

// POST /api/v1/jobs
registry.registerPath({
  method: 'post',
  path: '/api/v1/jobs',
  tags: ['Jobs'],
  summary: 'Create a job',
  description:
    'Create a new scheduled job. Cron jobs require cron_expression; delayed jobs require scheduled_at.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    body: {
      required: true,
      content: { 'application/json': { schema: CreateJobBodySchema } },
    },
  },
  responses: {
    201: successResponse(JobSchema, 'Job created'),
    400: errorResponse('Validation error'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// GET /api/v1/jobs
registry.registerPath({
  method: 'get',
  path: '/api/v1/jobs',
  tags: ['Jobs'],
  summary: 'List jobs',
  description: 'Retrieve a paginated list of jobs with optional filters.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    query: z.object({
      status: JobStatusSchema.optional(),
      type: JobTypeSchema.optional(),
      handler: z.string().optional(),
      cursor: z.string().optional(),
      limit: z.coerce.number().int().min(1).max(100).default(20).optional(),
    }),
  },
  responses: {
    200: paginatedResponse(JobSchema, 'List of jobs'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// GET /api/v1/jobs/:id
registry.registerPath({
  method: 'get',
  path: '/api/v1/jobs/{id}',
  tags: ['Jobs'],
  summary: 'Get a job',
  description: 'Retrieve a single job by its ID.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    params: z.object({ id: z.string() }),
  },
  responses: {
    200: successResponse(JobSchema, 'Job details'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    404: errorResponse('Job not found'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// PATCH /api/v1/jobs/:id
registry.registerPath({
  method: 'patch',
  path: '/api/v1/jobs/{id}',
  tags: ['Jobs'],
  summary: 'Update a job',
  description: 'Partially update a job. Only provided fields are changed.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    params: z.object({ id: z.string() }),
    body: {
      required: true,
      content: { 'application/json': { schema: UpdateJobBodySchema } },
    },
  },
  responses: {
    200: successResponse(JobSchema, 'Job updated'),
    400: errorResponse('Validation error'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    404: errorResponse('Job not found'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// DELETE /api/v1/jobs/:id
registry.registerPath({
  method: 'delete',
  path: '/api/v1/jobs/{id}',
  tags: ['Jobs'],
  summary: 'Delete a job',
  description: 'Delete a job by its ID.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    params: z.object({ id: z.string() }),
  },
  responses: {
    204: { description: 'Job deleted' },
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    404: errorResponse('Job not found'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// POST /api/v1/jobs/:id/trigger
registry.registerPath({
  method: 'post',
  path: '/api/v1/jobs/{id}/trigger',
  tags: ['Jobs'],
  summary: 'Trigger a job',
  description: 'Manually trigger immediate execution of a job.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    params: z.object({ id: z.string() }),
  },
  responses: {
    202: successResponse(
      z.object({ message: z.string(), job_id: z.string() }),
      'Job triggered'
    ),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    404: errorResponse('Job not found'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// GET /api/v1/jobs/:id/runs
registry.registerPath({
  method: 'get',
  path: '/api/v1/jobs/{id}/runs',
  tags: ['Jobs'],
  summary: 'Get job runs',
  description: 'Retrieve a paginated list of execution runs for a specific job.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    params: z.object({ id: z.string() }),
    query: z.object({
      cursor: z.string().optional(),
      limit: z.coerce.number().int().min(1).max(100).default(20).optional(),
    }),
  },
  responses: {
    200: paginatedResponse(JobRunSchema, 'List of job runs'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    404: errorResponse('Job not found'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// POST /api/v1/webhooks
registry.registerPath({
  method: 'post',
  path: '/api/v1/webhooks',
  tags: ['Webhooks'],
  summary: 'Create a webhook',
  description: 'Register a new webhook to receive notifications for job events.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    body: {
      required: true,
      content: { 'application/json': { schema: CreateWebhookBodySchema } },
    },
  },
  responses: {
    201: successResponse(WebhookSchema, 'Webhook created'),
    400: errorResponse('Validation error'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// GET /api/v1/webhooks
registry.registerPath({
  method: 'get',
  path: '/api/v1/webhooks',
  tags: ['Webhooks'],
  summary: 'List webhooks',
  description: 'Retrieve a paginated list of registered webhooks.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    query: z.object({
      cursor: z.string().optional(),
      limit: z.coerce.number().int().min(1).max(100).default(20).optional(),
    }),
  },
  responses: {
    200: paginatedResponse(WebhookSchema, 'List of webhooks'),
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// DELETE /api/v1/webhooks/:id
registry.registerPath({
  method: 'delete',
  path: '/api/v1/webhooks/{id}',
  tags: ['Webhooks'],
  summary: 'Delete a webhook',
  description: 'Remove a registered webhook.',
  security: [{ [apiKeyScheme.name]: [] }],
  request: {
    params: z.object({ id: z.string() }),
  },
  responses: {
    204: { description: 'Webhook deleted' },
    401: errorResponse('Missing API key'),
    403: errorResponse('Invalid API key'),
    404: errorResponse('Webhook not found'),
    429: errorResponse('Rate limit exceeded'),
  },
});

// ── Generate document ────────────────────────────────────────────────

const generator = new OpenApiGeneratorV3(registry.definitions);

export const openApiDocument = generator.generateDocument({
  openapi: '3.0.3',
  info: {
    title: 'Distributed Task Scheduler API',
    version: '1.0.0',
    description:
      'A distributed task scheduler supporting cron, one-time, and delayed jobs with webhook notifications.',
  },
  servers: [{ url: 'http://localhost:3000', description: 'Local development' }],
});
