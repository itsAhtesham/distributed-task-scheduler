import { generateId } from '../utils/idGenerator.js';
import { validateCronExpression, getNextRunDate } from '../utils/cronParser.js';
import { enqueueTask } from './queueService.js';
import * as jobQueries from '../db/queries/jobs.queries.js';
import type { Job, JobRun, CreateJobInput, UpdateJobInput, JobFilters } from '../types/job.types.js';
import type { PaginationParams, PaginatedResult } from '../types/common.types.js';
import { logger } from '../utils/logger.js';

export async function createJob(input: CreateJobInput): Promise<Job> {
  // Validate cron expression if provided
  if (input.type === 'cron' && input.cron_expression) {
    if (!validateCronExpression(input.cron_expression)) {
      throw new ValidationError('Invalid cron expression', 'cron_expression');
    }
  }

  const id = generateId();
  let nextRunAt: Date | null = null;

  if (input.type === 'cron' && input.cron_expression) {
    nextRunAt = getNextRunDate(input.cron_expression);
  } else if (input.type === 'delayed' && input.scheduled_at) {
    nextRunAt = new Date(input.scheduled_at);
  }

  const job = await jobQueries.insertJob(id, input, nextRunAt);

  // For 'once' type jobs, enqueue immediately
  if (input.type === 'once') {
    await enqueueTask({
      job_id: job.id,
      payload: JSON.stringify(input.payload),
      attempt: 1,
      priority: input.priority ?? 0,
      enqueued_at: new Date().toISOString(),
      timeout_ms: input.timeout_ms ?? 30000,
      max_retries: input.max_retries ?? 3,
      handler: input.handler,
    });
    logger.info({ jobId: job.id }, 'One-time job enqueued immediately');
  }

  return job;
}

export async function getJob(id: string): Promise<Job> {
  const job = await jobQueries.findJobById(id);
  if (!job) {
    throw new NotFoundError(`Job ${id} not found`);
  }
  return job;
}

export async function listJobs(
  filters: JobFilters,
  pagination: PaginationParams
): Promise<PaginatedResult<Job>> {
  return jobQueries.listJobs(filters, pagination);
}

export async function updateJob(id: string, updates: UpdateJobInput): Promise<Job> {
  const existing = await jobQueries.findJobById(id);
  if (!existing) {
    throw new NotFoundError(`Job ${id} not found`);
  }

  // If updating cron expression, recalculate next_run_at
  let nextRunAt: Date | null | undefined = undefined;
  if (updates.cron_expression && existing.type === 'cron') {
    if (!validateCronExpression(updates.cron_expression)) {
      throw new ValidationError('Invalid cron expression', 'cron_expression');
    }
    nextRunAt = getNextRunDate(updates.cron_expression);
  }

  // If resuming a paused cron job, recalculate next_run_at
  if (updates.status === 'active' && existing.status === 'paused' && existing.type === 'cron' && existing.cron_expression) {
    nextRunAt = getNextRunDate(existing.cron_expression);
  }

  const updateData = nextRunAt !== undefined ? { ...updates, next_run_at: nextRunAt } : updates;
  const job = await jobQueries.updateJob(id, updateData);
  if (!job) {
    throw new NotFoundError(`Job ${id} not found`);
  }
  return job;
}

export async function deleteJob(id: string): Promise<void> {
  const deleted = await jobQueries.deleteJob(id);
  if (!deleted) {
    throw new NotFoundError(`Job ${id} not found`);
  }
}

export async function triggerJob(id: string, workerId?: string): Promise<JobRun> {
  const job = await jobQueries.findJobById(id);
  if (!job) {
    throw new NotFoundError(`Job ${id} not found`);
  }

  await enqueueTask({
    job_id: job.id,
    payload: JSON.stringify(job.payload),
    attempt: 1,
    priority: job.priority,
    enqueued_at: new Date().toISOString(),
    timeout_ms: job.timeout_ms,
    max_retries: job.max_retries,
    handler: job.handler,
  });

  const runId = generateId();
  const jobRun = await jobQueries.insertJobRun(runId, job.id, workerId || 'api-trigger', 1);

  logger.info({ jobId: id, runId }, 'Job triggered manually');
  return jobRun;
}

export async function getJobRuns(
  jobId: string,
  pagination: PaginationParams
): Promise<PaginatedResult<JobRun>> {
  // Verify job exists
  const job = await jobQueries.findJobById(jobId);
  if (!job) {
    throw new NotFoundError(`Job ${jobId} not found`);
  }
  return jobQueries.getJobRuns(jobId, pagination);
}

// Custom error classes
export class NotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NotFoundError';
  }
}

export class ValidationError extends Error {
  public field: string;
  constructor(message: string, field: string) {
    super(message);
    this.name = 'ValidationError';
    this.field = field;
  }
}
