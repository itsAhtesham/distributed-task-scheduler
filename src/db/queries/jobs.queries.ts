import { pool } from '../../config/database.js';
import type { Job, JobRun, JobFilters, CreateJobInput, UpdateJobInput } from '../../types/job.types.js';
import type { PaginationParams, PaginatedResult } from '../../types/common.types.js';

export async function insertJob(id: string, input: CreateJobInput, nextRunAt: Date | null): Promise<Job> {
  const result = await pool.query(
    `INSERT INTO jobs (id, name, description, type, cron_expression, scheduled_at, payload, handler, max_retries, timeout_ms, priority, metadata, next_run_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
     RETURNING *`,
    [
      id,
      input.name,
      input.description || null,
      input.type,
      input.cron_expression || null,
      input.scheduled_at || null,
      JSON.stringify(input.payload),
      input.handler,
      input.max_retries,
      input.timeout_ms,
      input.priority,
      JSON.stringify(input.metadata),
      nextRunAt,
    ]
  );
  return result.rows[0] as Job;
}

export async function findJobById(id: string): Promise<Job | null> {
  const result = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  return (result.rows[0] as Job) || null;
}

export async function listJobs(
  filters: JobFilters,
  pagination: PaginationParams
): Promise<PaginatedResult<Job>> {
  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIndex = 1;

  if (filters.status) {
    conditions.push(`status = $${paramIndex++}`);
    params.push(filters.status);
  }
  if (filters.type) {
    conditions.push(`type = $${paramIndex++}`);
    params.push(filters.type);
  }
  if (filters.handler) {
    conditions.push(`handler = $${paramIndex++}`);
    params.push(filters.handler);
  }
  if (pagination.cursor) {
    conditions.push(`id > $${paramIndex++}`);
    params.push(pagination.cursor);
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const limit = pagination.limit + 1;

  const result = await pool.query(
    `SELECT * FROM jobs ${whereClause} ORDER BY id ASC LIMIT $${paramIndex}`,
    [...params, limit]
  );

  const rows = result.rows as Job[];
  const hasMore = rows.length > pagination.limit;
  const data = hasMore ? rows.slice(0, pagination.limit) : rows;

  return {
    data,
    pagination: {
      next_cursor: hasMore ? data[data.length - 1].id : null,
      has_more: hasMore,
      limit: pagination.limit,
    },
  };
}

export async function updateJob(id: string, updates: UpdateJobInput & { next_run_at?: Date | null }): Promise<Job | null> {
  const setClauses: string[] = ['updated_at = NOW()'];
  const params: unknown[] = [];
  let paramIndex = 1;

  const fields: Record<string, unknown> = { ...updates };
  if (fields.payload) fields.payload = JSON.stringify(fields.payload);
  if (fields.metadata) fields.metadata = JSON.stringify(fields.metadata);

  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined) {
      setClauses.push(`${key} = $${paramIndex++}`);
      params.push(value);
    }
  }

  params.push(id);

  const result = await pool.query(
    `UPDATE jobs SET ${setClauses.join(', ')} WHERE id = $${paramIndex} RETURNING *`,
    params
  );
  return (result.rows[0] as Job) || null;
}

export async function deleteJob(id: string): Promise<boolean> {
  const result = await pool.query('DELETE FROM jobs WHERE id = $1', [id]);
  return (result.rowCount ?? 0) > 0;
}

export async function findDueCronJobs(): Promise<Job[]> {
  const result = await pool.query(
    `SELECT * FROM jobs WHERE type = 'cron' AND status = 'active' AND next_run_at <= NOW() ORDER BY priority DESC, next_run_at ASC`
  );
  return result.rows as Job[];
}

export async function updateJobNextRun(id: string, nextRunAt: Date): Promise<void> {
  await pool.query(
    `UPDATE jobs SET next_run_at = $1, last_run_at = NOW(), updated_at = NOW() WHERE id = $2`,
    [nextRunAt, id]
  );
}

export async function updateJobStatus(id: string, status: string): Promise<void> {
  await pool.query(
    `UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2`,
    [status, id]
  );
}

// Job runs queries
export async function insertJobRun(
  id: string,
  jobId: string,
  workerId: string,
  attempt: number
): Promise<JobRun> {
  const result = await pool.query(
    `INSERT INTO job_runs (id, job_id, worker_id, attempt) VALUES ($1, $2, $3, $4) RETURNING *`,
    [id, jobId, workerId, attempt]
  );
  return result.rows[0] as JobRun;
}

export async function updateJobRun(
  id: string,
  status: string,
  errorMessage?: string,
  resultData?: Record<string, unknown>
): Promise<void> {
  await pool.query(
    `UPDATE job_runs SET status = $1, completed_at = NOW(), duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000, error_message = $2, result = $3 WHERE id = $4`,
    [status, errorMessage || null, resultData ? JSON.stringify(resultData) : null, id]
  );
}

export async function getJobRuns(
  jobId: string,
  pagination: PaginationParams
): Promise<PaginatedResult<JobRun>> {
  const params: unknown[] = [jobId];
  let paramIndex = 2;
  let cursorClause = '';

  if (pagination.cursor) {
    cursorClause = `AND id > $${paramIndex++}`;
    params.push(pagination.cursor);
  }

  const limit = pagination.limit + 1;
  params.push(limit);

  const result = await pool.query(
    `SELECT * FROM job_runs WHERE job_id = $1 ${cursorClause} ORDER BY id ASC LIMIT $${paramIndex}`,
    params
  );

  const rows = result.rows as JobRun[];
  const hasMore = rows.length > pagination.limit;
  const data = hasMore ? rows.slice(0, pagination.limit) : rows;

  return {
    data,
    pagination: {
      next_cursor: hasMore ? data[data.length - 1].id : null,
      has_more: hasMore,
      limit: pagination.limit,
    },
  };
}
