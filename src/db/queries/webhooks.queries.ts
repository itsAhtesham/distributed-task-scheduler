import { pool } from '../../config/database.js';
import type { Webhook, CreateWebhookInput } from '../../types/webhook.types.js';
import type { PaginationParams, PaginatedResult } from '../../types/common.types.js';

export async function insertWebhook(id: string, input: CreateWebhookInput): Promise<Webhook> {
  const result = await pool.query(
    `INSERT INTO webhooks (id, job_id, url, events, secret) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
    [id, input.job_id || null, input.url, input.events, input.secret || null]
  );
  return result.rows[0] as Webhook;
}

export async function findWebhookById(id: string): Promise<Webhook | null> {
  const result = await pool.query('SELECT * FROM webhooks WHERE id = $1', [id]);
  return (result.rows[0] as Webhook) || null;
}

export async function listWebhooks(pagination: PaginationParams): Promise<PaginatedResult<Webhook>> {
  const params: unknown[] = [];
  let paramIndex = 1;
  let cursorClause = '';

  if (pagination.cursor) {
    cursorClause = `WHERE id > $${paramIndex++}`;
    params.push(pagination.cursor);
  }

  const limit = pagination.limit + 1;
  params.push(limit);

  const result = await pool.query(
    `SELECT * FROM webhooks ${cursorClause} ORDER BY id ASC LIMIT $${paramIndex}`,
    params
  );

  const rows = result.rows as Webhook[];
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

export async function deleteWebhook(id: string): Promise<boolean> {
  const result = await pool.query('DELETE FROM webhooks WHERE id = $1', [id]);
  return (result.rowCount ?? 0) > 0;
}

export async function findWebhooksForJob(jobId: string, event: string): Promise<Webhook[]> {
  const result = await pool.query(
    `SELECT * FROM webhooks WHERE (job_id = $1 OR job_id IS NULL) AND is_active = true AND $2 = ANY(events)`,
    [jobId, event]
  );
  return result.rows as Webhook[];
}
