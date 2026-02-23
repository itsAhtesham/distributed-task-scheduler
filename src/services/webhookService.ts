import crypto from 'node:crypto';
import { findWebhooksForJob } from '../db/queries/webhooks.queries.js';
import { env } from '../config/env.js';
import { logger } from '../utils/logger.js';
import type { WebhookEvent } from '../types/webhook.types.js';

interface WebhookPayload {
  event: WebhookEvent;
  job_id: string;
  job_run_id: string;
  timestamp: string;
  data: Record<string, unknown>;
}

function signPayload(payload: string, secret: string): string {
  return crypto.createHmac('sha256', secret).update(payload).digest('hex');
}

async function sendWithRetry(
  url: string,
  body: string,
  headers: Record<string, string>,
  retries: number = env.WEBHOOK_MAX_RETRIES
): Promise<void> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), env.WEBHOOK_TIMEOUT_MS);

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...headers,
        },
        body,
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (response.ok) {
        logger.debug({ url, attempt }, 'Webhook delivered successfully');
        return;
      }

      logger.warn({ url, status: response.status, attempt }, 'Webhook delivery failed with status');
    } catch (err) {
      logger.warn({ url, err, attempt }, 'Webhook delivery failed');
    }

    if (attempt < retries) {
      // Exponential backoff: 1s, 2s, 4s
      const delay = Math.pow(2, attempt) * 1000;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  logger.error({ url }, 'Webhook delivery failed after all retries');
}

export async function dispatchWebhook(
  event: WebhookEvent,
  jobId: string,
  jobRunId: string,
  data: Record<string, unknown> = {}
): Promise<void> {
  try {
    const webhooks = await findWebhooksForJob(jobId, event);

    if (webhooks.length === 0) return;

    const payload: WebhookPayload = {
      event,
      job_id: jobId,
      job_run_id: jobRunId,
      timestamp: new Date().toISOString(),
      data,
    };

    const body = JSON.stringify(payload);

    // Fire-and-forget for each webhook
    for (const webhook of webhooks) {
      const headers: Record<string, string> = {};

      if (webhook.secret) {
        headers['X-Signature-256'] = `sha256=${signPayload(body, webhook.secret)}`;
      }

      // Don't await - fire and forget
      sendWithRetry(webhook.url, body, headers).catch((err) => {
        logger.error({ err, webhookId: webhook.id }, 'Unhandled webhook dispatch error');
      });
    }
  } catch (err) {
    logger.error({ err, event, jobId }, 'Failed to dispatch webhooks');
  }
}
