import { readDLQFromGroup, acknowledgeDLQMessage, purgeDLQOlderThan } from '../services/queueService.js';
import { env } from '../config/env.js';
import { logger } from '../utils/logger.js';

const DLQ_CONSUMER_ID = 'dlq-processor';

export class DLQProcessor {
  private pollInterval: ReturnType<typeof setInterval> | null = null;
  private isRunning: boolean = false;
  private inProgressMessages: Set<string> = new Set();

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info({ workerId: DLQ_CONSUMER_ID }, 'DLQ processor started');

    this.pollInterval = setInterval(async () => {
      try {
        const messages = await readDLQFromGroup(DLQ_CONSUMER_ID, env.DLQ_BATCH_SIZE, 0);

        for (const msg of messages) {
          this.inProgressMessages.add(msg.id);
          this.processMessage(msg.id, msg.fields).finally(() => {
            this.inProgressMessages.delete(msg.id);
          });
        }

        // Retention cleanup
        const maxAgeMs = env.DLQ_RETENTION_HOURS * 60 * 60 * 1000;
        const purged = await purgeDLQOlderThan(maxAgeMs);
        if (purged > 0) {
          logger.info({ purged }, 'Purged expired DLQ messages');
        }
      } catch (err) {
        logger.error({ err }, 'DLQ processor poll failed');
      }
    }, env.DLQ_POLL_INTERVAL_MS);
  }

  stop(): void {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    this.isRunning = false;
    logger.info({ workerId: DLQ_CONSUMER_ID }, 'DLQ processor stopped');
  }

  async drain(): Promise<void> {
    const maxWait = 30000;
    const start = Date.now();

    while (this.inProgressMessages.size > 0 && Date.now() - start < maxWait) {
      logger.info({ count: this.inProgressMessages.size }, 'Waiting for in-progress DLQ messages to complete');
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    if (this.inProgressMessages.size > 0) {
      logger.warn({ count: this.inProgressMessages.size }, 'Force stopping with DLQ messages still in progress');
    }
  }

  private async processMessage(messageId: string, fields: Record<string, string>): Promise<void> {
    logger.warn({
      messageId,
      jobId: fields.job_id,
      handler: fields.handler,
      error: fields.error,
      attempt: fields.attempt,
      failedAt: fields.failed_at,
    }, 'Dead letter queue entry');

    await acknowledgeDLQMessage(messageId);
  }
}
