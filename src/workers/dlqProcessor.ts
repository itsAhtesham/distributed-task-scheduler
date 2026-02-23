import { readDLQMessages } from '../services/queueService.js';
import { logger } from '../utils/logger.js';

export class DLQProcessor {
  private pollInterval: ReturnType<typeof setInterval> | null = null;
  private isRunning: boolean = false;

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info('DLQ processor started');

    this.pollInterval = setInterval(async () => {
      try {
        const messages = await readDLQMessages(20);

        if (messages.length > 0) {
          logger.info({ count: messages.length }, 'DLQ messages found');

          for (const msg of messages) {
            logger.warn({
              messageId: msg.id,
              jobId: msg.fields.job_id,
              handler: msg.fields.handler,
              error: msg.fields.error,
              attempt: msg.fields.attempt,
              failedAt: msg.fields.failed_at,
            }, 'Dead letter queue entry');
          }
        }
      } catch (err) {
        logger.error({ err }, 'DLQ processor poll failed');
      }
    }, 30000); // Check every 30 seconds
  }

  stop(): void {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    this.isRunning = false;
    logger.info('DLQ processor stopped');
  }
}
