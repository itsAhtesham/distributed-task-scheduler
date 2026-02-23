import { readTasks, acknowledgeTask, enqueueTask, moveToDLQ } from '../services/queueService.js';
import { dispatchWebhook } from '../services/webhookService.js';
import * as jobQueries from '../db/queries/jobs.queries.js';
import { generateId } from '../utils/idGenerator.js';
import { logger } from '../utils/logger.js';
import type { TaskMessage } from '../types/common.types.js';

// Job handler registry
const handlers: Record<string, (payload: Record<string, unknown>) => Promise<unknown>> = {
  'send-email': async (payload) => {
    logger.info({ to: payload.to }, 'Simulating email send');
    await new Promise(resolve => setTimeout(resolve, 500));
    return { sent: true, to: payload.to };
  },

  'process-image': async (payload) => {
    logger.info({ url: payload.url }, 'Simulating image processing');
    await new Promise(resolve => setTimeout(resolve, 1000));
    return { processed: true, url: payload.url };
  },

  'generate-report': async (payload) => {
    logger.info({ type: payload.type }, 'Simulating report generation');
    await new Promise(resolve => setTimeout(resolve, 2000));
    return { generated: true, type: payload.type };
  },

  'http-request': async (payload) => {
    const url = payload.url as string;
    const options = (payload.options || {}) as RequestInit;

    const response = await fetch(url, options);
    const body = await response.text();

    let parsed: unknown;
    try {
      parsed = JSON.parse(body);
    } catch {
      parsed = body;
    }

    return { status: response.status, body: parsed };
  },
};

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`Task timed out after ${timeoutMs}ms`)), timeoutMs)
    ),
  ]);
}

export class TaskConsumer {
  private workerId: string;
  private isRunning: boolean = false;
  private inProgressTasks: Set<string> = new Set();

  constructor(workerId: string) {
    this.workerId = workerId;
  }

  async start(): Promise<void> {
    this.isRunning = true;
    logger.info({ workerId: this.workerId }, 'Task consumer started');

    while (this.isRunning) {
      try {
        const tasks = await readTasks(this.workerId, 5, 2000);

        for (const task of tasks) {
          this.inProgressTasks.add(task.id);
          this.processTask(task.id, task.message).finally(() => {
            this.inProgressTasks.delete(task.id);
          });
        }
      } catch (err) {
        logger.error({ err }, 'Error reading tasks from queue');
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  stop(): void {
    this.isRunning = false;
    logger.info({ workerId: this.workerId }, 'Task consumer stopping');
  }

  async drain(): Promise<void> {
    // Wait for in-progress tasks to complete
    const maxWait = 30000;
    const start = Date.now();

    while (this.inProgressTasks.size > 0 && Date.now() - start < maxWait) {
      logger.info({ count: this.inProgressTasks.size }, 'Waiting for in-progress tasks to complete');
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    if (this.inProgressTasks.size > 0) {
      logger.warn({ count: this.inProgressTasks.size }, 'Force stopping with tasks still in progress');
    }
  }

  private async processTask(messageId: string, message: TaskMessage): Promise<void> {
    const handler = handlers[message.handler];

    if (!handler) {
      logger.error({ handler: message.handler, jobId: message.job_id }, 'Unknown handler');
      await acknowledgeTask(messageId);
      await jobQueries.updateJobStatus(message.job_id, 'failed');
      return;
    }

    // Create job run record
    const runId = generateId();
    try {
      await jobQueries.insertJobRun(runId, message.job_id, this.workerId, message.attempt);
    } catch (err) {
      logger.error({ err, jobId: message.job_id }, 'Failed to create job run record');
    }

    try {
      const payload = JSON.parse(message.payload) as Record<string, unknown>;
      const result = await withTimeout(handler(payload), message.timeout_ms);

      // Success
      await acknowledgeTask(messageId);
      await jobQueries.updateJobRun(runId, 'completed', undefined, result as Record<string, unknown>);

      // Mark one-time jobs as completed
      try {
        const job = await jobQueries.findJobById(message.job_id);
        if (job && job.type === 'once') {
          await jobQueries.updateJobStatus(message.job_id, 'completed');
        }
      } catch { /* non-critical */ }

      await dispatchWebhook('completed', message.job_id, runId, result as Record<string, unknown>);

      logger.info({ jobId: message.job_id, runId, attempt: message.attempt }, 'Task completed successfully');
    } catch (err) {
      const error = err as Error;
      const errorMessage = error.message || 'Unknown error';

      logger.error({ jobId: message.job_id, attempt: message.attempt, error: errorMessage }, 'Task execution failed');

      await acknowledgeTask(messageId);

      if (message.attempt < message.max_retries) {
        // Retry
        await jobQueries.updateJobRun(runId, 'retrying', errorMessage);
        await enqueueTask({
          ...message,
          attempt: message.attempt + 1,
          enqueued_at: new Date().toISOString(),
        });
        await dispatchWebhook('retrying', message.job_id, runId, { error: errorMessage, attempt: message.attempt });
      } else {
        // Move to DLQ
        const isTimeout = errorMessage.includes('timed out');
        await jobQueries.updateJobRun(runId, isTimeout ? 'timed_out' : 'failed', errorMessage);
        await jobQueries.updateJobStatus(message.job_id, 'failed');
        await moveToDLQ(message, errorMessage);
        await dispatchWebhook('failed', message.job_id, runId, { error: errorMessage, attempt: message.attempt });
      }
    }
  }
}
