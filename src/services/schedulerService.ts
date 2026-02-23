import { findDueCronJobs, updateJobNextRun } from '../db/queries/jobs.queries.js';
import { enqueueTask } from './queueService.js';
import { getNextRunDate } from '../utils/cronParser.js';
import { pool } from '../config/database.js';
import { env } from '../config/env.js';
import { logger } from '../utils/logger.js';

export class SchedulerService {
  private pollInterval: ReturnType<typeof setInterval> | null = null;
  private isRunning: boolean = false;

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    logger.info('Scheduler service started');
    this.poll(); // Initial poll

    this.pollInterval = setInterval(() => {
      this.poll();
    }, env.SCHEDULER_POLL_INTERVAL_MS);
  }

  stop(): void {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    this.isRunning = false;
    logger.info('Scheduler service stopped');
  }

  private async poll(): Promise<void> {
    try {
      const dueJobs = await findDueCronJobs();

      if (dueJobs.length > 0) {
        logger.info({ count: dueJobs.length }, 'Found due cron jobs');
      }

      for (const job of dueJobs) {
        try {
          // Use PostgreSQL advisory lock per job to prevent double-scheduling
          const lockResult = await pool.query(
            'SELECT pg_try_advisory_xact_lock($1)',
            [hashJobId(job.id)]
          );

          const acquired = lockResult.rows[0]?.pg_try_advisory_xact_lock;
          if (!acquired) {
            logger.debug({ jobId: job.id }, 'Could not acquire advisory lock, skipping');
            continue;
          }

          // Enqueue the task
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

          // Calculate and update next run
          if (job.cron_expression) {
            const nextRun = getNextRunDate(job.cron_expression);
            await updateJobNextRun(job.id, nextRun);
          }

          logger.info({ jobId: job.id, name: job.name }, 'Cron job scheduled');
        } catch (err) {
          logger.error({ err, jobId: job.id }, 'Failed to schedule cron job');
        }
      }
    } catch (err) {
      logger.error({ err }, 'Scheduler poll failed');
    }
  }
}

// Hash a ULID string to a numeric value for pg advisory lock
function hashJobId(id: string): number {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    const char = id.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash |= 0; // Convert to 32-bit integer
  }
  return Math.abs(hash);
}
