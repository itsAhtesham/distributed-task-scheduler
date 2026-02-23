import { redis } from '../config/redis.js';
import { env } from '../config/env.js';
import { logger } from '../utils/logger.js';

export class Heartbeat {
  private workerId: string;
  private interval: ReturnType<typeof setInterval> | null = null;

  constructor(workerId: string) {
    this.workerId = workerId;
  }

  start(): void {
    this.sendHeartbeat(); // Send immediately

    this.interval = setInterval(() => {
      this.sendHeartbeat();
    }, env.HEARTBEAT_INTERVAL_MS);

    logger.info({ workerId: this.workerId }, 'Heartbeat started');
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    logger.info({ workerId: this.workerId }, 'Heartbeat stopped');
  }

  private async sendHeartbeat(): Promise<void> {
    try {
      await redis.set(
        `worker:${this.workerId}:heartbeat`,
        new Date().toISOString(),
        'PX',
        env.LEADER_LOCK_TTL_MS // 15s TTL, same as leader lock
      );
    } catch (err) {
      logger.error({ err, workerId: this.workerId }, 'Failed to send heartbeat');
    }
  }

  static async getActiveWorkers(): Promise<Array<{ id: string; lastHeartbeat: string }>> {
    const keys = await redis.keys('worker:*:heartbeat');
    const workers: Array<{ id: string; lastHeartbeat: string }> = [];

    for (const key of keys) {
      const value = await redis.get(key);
      if (value) {
        const workerId = key.replace('worker:', '').replace(':heartbeat', '');
        workers.push({ id: workerId, lastHeartbeat: value });
      }
    }

    return workers;
  }
}
