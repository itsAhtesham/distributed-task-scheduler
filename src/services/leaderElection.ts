import { redis } from '../config/redis.js';
import { env } from '../config/env.js';
import { logger } from '../utils/logger.js';

const LEADER_LOCK_KEY = 'leader_lock';

// Lua script for atomic check-and-delete (only delete if we own the lock)
const RELEASE_LOCK_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

export class LeaderElection {
  private workerId: string;
  private _isLeader: boolean = false;
  private renewInterval: ReturnType<typeof setInterval> | null = null;

  constructor(workerId: string) {
    this.workerId = workerId;
  }

  async tryAcquireLeadership(): Promise<boolean> {
    try {
      const result = await redis.set(
        LEADER_LOCK_KEY,
        this.workerId,
        'PX',
        env.LEADER_LOCK_TTL_MS,
        'NX'
      );

      if (result === 'OK') {
        this._isLeader = true;
        logger.info({ workerId: this.workerId }, 'Acquired leadership');
        return true;
      }

      // Check if we already hold the lock
      const currentLeader = await redis.get(LEADER_LOCK_KEY);
      if (currentLeader === this.workerId) {
        this._isLeader = true;
        return true;
      }

      this._isLeader = false;
      return false;
    } catch (err) {
      logger.error({ err, workerId: this.workerId }, 'Failed to acquire leadership');
      this._isLeader = false;
      return false;
    }
  }

  async renewLeadership(): Promise<boolean> {
    if (!this._isLeader) return false;

    try {
      // Verify we still own the lock before renewing
      const currentLeader = await redis.get(LEADER_LOCK_KEY);
      if (currentLeader !== this.workerId) {
        this._isLeader = false;
        logger.warn({ workerId: this.workerId }, 'Lost leadership (lock owned by another worker)');
        return false;
      }

      await redis.pexpire(LEADER_LOCK_KEY, env.LEADER_LOCK_TTL_MS);
      return true;
    } catch (err) {
      logger.error({ err, workerId: this.workerId }, 'Failed to renew leadership');
      this._isLeader = false;
      return false;
    }
  }

  async releaseLeadership(): Promise<void> {
    try {
      await redis.eval(RELEASE_LOCK_SCRIPT, 1, LEADER_LOCK_KEY, this.workerId);
      this._isLeader = false;
      logger.info({ workerId: this.workerId }, 'Released leadership');
    } catch (err) {
      logger.error({ err, workerId: this.workerId }, 'Failed to release leadership');
    }
  }

  isLeader(): boolean {
    return this._isLeader;
  }

  startElectionLoop(): void {
    // Try to acquire leadership immediately
    this.tryAcquireLeadership();

    this.renewInterval = setInterval(async () => {
      if (this._isLeader) {
        const renewed = await this.renewLeadership();
        if (!renewed) {
          // Lost leadership, try to re-acquire
          await this.tryAcquireLeadership();
        }
      } else {
        await this.tryAcquireLeadership();
      }
    }, env.LEADER_RENEW_INTERVAL_MS);
  }

  stopElectionLoop(): void {
    if (this.renewInterval) {
      clearInterval(this.renewInterval);
      this.renewInterval = null;
    }
  }
}
