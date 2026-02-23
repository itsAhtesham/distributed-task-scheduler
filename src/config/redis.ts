import { Redis } from 'ioredis';
import { env } from './env.js';
import { logger } from '../utils/logger.js';

export function createRedisClient(name: string = 'default'): Redis {
  const client = new Redis(env.REDIS_URL, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    retryStrategy(times: number) {
      const delay = Math.min(times * 200, 5000);
      return delay;
    },
  });

  client.on('connect', () => {
    logger.info({ client: name }, 'Redis client connected');
  });

  client.on('error', (err: Error) => {
    logger.error({ err, client: name }, 'Redis client error');
  });

  return client;
}

export const redis = createRedisClient('main');

export async function checkRedisConnection(): Promise<boolean> {
  try {
    const result = await redis.ping();
    return result === 'PONG';
  } catch (err) {
    logger.error({ err }, 'Redis connection check failed');
    return false;
  }
}

export async function closeRedisConnection(): Promise<void> {
  await redis.quit();
  logger.info('Redis connection closed');
}
