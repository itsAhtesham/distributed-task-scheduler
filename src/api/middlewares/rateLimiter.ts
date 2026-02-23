import type { Request, Response, NextFunction } from 'express';
import { redis } from '../../config/redis.js';
import { env } from '../../config/env.js';

export function rateLimiter(maxRequests?: number, windowMs?: number) {
  const limit = maxRequests ?? env.RATE_LIMIT_MAX;
  const window = windowMs ?? env.RATE_LIMIT_WINDOW_MS;

  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const clientId = (req.headers['x-api-key'] as string) || req.ip || 'unknown';
    const windowTimestamp = Math.floor(Date.now() / window);
    const key = `ratelimit:${clientId}:${windowTimestamp}`;

    try {
      const multi = redis.multi();
      multi.incr(key);
      multi.pexpire(key, window);
      const results = await multi.exec();

      const count = results?.[0]?.[1] as number;

      // Set rate limit headers
      res.set('X-RateLimit-Limit', String(limit));
      res.set('X-RateLimit-Remaining', String(Math.max(0, limit - count)));
      res.set('X-RateLimit-Reset', String(Math.ceil((windowTimestamp + 1) * window / 1000)));

      if (count > limit) {
        const retryAfter = Math.ceil(window / 1000);
        res.set('Retry-After', String(retryAfter));
        res.status(429).json({
          success: false,
          error: {
            code: 'RATE_LIMIT_EXCEEDED',
            message: `Rate limit exceeded. Try again in ${retryAfter} seconds.`,
          },
        });
        return;
      }

      next();
    } catch (err) {
      // If Redis fails, allow the request through (fail open)
      next();
    }
  };
}
