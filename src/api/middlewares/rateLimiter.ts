import type { Request, Response, NextFunction } from 'express';
import { redis } from '../../config/redis.js';
import { env } from '../../config/env.js';

const INCR_WITH_EXPIRY_SCRIPT = `
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return current
`;

export function rateLimiter(maxRequests?: number, windowMs?: number) {
  const limit = maxRequests ?? env.RATE_LIMIT_MAX;
  const window = windowMs ?? env.RATE_LIMIT_WINDOW_MS;

  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const clientId = (req.headers['x-api-key'] as string) || req.ip || 'unknown';
    const windowTimestamp = Math.floor(Date.now() / window);
    const key = `ratelimit:${clientId}:${windowTimestamp}`;

    try {
      const count = await redis.eval(INCR_WITH_EXPIRY_SCRIPT, 1, key, window) as number;

      const resetTime = (windowTimestamp + 1) * window;
      const currTime = Date.now();
      const retryAfter = Math.max(0, Math.ceil((resetTime - currTime) / 1000));

      // Set rate limit headers
      res.set('X-RateLimit-Limit', String(limit));
      res.set('X-RateLimit-Remaining', String(Math.max(0, limit - count)));
      res.set('X-RateLimit-Reset', String(Math.ceil(resetTime / 1000)));

      if (count > limit) {
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
