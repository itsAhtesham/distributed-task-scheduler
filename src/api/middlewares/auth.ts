import type { Request, Response, NextFunction } from 'express';
import { env } from '../../config/env.js';

export function authenticate(req: Request, res: Response, next: NextFunction): void {
  const apiKey = req.headers['x-api-key'] as string;

  if (!apiKey) {
    res.status(401).json({
      success: false,
      error: {
        code: 'UNAUTHORIZED',
        message: 'Missing API key. Provide it via X-API-Key header.',
      },
    });
    return;
  }

  if (apiKey !== env.API_KEY) {
    res.status(403).json({
      success: false,
      error: {
        code: 'FORBIDDEN',
        message: 'Invalid API key.',
      },
    });
    return;
  }

  next();
}
