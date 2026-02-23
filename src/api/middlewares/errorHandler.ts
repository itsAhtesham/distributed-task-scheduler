import type { Request, Response, NextFunction } from 'express';
import { logger } from '../../utils/logger.js';
import { NotFoundError, ValidationError } from '../../services/jobService.js';

export function errorHandler(err: Error, _req: Request, res: Response, _next: NextFunction): void {
  logger.error({ err, stack: err.stack }, 'Unhandled error');

  if (err instanceof NotFoundError) {
    res.status(404).json({
      success: false,
      error: {
        code: 'NOT_FOUND',
        message: err.message,
      },
    });
    return;
  }

  if (err instanceof ValidationError) {
    res.status(400).json({
      success: false,
      error: {
        code: 'VALIDATION_ERROR',
        message: err.message,
        details: [{ field: err.field, message: err.message }],
      },
    });
    return;
  }

  res.status(500).json({
    success: false,
    error: {
      code: 'INTERNAL_ERROR',
      message: 'An unexpected error occurred',
    },
  });
}
