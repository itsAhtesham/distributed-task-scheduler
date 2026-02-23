import type { Request, Response, NextFunction } from 'express';
import type { PaginationParams } from '../../types/common.types.js';

declare global {
  namespace Express {
    interface Request {
      pagination: PaginationParams;
    }
  }
}

export function pagination(defaultLimit: number = 20, maxLimit: number = 100) {
  return (req: Request, _res: Response, next: NextFunction): void => {
    const cursor = req.query.cursor as string | undefined;
    let limit = parseInt(req.query.limit as string, 10);

    if (isNaN(limit) || limit < 1) {
      limit = defaultLimit;
    } else if (limit > maxLimit) {
      limit = maxLimit;
    }

    req.pagination = { cursor, limit };
    next();
  };
}
