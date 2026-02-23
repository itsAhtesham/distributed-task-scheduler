import type { Request, Response, NextFunction } from 'express';
import { generateId } from '../../utils/idGenerator.js';
import * as webhookQueries from '../../db/queries/webhooks.queries.js';

export async function createWebhook(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const id = generateId();
    const webhook = await webhookQueries.insertWebhook(id, req.body);
    res.status(201).json({ success: true, data: webhook });
  } catch (err) {
    next(err);
  }
}

export async function listWebhooks(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const result = await webhookQueries.listWebhooks(req.pagination);
    res.json({ success: true, ...result });
  } catch (err) {
    next(err);
  }
}

export async function deleteWebhook(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const deleted = await webhookQueries.deleteWebhook(req.params.id as string);
    if (!deleted) {
      res.status(404).json({
        success: false,
        error: { code: 'NOT_FOUND', message: `Webhook ${req.params.id} not found` },
      });
      return;
    }
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}
