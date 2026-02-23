import type { Request, Response, NextFunction } from 'express';
import * as jobService from '../../services/jobService.js';
import type { JobFilters } from '../../types/job.types.js';

export async function createJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.createJob(req.body);
    res.status(201).json({ success: true, data: job });
  } catch (err) {
    next(err);
  }
}

export async function getJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.getJob(req.params.id as string);
    res.json({ success: true, data: job });
  } catch (err) {
    next(err);
  }
}

export async function listJobs(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const filters: JobFilters = {};
    if (req.query.status) filters.status = req.query.status as JobFilters['status'];
    if (req.query.type) filters.type = req.query.type as JobFilters['type'];
    if (req.query.handler) filters.handler = req.query.handler as string;

    const result = await jobService.listJobs(filters, req.pagination);
    res.json({ success: true, ...result });
  } catch (err) {
    next(err);
  }
}

export async function updateJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const job = await jobService.updateJob(req.params.id as string, req.body);
    res.json({ success: true, data: job });
  } catch (err) {
    next(err);
  }
}

export async function deleteJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    await jobService.deleteJob(req.params.id as string);
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

export async function triggerJob(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const jobRun = await jobService.triggerJob(req.params.id as string);
    res.status(202).json({ success: true, data: jobRun });
  } catch (err) {
    next(err);
  }
}

export async function getJobRuns(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const result = await jobService.getJobRuns(req.params.id as string, req.pagination);
    res.json({ success: true, ...result });
  } catch (err) {
    next(err);
  }
}
