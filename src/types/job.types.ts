import { z } from 'zod';

export const JobType = z.enum(['cron', 'once', 'delayed']);
export type JobType = z.infer<typeof JobType>;

export const JobStatus = z.enum(['active', 'paused', 'completed', 'failed', 'cancelled']);
export type JobStatus = z.infer<typeof JobStatus>;

export const JobRunStatus = z.enum(['running', 'completed', 'failed', 'timed_out', 'retrying']);
export type JobRunStatus = z.infer<typeof JobRunStatus>;

export interface Job {
  id: string;
  name: string;
  description: string | null;
  type: JobType;
  cron_expression: string | null;
  scheduled_at: Date | null;
  payload: Record<string, unknown>;
  handler: string;
  status: JobStatus;
  max_retries: number;
  retry_count: number;
  timeout_ms: number;
  priority: number;
  metadata: Record<string, unknown>;
  created_at: Date;
  updated_at: Date;
  next_run_at: Date | null;
  last_run_at: Date | null;
}

export interface JobRun {
  id: string;
  job_id: string;
  worker_id: string;
  status: JobRunStatus;
  started_at: Date;
  completed_at: Date | null;
  duration_ms: number | null;
  attempt: number;
  error_message: string | null;
  result: Record<string, unknown> | null;
  created_at: Date;
}

export const CreateJobSchema = z.object({
  name: z.string().min(1).max(255),
  description: z.string().optional(),
  type: JobType,
  cron_expression: z.string().optional(),
  scheduled_at: z.string().datetime().optional(),
  payload: z.record(z.unknown()).default({}),
  handler: z.string().min(1).max(255),
  max_retries: z.number().int().min(0).max(10).default(3),
  timeout_ms: z.number().int().min(1000).max(300000).default(30000),
  priority: z.number().int().min(0).max(100).default(0),
  metadata: z.record(z.unknown()).default({}),
}).refine(
  (data) => {
    if (data.type === 'cron' && !data.cron_expression) return false;
    if ((data.type === 'once' || data.type === 'delayed') && !data.scheduled_at && data.type === 'delayed') return false;
    return true;
  },
  { message: 'Cron jobs require cron_expression; delayed jobs require scheduled_at' }
);

export type CreateJobInput = z.infer<typeof CreateJobSchema>;

export const UpdateJobSchema = z.object({
  name: z.string().min(1).max(255).optional(),
  description: z.string().optional(),
  status: z.enum(['active', 'paused', 'cancelled']).optional(),
  cron_expression: z.string().optional(),
  payload: z.record(z.unknown()).optional(),
  max_retries: z.number().int().min(0).max(10).optional(),
  timeout_ms: z.number().int().min(1000).max(300000).optional(),
  priority: z.number().int().min(0).max(100).optional(),
  metadata: z.record(z.unknown()).optional(),
});

export type UpdateJobInput = z.infer<typeof UpdateJobSchema>;

export interface JobFilters {
  status?: JobStatus;
  type?: JobType;
  handler?: string;
}
