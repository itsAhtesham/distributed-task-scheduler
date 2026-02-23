import { z } from 'zod';

const envSchema = z.object({
  // Database
  DATABASE_URL: z.string().url(),

  // Redis
  REDIS_URL: z.string().url(),

  // API
  PORT: z.coerce.number().default(3000),
  API_KEY: z.string().min(1),
  RATE_LIMIT_MAX: z.coerce.number().default(100),
  RATE_LIMIT_WINDOW_MS: z.coerce.number().default(60000),

  // Worker
  WORKER_ID: z.string().optional(),
  LEADER_LOCK_TTL_MS: z.coerce.number().default(15000),
  LEADER_RENEW_INTERVAL_MS: z.coerce.number().default(5000),
  HEARTBEAT_INTERVAL_MS: z.coerce.number().default(5000),
  SCHEDULER_POLL_INTERVAL_MS: z.coerce.number().default(10000),

  // Webhook
  WEBHOOK_TIMEOUT_MS: z.coerce.number().default(5000),
  WEBHOOK_MAX_RETRIES: z.coerce.number().default(3),

  // Node env
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
});

export type Env = z.infer<typeof envSchema>;

function loadEnv(): Env {
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    console.error('Invalid environment variables:');
    console.error(result.error.format());
    process.exit(1);
  }
  return result.data;
}

export const env = loadEnv();
