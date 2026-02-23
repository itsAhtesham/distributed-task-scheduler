import { z } from 'zod';

export const WebhookEvent = z.enum(['completed', 'failed', 'retrying']);
export type WebhookEvent = z.infer<typeof WebhookEvent>;

export interface Webhook {
  id: string;
  job_id: string | null;
  url: string;
  events: WebhookEvent[];
  secret: string | null;
  is_active: boolean;
  created_at: Date;
}

export const CreateWebhookSchema = z.object({
  job_id: z.string().optional(),
  url: z.string().url().max(2048),
  events: z.array(WebhookEvent).min(1),
  secret: z.string().max(255).optional(),
});

export type CreateWebhookInput = z.infer<typeof CreateWebhookSchema>;
