import { redis } from '../config/redis.js';
import { logger } from '../utils/logger.js';
import type { TaskMessage } from '../types/common.types.js';

const TASK_QUEUE = 'task_queue';
const TASK_DLQ = 'task_dlq';
const CONSUMER_GROUP = 'workers';

export async function initializeStreams(): Promise<void> {
  try {
    await redis.xgroup('CREATE', TASK_QUEUE, CONSUMER_GROUP, '$', 'MKSTREAM');
    logger.info('Consumer group created for task_queue');
  } catch (err: unknown) {
    const error = err as Error;
    if (!error.message.includes('BUSYGROUP')) {
      throw err;
    }
    // Group already exists
  }

  try {
    await redis.xgroup('CREATE', TASK_DLQ, 'dlq_workers', '$', 'MKSTREAM');
    logger.info('Consumer group created for task_dlq');
  } catch (err: unknown) {
    const error = err as Error;
    if (!error.message.includes('BUSYGROUP')) {
      throw err;
    }
  }
}

export async function enqueueTask(message: TaskMessage): Promise<string> {
  const messageId = await redis.xadd(
    TASK_QUEUE,
    '*',
    'job_id', message.job_id,
    'payload', message.payload,
    'attempt', String(message.attempt),
    'priority', String(message.priority),
    'enqueued_at', message.enqueued_at,
    'timeout_ms', String(message.timeout_ms),
    'max_retries', String(message.max_retries),
    'handler', message.handler
  );

  logger.debug({ messageId, jobId: message.job_id }, 'Task enqueued');
  return messageId as string;
}

export async function moveToDLQ(message: TaskMessage, error: string): Promise<void> {
  await redis.xadd(
    TASK_DLQ,
    '*',
    'job_id', message.job_id,
    'payload', message.payload,
    'attempt', String(message.attempt),
    'priority', String(message.priority),
    'enqueued_at', message.enqueued_at,
    'timeout_ms', String(message.timeout_ms),
    'max_retries', String(message.max_retries),
    'handler', message.handler,
    'error', error,
    'failed_at', new Date().toISOString()
  );

  logger.warn({ jobId: message.job_id, error }, 'Task moved to DLQ');
}

export async function acknowledgeTask(messageId: string): Promise<void> {
  await redis.xack(TASK_QUEUE, CONSUMER_GROUP, messageId);
}

export async function readTasks(
  consumerId: string,
  count: number = 5,
  blockMs: number = 2000
): Promise<Array<{ id: string; message: TaskMessage }>> {
  const result = await redis.xreadgroup(
    'GROUP', CONSUMER_GROUP, consumerId,
    'COUNT', String(count),
    'BLOCK', String(blockMs),
    'STREAMS', TASK_QUEUE, '>'
  );

  if (!result) return [];

  const tasks: Array<{ id: string; message: TaskMessage }> = [];

  for (const [, messages] of result as Array<[string, Array<[string, string[]]>]>) {
    for (const [id, fields] of messages) {
      const fieldMap: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) {
        fieldMap[fields[i]] = fields[i + 1];
      }

      tasks.push({
        id,
        message: {
          job_id: fieldMap.job_id,
          payload: fieldMap.payload,
          attempt: parseInt(fieldMap.attempt, 10),
          priority: parseInt(fieldMap.priority, 10),
          enqueued_at: fieldMap.enqueued_at,
          timeout_ms: parseInt(fieldMap.timeout_ms, 10),
          max_retries: parseInt(fieldMap.max_retries, 10),
          handler: fieldMap.handler,
        },
      });
    }
  }

  return tasks;
}

const DLQ_CONSUMER_GROUP = 'dlq_workers';

export async function readDLQFromGroup(
  consumerId: string,
  count: number = 10,
  blockMs: number = 2000
): Promise<Array<{ id: string; fields: Record<string, string> }>> {
  const result = await redis.xreadgroup(
    'GROUP', DLQ_CONSUMER_GROUP, consumerId,
    'COUNT', String(count),
    'BLOCK', String(blockMs),
    'STREAMS', TASK_DLQ, '>'
  );

  if (!result) return [];

  const messages: Array<{ id: string; fields: Record<string, string> }> = [];

  for (const [, entries] of result as Array<[string, Array<[string, string[]]>]>) {
    for (const [id, fields] of entries) {
      const fieldMap: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) {
        fieldMap[fields[i]] = fields[i + 1];
      }
      messages.push({ id, fields: fieldMap });
    }
  }

  return messages;
}

export async function acknowledgeDLQMessage(messageId: string): Promise<void> {
  await redis.xack(TASK_DLQ, DLQ_CONSUMER_GROUP, messageId);
}

export async function purgeDLQOlderThan(maxAgeMs: number): Promise<number> {
  const cutoff = Date.now() - maxAgeMs;
  const result = await redis.xrange(TASK_DLQ, '-', '+');
  if (!result) return 0;

  let deleted = 0;
  for (const [id, fields] of result as Array<[string, string[]]>) {
    const fieldMap: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      fieldMap[fields[i]] = fields[i + 1];
    }
    if (fieldMap.failed_at) {
      const failedAt = new Date(fieldMap.failed_at).getTime();
      if (failedAt < cutoff) {
        await redis.xdel(TASK_DLQ, id);
        deleted++;
      }
    }
  }

  return deleted;
}

export async function readDLQMessages(count: number = 10): Promise<Array<{ id: string; fields: Record<string, string> }>> {
  const result = await redis.xrange(TASK_DLQ, '-', '+', 'COUNT', count);
  if (!result) return [];

  return result.map(([id, fields]: [string, string[]]) => {
    const fieldMap: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      fieldMap[fields[i]] = fields[i + 1];
    }
    return { id, fields: fieldMap };
  });
}

export async function replayDLQMessage(messageId: string): Promise<boolean> {
  const messages = await redis.xrange(TASK_DLQ, messageId, messageId);
  if (!messages || messages.length === 0) return false;

  const [, fields] = messages[0];
  const fieldMap: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    fieldMap[fields[i]] = fields[i + 1];
  }

  await enqueueTask({
    job_id: fieldMap.job_id,
    payload: fieldMap.payload,
    attempt: 1,
    priority: parseInt(fieldMap.priority, 10),
    enqueued_at: new Date().toISOString(),
    timeout_ms: parseInt(fieldMap.timeout_ms, 10),
    max_retries: parseInt(fieldMap.max_retries, 10),
    handler: fieldMap.handler,
  });

  await redis.xdel(TASK_DLQ, messageId);
  return true;
}

export async function purgeDLQ(): Promise<number> {
  const result = await redis.xlen(TASK_DLQ);
  if (result > 0) {
    await redis.xtrim(TASK_DLQ, 'MAXLEN', 0);
  }
  return result;
}

export { TASK_QUEUE, TASK_DLQ, CONSUMER_GROUP, DLQ_CONSUMER_GROUP };
