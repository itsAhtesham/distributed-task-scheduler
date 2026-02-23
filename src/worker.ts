import os from 'node:os';
import { nanoid } from 'nanoid';
import { env } from './config/env.js';
import { logger } from './utils/logger.js';
import { runMigrations } from './db/migrate.js';
import { initializeStreams } from './services/queueService.js';
import { LeaderElection } from './services/leaderElection.js';
import { SchedulerService } from './services/schedulerService.js';
import { TaskConsumer } from './workers/taskConsumer.js';
import { DLQProcessor } from './workers/dlqProcessor.js';
import { Heartbeat } from './workers/heartbeat.js';
import { closeDatabasePool } from './config/database.js';
import { closeRedisConnection } from './config/redis.js';

// Generate unique worker ID
const workerId = env.WORKER_ID || `${os.hostname()}-${nanoid(6)}`;

const leaderElection = new LeaderElection(workerId);
const schedulerService = new SchedulerService();
const taskConsumer = new TaskConsumer(workerId);
const dlqProcessor = new DLQProcessor();
const heartbeat = new Heartbeat(workerId);

let wasLeader = false;

async function start(): Promise<void> {
  logger.info({ workerId }, 'Worker starting...');

  try {
    // Run database migrations
    await runMigrations();
    logger.info('Database migrations completed');

    // Initialize Redis Streams
    await initializeStreams();
    logger.info('Redis Streams initialized');

    // Start heartbeat
    heartbeat.start();

    // Start leader election loop
    leaderElection.startElectionLoop();

    // Monitor leader status and start/stop scheduler accordingly
    setInterval(() => {
      const isLeader = leaderElection.isLeader();

      if (isLeader && !wasLeader) {
        logger.info({ workerId }, 'Became leader - starting scheduler and DLQ processor');
        schedulerService.start();
        dlqProcessor.start();
        wasLeader = true;
      } else if (!isLeader && wasLeader) {
        logger.info({ workerId }, 'Lost leadership - stopping scheduler and DLQ processor');
        schedulerService.stop();
        dlqProcessor.stop();
        wasLeader = false;
      }
    }, 2000);

    // Start task consumer (all workers consume tasks)
    taskConsumer.start();

    logger.info({ workerId }, 'Worker started successfully');
  } catch (err) {
    logger.error({ err, workerId }, 'Failed to start worker');
    process.exit(1);
  }
}

async function shutdown(): Promise<void> {
  logger.info({ workerId }, 'Shutting down worker...');

  // Stop consuming new tasks
  taskConsumer.stop();

  // Stop leader duties
  schedulerService.stop();
  dlqProcessor.stop();
  leaderElection.stopElectionLoop();

  // Release leadership
  await leaderElection.releaseLeadership();

  // Stop heartbeat
  heartbeat.stop();

  // Drain in-progress tasks
  await taskConsumer.drain();

  // Close connections
  await closeRedisConnection();
  await closeDatabasePool();

  logger.info({ workerId }, 'Worker shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start();
