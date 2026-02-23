import { Router } from 'express';
import { checkDatabaseConnection } from '../../config/database.js';
import { checkRedisConnection } from '../../config/redis.js';
import { Heartbeat } from '../../workers/heartbeat.js';

const router = Router();

router.get('/', async (_req, res) => {
  const [dbHealthy, redisHealthy] = await Promise.all([
    checkDatabaseConnection(),
    checkRedisConnection(),
  ]);

  let workers: Array<{ id: string; lastHeartbeat: string }> = [];
  try {
    workers = await Heartbeat.getActiveWorkers();
  } catch {
    // Workers may not be available
  }

  const healthy = dbHealthy && redisHealthy;

  res.status(healthy ? 200 : 503).json({
    success: true,
    data: {
      status: healthy ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      services: {
        database: dbHealthy ? 'connected' : 'disconnected',
        redis: redisHealthy ? 'connected' : 'disconnected',
      },
      workers: {
        active: workers.length,
        nodes: workers,
      },
    },
  });
});

export default router;
