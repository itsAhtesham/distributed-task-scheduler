import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import { env } from './config/env.js';
import { logger } from './utils/logger.js';
import { runMigrations } from './db/migrate.js';
import { authenticate } from './api/middlewares/auth.js';
import { rateLimiter } from './api/middlewares/rateLimiter.js';
import { errorHandler } from './api/middlewares/errorHandler.js';
import jobsRoutes from './api/routes/jobs.routes.js';
import webhooksRoutes from './api/routes/webhooks.routes.js';
import healthRoutes from './api/routes/health.routes.js';

const app = express();

// Global middlewares
app.use(helmet());
app.use(cors());
app.use(express.json());

// Health check (no auth required)
app.use('/api/v1/health', healthRoutes);

// Auth + rate limiting for all other routes
app.use('/api/v1', authenticate);
app.use('/api/v1', rateLimiter());

// API routes
app.use('/api/v1/jobs', jobsRoutes);
app.use('/api/v1/webhooks', webhooksRoutes);

// Error handler
app.use(errorHandler);

async function start(): Promise<void> {
  try {
    // Run database migrations
    await runMigrations();
    logger.info('Database migrations completed');

    app.listen(env.PORT, () => {
      logger.info({ port: env.PORT }, 'API server started');
    });
  } catch (err) {
    logger.error({ err }, 'Failed to start API server');
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down...');
  process.exit(0);
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down...');
  process.exit(0);
});

start();
