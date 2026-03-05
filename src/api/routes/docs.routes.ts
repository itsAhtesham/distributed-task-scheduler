import { Router } from 'express';
import swaggerUi from 'swagger-ui-express';
import { openApiDocument } from '../openapi.js';

const router = Router();

// Raw OpenAPI spec
router.get('/openapi.json', (_req, res) => {
  res.json(openApiDocument);
});

// Swagger UI
router.use('/docs', swaggerUi.serve, swaggerUi.setup(openApiDocument));

// Stoplight Elements (CDN)
router.get('/reference', (_req, res) => {
  res.send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>API Reference — Distributed Task Scheduler</title>
  <link rel="stylesheet" href="https://unpkg.com/@stoplight/elements/styles.min.css" />
</head>
<body>
  <elements-api
    apiDescriptionUrl="/openapi.json"
    router="hash"
    layout="sidebar"
  />
  <script src="https://unpkg.com/@stoplight/elements/web-components.min.js"></script>
</body>
</html>`);
});

export default router;
