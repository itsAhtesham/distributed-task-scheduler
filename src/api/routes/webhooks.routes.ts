import { Router } from 'express';
import * as webhooksController from '../controllers/webhooks.controller.js';
import { validate } from '../middlewares/validate.js';
import { pagination } from '../middlewares/pagination.js';
import { CreateWebhookSchema } from '../../types/webhook.types.js';

const router = Router();

router.post('/', validate(CreateWebhookSchema), webhooksController.createWebhook);
router.get('/', pagination(), webhooksController.listWebhooks);
router.delete('/:id', webhooksController.deleteWebhook);

export default router;
