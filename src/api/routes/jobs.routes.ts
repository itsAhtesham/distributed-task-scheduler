import { Router } from 'express';
import * as jobsController from '../controllers/jobs.controller.js';
import { validate } from '../middlewares/validate.js';
import { pagination } from '../middlewares/pagination.js';
import { CreateJobSchema, UpdateJobSchema } from '../../types/job.types.js';

const router = Router();

router.post('/', validate(CreateJobSchema), jobsController.createJob);
router.get('/', pagination(), jobsController.listJobs);
router.get('/:id', jobsController.getJob);
router.patch('/:id', validate(UpdateJobSchema), jobsController.updateJob);
router.delete('/:id', jobsController.deleteJob);
router.post('/:id/trigger', jobsController.triggerJob);
router.get('/:id/runs', pagination(), jobsController.getJobRuns);

export default router;
