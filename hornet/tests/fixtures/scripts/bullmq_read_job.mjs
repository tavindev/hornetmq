// Reads a job by ID from a queue using BullMQ's Queue.getJob().
// Usage: node bullmq_read_job.mjs <queue_name> <job_id>
// Prints job JSON to stdout, or "NOT_FOUND".

import { Queue } from 'bullmq';
import IORedis from 'ioredis';

const [,, queueName, jobId] = process.argv;

const connection = new IORedis({ maxRetriesPerRequest: null });
const queue = new Queue(queueName, { connection });

const job = await queue.getJob(jobId);

if (job) {
  console.log(JSON.stringify({
    id: job.id,
    name: job.name,
    data: job.data,
    opts: job.opts,
  }));
} else {
  console.log('NOT_FOUND');
}

await queue.close();
await connection.quit();
