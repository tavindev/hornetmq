// Produces a job using the real BullMQ library.
// Usage: node bullmq_produce.mjs <queue_name> <job_name> <data_json> [opts_json]
// Prints the job ID to stdout.

import { Queue } from 'bullmq';
import IORedis from 'ioredis';

const [,, queueName, jobName, dataJson, optsJson] = process.argv;

const connection = new IORedis({ maxRetriesPerRequest: null });
const queue = new Queue(queueName, { connection });

const data = JSON.parse(dataJson);
const opts = optsJson ? JSON.parse(optsJson) : {};

const job = await queue.add(jobName, data, opts);
console.log(job.id);

await queue.close();
await connection.quit();
