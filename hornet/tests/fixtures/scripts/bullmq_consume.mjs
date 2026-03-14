// Consumes a single job from a queue using BullMQ Worker.
// Usage: node bullmq_consume.mjs <queue_name> <timeout_ms>
// Prints the job data JSON to stdout when processed, or "TIMEOUT" if no job arrived.

import { Worker } from 'bullmq';
import IORedis from 'ioredis';

const [,, queueName, timeoutMs] = process.argv;
const timeout = parseInt(timeoutMs || '5000', 10);

const connection = new IORedis({ maxRetriesPerRequest: null });

const result = await new Promise((resolve) => {
  const timer = setTimeout(() => {
    resolve(null);
  }, timeout);

  const worker = new Worker(queueName, async (job) => {
    clearTimeout(timer);
    resolve({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
    });
    return 'ok';
  }, { connection, autorun: true });

  // Store worker ref so we can close it
  globalThis.__worker = worker;
});

if (result) {
  console.log(JSON.stringify(result));
} else {
  console.log('TIMEOUT');
}

if (globalThis.__worker) {
  await globalThis.__worker.close();
}
await connection.quit();
