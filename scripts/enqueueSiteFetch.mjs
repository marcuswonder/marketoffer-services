import { Queue } from 'bullmq';

const argv = process.argv.slice(2);
if (argv.length < 3) {
  console.error('Usage: node scripts/enqueueSiteFetch.mjs <host> <companyNumber> <companyName> [postcode] [address]');
  process.exit(1);
}

const [host, companyNumber, ...rest] = argv;
const companyName = rest[0] || '';
const postcode = rest[1] || '';
const address = rest.slice(2).join(' ') || '';

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

const queue = new Queue('site-fetch', { connection: { url: redisUrl } });

const payload = { host, companyNumber, companyName, postcode, address };
const jobId = `manual:${Date.now()}:${host}`;

const job = await queue.add('fetch', payload, { jobId, attempts: 3, backoff: { type: 'exponential', delay: 1500 } });
console.log('Enqueued site-fetch', { id: job.id, payload });
await queue.close();

