import { Queue } from "bullmq";
import IORedis from "ioredis";

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";

export const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

export const chQ = new Queue("ch-appointments", { connection });
export const companyQ = new Queue("company-discovery", { connection });
export const personQ = new Queue("person-linkedin", { connection });
export const siteFetchQ = new Queue("site-fetch", { connection });
export const ownerQ = new Queue("owner-discovery", { connection });

// Schedulers to handle delayed/retried jobs
new Queue("ch-appointments", { connection });
new Queue("company-discovery", { connection });
new Queue("person-linkedin", { connection });
new Queue("site-fetch", { connection });
new Queue("owner-discovery", { connection });
