import { Queue, QueueScheduler } from "bullmq";
import IORedis from "ioredis";

export const connection = new IORedis(process.env.REDIS_URL || "redis://localhost:6379");

export const chQ = new Queue("ch:appointments", { connection });
export const companyQ = new Queue("company:discovery", { connection });
export const personQ = new Queue("person:linkedin", { connection });

// Schedulers to handle delayed/retried jobs
new QueueScheduler("ch:appointments", { connection });
new QueueScheduler("company:discovery", { connection });
new QueueScheduler("person:linkedin", { connection });
