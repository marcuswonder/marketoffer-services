import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { fetch } from "undici";
import { logger } from "../lib/logger.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { base } from "../lib/airtable.js";
const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";

async function serperSearch(q: string) {
  const res = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ q, gl: "uk", hl: "en", autocorrect: true })
  });
  if (!res.ok) throw new Error(`Serper ${res.status}`);
  // return res.json();
  return (await res.json()) as any;
}

await initDb();
export default new Worker("person-linkedin", async job => {
  const { personId } = job.data as { personId: string };
  await startJob({ jobId: job.id as string, queue: 'person-linkedin', name: job.name, payload: job.data });
  try {
    const person = await base("People").find(personId);
    const fullName = [person.get("first_name"), person.get("middle_names"), person.get("last_name")].filter(Boolean).join(" ");
    const dob = person.get("dob_string") as string | undefined;
    await logEvent(job.id as string, 'info', 'Loaded person', { personId, fullName, hasDob: Boolean(dob) });

    // Starter strategy: name-focused search (you can enhance to include trading names later)
    const queries: string[] = [];
    queries.push(`${fullName} UK site:linkedin.com/in`);
    if (dob) queries.push(`${fullName} ${dob} site:linkedin.com/in`);
    await logEvent(job.id as string, 'info', 'Built queries', { queries });

    const urls = new Set<string>();
    for (const q of queries) {
      const data = await serperSearch(q);
      // for (const it of data.organic || []) {
      for (const it of (data as any).organic || []) {
        const link = it.link || "";
        if (link && link.includes("linkedin.com/in")) urls.add(link.split("?")[0]);
      }
      await logEvent(job.id as string, 'debug', 'Query processed', { q, found: Array.from(urls).length });
      await new Promise(r => setTimeout(r, 1500)); // gentle pacing
    }

    if (urls.size) {
      if (WRITE_TO_AIRTABLE) {
        await base("People").update(personId, { potential_linkedins: Array.from(urls) as any });
      } else {
        await logEvent(job.id as string, 'info', 'Airtable write skipped', {
          reason: 'WRITE_TO_AIRTABLE=false',
          table: 'People',
          field: 'potential_linkedins',
          count: Array.from(urls).length
        });
      }
    }

    await completeJob(job.id as string, { personId, results: Array.from(urls) });
    logger.info({ personId, results: urls.size }, "Person LinkedIn discovery done");
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ personId, err: String(err) }, 'Person LinkedIn worker failed');
    throw err;
  }
}, { connection, concurrency: 1 });
