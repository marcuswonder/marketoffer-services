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
type PersonSearchPayload = {
  personId?: string;
  person?: { firstName: string; middleNames?: string; lastName: string; dob?: string };
  context?: { companyNumber?: string; companyName?: string; websites?: string[]; companyLinkedIns?: string[]; personalLinkedIns?: string[] };
};

export default new Worker("person-linkedin", async job => {
  const { personId, person, context } = job.data as PersonSearchPayload;
  await startJob({ jobId: job.id as string, queue: 'person-linkedin', name: job.name, payload: job.data });
  try {
    let fullName = "";
    let dob: string | undefined;

    if (personId) {
      const rec = await base("People").find(personId);
      fullName = [rec.get("first_name"), rec.get("middle_names"), rec.get("last_name")].filter(Boolean).join(" ");
      dob = rec.get("dob_string") as string | undefined;
      await logEvent(job.id as string, 'info', 'Loaded person (Airtable)', { personId, fullName, hasDob: Boolean(dob) });
    } else if (person) {
      fullName = [person.firstName, person.middleNames, person.lastName].filter(Boolean).join(" ");
      dob = person.dob || undefined;
      await logEvent(job.id as string, 'info', 'Loaded person (payload)', { fullName, hasDob: Boolean(dob), context });
    } else {
      throw new Error('Missing personId or person payload');
    }

    // Build queries
    const queries: string[] = [];
    if (fullName.trim()) {
      queries.push(`${fullName} UK site:linkedin.com/in`);
      if (dob) queries.push(`${fullName} ${dob} site:linkedin.com/in`);
    }
    const websites: string[] = Array.isArray(context?.websites) ? (context!.websites as string[]) : [];
    const companyNameCtx: string = (context?.companyName || '').toString();
    const personalSeeds: string[] = Array.isArray(context?.personalLinkedIns) ? (context!.personalLinkedIns as string[]) : [];
    // Use any known company website hosts as additional disambiguation
    for (const w of websites) {
      const host = (w || '').toString().replace(/^https?:\/\//i, '').replace(/\/$/, '');
      if (!host) continue;
      if (fullName.trim()) queries.push(`${fullName} ${host} site:linkedin.com/in`);
    }
    if (companyNameCtx && fullName.trim()) {
      queries.push(`${fullName} ${companyNameCtx} UK site:linkedin.com/in`);
      queries.push(`${fullName} "${companyNameCtx}" site:linkedin.com/in`);
    }
    await logEvent(job.id as string, 'info', 'Built queries', { queries, context, personalSeeds: personalSeeds.length });

    const urls = new Set<string>();
    for (const q of queries) {
      const data = await serperSearch(q);
      for (const it of (data as any).organic || []) {
        const link = it.link || "";
        if (link && link.includes("linkedin.com/in")) urls.add(link.split("?")[0]);
      }
      await logEvent(job.id as string, 'debug', 'Query processed', { q, found: Array.from(urls).length });
      await new Promise(r => setTimeout(r, 1500)); // gentle pacing
    }

    // Seed with any personal LinkedIns we already have
    for (const u of personalSeeds) if (u.includes('linkedin.com/in')) urls.add(u);

    if (urls.size && personId && WRITE_TO_AIRTABLE) {
      await base("People").update(personId, { potential_linkedins: Array.from(urls) as any });
    } else if (urls.size && !personId) {
      await logEvent(job.id as string, 'info', 'Airtable write skipped (no personId)', { count: Array.from(urls).length });
    } else if (urls.size && personId && !WRITE_TO_AIRTABLE) {
      await logEvent(job.id as string, 'info', 'Airtable write skipped (flag false)', { count: Array.from(urls).length });
    }

    await completeJob(job.id as string, { personId: personId || null, fullName, results: Array.from(urls), context });
    logger.info({ personId: personId || null, results: urls.size }, "Person LinkedIn discovery done");
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ err: String(err) }, 'Person LinkedIn worker failed');
    throw err;
  }
}, { connection, concurrency: 1 });
