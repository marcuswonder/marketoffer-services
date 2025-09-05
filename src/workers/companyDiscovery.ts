import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { siteFetchQ } from "../queues/index.js";
import { cleanCompanyName, baseHost } from "../lib/normalize.js";
import { batchCreate } from "../lib/airtable.js";
import { logger } from "../lib/logger.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { fetch } from "undici";
import fs from "fs";
import path from "path";

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";
const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';
const genericHostsPath = path.join(process.cwd(), "config", "genericHosts.json");
const genericHosts:Set<string> = new Set(
  (JSON.parse(fs.readFileSync(genericHostsPath, "utf-8")) as string[]).map(s => s.toLowerCase())
);

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

function asArray<T = any>(val: any): T[] {
  if (Array.isArray(val)) return val as T[];
  if (val && typeof val === 'object') return [val as T];
  return [] as T[];
}

await initDb();
export default new Worker("company-discovery", async job => {
  const { companyNumber, companyName, address, postcode } = job.data as any;
  await startJob({ jobId: job.id as string, queue: 'company-discovery', name: job.name, payload: job.data });
  try {
    const cleaned = cleanCompanyName(companyName || "");
    const queries = [
      cleaned,
      [cleaned, postcode, address].filter(Boolean).join(" ")
    ].filter(Boolean);
    await logEvent(job.id as string, 'info', 'Built queries', { queries });

    const hosts = new Set<string>();
    const genericList = Array.from(genericHosts);
    const isGeneric = (host: string) => {
      if (genericHosts.has(host)) return true;
      for (const g of genericList) {
        if (host === g || host.endsWith(`.${g}`)) return true;
      }
      return false;
    };
    for (const q of queries) {
      const data = await serperSearch(q);
      const organic = asArray((data as any).organic);
      const peopleAlso = asArray((data as any).peopleAlsoSearch);
      const knowledge = asArray((data as any).knowledgeGraph);
      const items = [...organic, ...peopleAlso, ...knowledge];
      await logEvent(job.id as string, 'debug', 'Serper results', {
        q,
        counts: {
          organic: organic.length,
          peopleAlsoSearch: peopleAlso.length,
          knowledgeGraph: knowledge.length,
        }
      });
      for (const it of items) {
        const url = it.link || it.url || "";
        if (!url) continue;
        const host = baseHost(url);
        if (!host) continue;
        if (isGeneric(host)) continue;
        hosts.add(host);
      }
    }

    const potential = Array.from(hosts);
    if (potential.length) {
      if (WRITE_TO_AIRTABLE) {
        await batchCreate("Companies", potential.map(h => ({
          fields: {
            company_number: companyNumber || "",
            company_name: companyName || "",
            potential_websites: [h]
          }
        })));
      } else {
        await logEvent(job.id as string, 'info', 'Airtable write skipped', {
          reason: 'WRITE_TO_AIRTABLE=false',
          table: 'Companies',
          potential_count: potential.length
        });
      }
    }

    // Enqueue site-fetch validations for each potential host
    let enq = 0;
    for (const host of potential) {
      const jobId = `sf:${companyNumber}:${host}`;
      await siteFetchQ.add(
        'audit',
        { companyNumber, companyName, address, postcode, host },
        { jobId, attempts: 3, backoff: { type: 'exponential', delay: 2000 } }
      );
      enq++;
    }
    await logEvent(job.id as string, 'info', 'Enqueued site-fetch audits', { count: enq });

    await completeJob(job.id as string, { companyNumber, companyName, potential, site_fetch_enqueued: potential.length });
    logger.info({ companyNumber, potential: potential.length, siteFetch: potential.length }, "Company discovery done");
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ companyNumber, err: String(err) }, "Company discovery failed");
    throw err;
  }
}, { connection, concurrency: 1 });
