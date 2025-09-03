import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
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
const genericHosts:Set<string> = new Set(JSON.parse(fs.readFileSync(genericHostsPath, "utf-8")));

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
    for (const q of queries) {
      const data = await serperSearch(q);
      // const items = [...(data.organic || []), ...(data.peopleAlsoSearch || []), ...(data.knowledgeGraph || [])];
      const items = [...((data as any).organic || []), ...((data as any).peopleAlsoSearch || []), ...((data as any).knowledgeGraph || [])];
      await logEvent(job.id as string, 'debug', 'Serper results', { q, counts: { organic: (data as any).organic?.length || 0, peopleAlsoSearch: (data as any).peopleAlsoSearch?.length || 0, knowledgeGraph: (data as any).knowledgeGraph?.length || 0 } });
      for (const it of items) {
        const url = it.link || it.url || "";
        if (!url) continue;
        const host = baseHost(url);
        if (!host) continue;
        if (genericHosts.has(host)) continue;
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

    await completeJob(job.id as string, { companyNumber, companyName, potential });
    logger.info({ companyNumber, potential: potential.length }, "Company discovery done");
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ companyNumber, err: String(err) }, "Company discovery failed");
    throw err;
  }
}, { connection, concurrency: 1 });
