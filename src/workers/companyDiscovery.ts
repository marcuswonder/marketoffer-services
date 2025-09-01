import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { cleanCompanyName, baseHost } from "../lib/normalize.js";
import { batchCreate } from "../lib/airtable.js";
import { logger } from "../lib/logger.js";
import { fetch } from "undici";
import fs from "fs";
import path from "path";

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";
const genericHostsPath = path.join(process.cwd(), "config", "genericHosts.json");
const genericHosts:Set<string> = new Set(JSON.parse(fs.readFileSync(genericHostsPath, "utf-8")));

async function serperSearch(q: string) {
  const res = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ q, gl: "uk", hl: "en", autocorrect: true })
  });
  if (!res.ok) throw new Error(`Serper ${res.status}`);
  return res.json();
}

export default new Worker("company:discovery", async job => {
  const { companyNumber, companyName, address, postcode } = job.data as any;
  const cleaned = cleanCompanyName(companyName || "");
  const queries = [
    cleaned,
    [cleaned, postcode, address].filter(Boolean).join(" ")
  ].filter(Boolean);

  const hosts = new Set<string>();
  for (const q of queries) {
    const data = await serperSearch(q);
    const items = [...(data.organic || []), ...(data.peopleAlsoSearch || []), ...(data.knowledgeGraph || [])];
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
    await batchCreate("Companies", potential.map(h => ({
      fields: {
        company_number: companyNumber || "",
        company_name: companyName || "",
        potential_websites: [h]
      }
    })));
  }

  logger.info({ companyNumber, potential: potential.length }, "Company discovery done");
}, { connection, concurrency: 1 });
