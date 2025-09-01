import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { fetch } from "undici";
import { logger } from "../lib/logger.js";
import { base } from "../lib/airtable.js";

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";

async function serperSearch(q: string) {
  const res = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ q, gl: "uk", hl: "en", autocorrect: true })
  });
  if (!res.ok) throw new Error(`Serper ${res.status}`);
  return res.json();
}

export default new Worker("person:linkedin", async job => {
  const { personId } = job.data as { personId: string };

  const person = await base("People").find(personId);
  const fullName = [person.get("first_name"), person.get("middle_names"), person.get("last_name")].filter(Boolean).join(" ");
  const dob = person.get("dob_string") as string | undefined;

  // Starter strategy: name-focused search (you can enhance to include trading names later)
  const queries: string[] = [];
  queries.push(`${fullName} UK site:linkedin.com/in`);
  if (dob) queries.push(`${fullName} ${dob} site:linkedin.com/in`);

  const urls = new Set<string>();
  for (const q of queries) {
    const data = await serperSearch(q);
    for (const it of data.organic || []) {
      const link = it.link || "";
      if (link && link.includes("linkedin.com/in")) urls.add(link.split("?")[0]);
    }
    await new Promise(r => setTimeout(r, 1500)); // gentle pacing
  }

  if (urls.size) {
    await base("People").update(personId, { potential_linkedins: Array.from(urls) as any });
  }

  logger.info({ personId, results: urls.size }, "Person LinkedIn discovery done");
}, { connection, concurrency: 1 });
