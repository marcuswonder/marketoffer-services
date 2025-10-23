import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { personQ } from "../queues/index.js";
import { siteFetchQ } from "../queues/index.js";
import { cleanCompanyName, baseHost } from "../lib/normalize.js";
import { batchCreate } from "../lib/airtable.js";
import { logger } from "../lib/logger.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { fetch } from "undici";
import fs from "fs";
import path from "path";
import { query } from "../lib/db.js";

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";
const BEE_KEY = process.env.SCRAPINGBEE_API_KEY || "";
const OPENAI_KEY = process.env.OPENAI_API_KEY || "";
const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';
const genericHostsPath = path.join(process.cwd(), "config", "genericHosts.json");
let genericHosts: Set<string>;
try {
  const rawHosts = JSON.parse(fs.readFileSync(genericHostsPath, "utf-8")) as string[];
  genericHosts = new Set((rawHosts || []).map((s) => s.toLowerCase()));
} catch (err) {
  logger.warn(
    { path: genericHostsPath, err: err instanceof Error ? err.message : String(err) },
    'Falling back to empty generic host list'
  );
  genericHosts = new Set();
}
const sicMapPath = path.join(process.cwd(), "config", "sicCodes.json");
const sicEntries: Array<{ code: number; key_phrases?: string[]; description?: string }> = JSON.parse(fs.readFileSync(sicMapPath, 'utf-8'));
const sicToPhrases = new Map<string, string[]>(sicEntries.map(e => [String(e.code), Array.isArray(e.key_phrases) ? e.key_phrases : []]));

async function serperSearch(q: string): Promise<{ status: number; headers: Record<string,string>; data: any }> {
  const res = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ q, gl: "uk", hl: "en", autocorrect: true, num: 10 })
  });
  const status = res.status;
  const headers: Record<string,string> = {};
  try { res.headers.forEach((v,k)=>{ headers[k]=v; }); } catch {}
  if (!res.ok) throw new Error(`Serper ${res.status}`);
  const data = await res.json();
  return { status, headers, data };
}

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

function asArray<T = any>(val: any): T[] {
  if (Array.isArray(val)) return val as T[];
  if (val && typeof val === 'object') return [val as T];
  return [] as T[];
}

type CanonicalCompanyRecord = {
  id: number;
  company_number: string;
  name: string | null;
  status: string | null;
  registered_address: string | null;
  registered_postcode: string | null;
  sic_codes: string[] | null;
  discovery_status: string | null;
  discovery_job_id: string | null;
};

async function ensureCanonicalCompany(input: {
  companyId?: number | null;
  companyNumber?: string | null;
  companyName?: string | null;
  companyStatus?: string | null;
  registeredAddress?: string | null;
  registeredPostcode?: string | null;
  sicCodes?: string[] | null;
}): Promise<CanonicalCompanyRecord> {
  const cleanedNumber = (input.companyNumber || '').trim();
  if (input.companyId) {
    const { rows } = await query<CanonicalCompanyRecord>(
      `SELECT id, company_number, name, status, registered_address, registered_postcode, sic_codes, discovery_status, discovery_job_id
         FROM ch_companies WHERE id = $1 LIMIT 1`,
      [input.companyId]
    );
    if (rows[0]) return rows[0];
  }
  if (cleanedNumber) {
    const { rows } = await query<CanonicalCompanyRecord>(
      `SELECT id, company_number, name, status, registered_address, registered_postcode, sic_codes, discovery_status, discovery_job_id
         FROM ch_companies WHERE company_number = $1 LIMIT 1`,
      [cleanedNumber]
    );
    if (rows[0]) return rows[0];
  }

  const numberToInsert = cleanedNumber || `unknown:${Date.now()}`;
  const { rows } = await query<CanonicalCompanyRecord>(
    `INSERT INTO ch_companies (company_number, name, status, registered_address, registered_postcode, sic_codes)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (company_number)
     DO UPDATE SET
       name = COALESCE(EXCLUDED.name, ch_companies.name),
       status = COALESCE(EXCLUDED.status, ch_companies.status),
       registered_address = COALESCE(EXCLUDED.registered_address, ch_companies.registered_address),
       registered_postcode = COALESCE(EXCLUDED.registered_postcode, ch_companies.registered_postcode),
       sic_codes = CASE
         WHEN EXCLUDED.sic_codes IS NOT NULL AND CARDINALITY(EXCLUDED.sic_codes) > 0
           THEN EXCLUDED.sic_codes
         ELSE ch_companies.sic_codes
       END,
       updated_at = now()
     RETURNING id, company_number, name, status, registered_address, registered_postcode, sic_codes, discovery_status, discovery_job_id`,
    [
      numberToInsert,
      input.companyName || null,
      input.companyStatus || null,
      input.registeredAddress || null,
      input.registeredPostcode || null,
      input.sicCodes && input.sicCodes.length ? input.sicCodes : null,
    ]
  );
  return rows[0];
}

await initDb();
export default new Worker("company-discovery", async job => {
  const data = job.data as any;
  let {
    companyNumber,
    companyName,
    address,
    postcode,
    rootJobId,
    parentJobId,
    requestSource,
    ownerJobId,
    chJobId,
    companyId: incomingCompanyId,
  } = data;
  const resolvedRoot = rootJobId || ownerJobId || chJobId || (job.id as string);
  let canonicalCompanyId: number | null = null;
  let metadataPayload: Record<string, any> = {};
  await startJob({
    jobId: job.id as string,
    queue: 'company-discovery',
    name: job.name,
    payload: job.data,
    rootJobId: resolvedRoot,
    parentJobId: parentJobId || chJobId || ownerJobId || null,
    requestSource: requestSource || 'company-discovery',
  });
  try {
    const canonical = await ensureCanonicalCompany({
      companyId: incomingCompanyId,
      companyNumber,
      companyName,
      companyStatus: null,
      registeredAddress: address,
      registeredPostcode: postcode,
    });
    canonicalCompanyId = canonical.id;
    companyNumber = canonical.company_number;
    companyName = canonical.name || companyName;
    address = canonical.registered_address || address;
    postcode = canonical.registered_postcode || postcode;

    if (canonical.discovery_status === 'running' && canonical.discovery_job_id && canonical.discovery_job_id !== job.id) {
      await logEvent(job.id as string, 'info', 'Company discovery already in progress', {
        companyId: canonicalCompanyId,
        companyNumber,
        discoveryStatus: canonical.discovery_status,
        discoveryJobId: canonical.discovery_job_id,
      });
      await completeJob(job.id as string, {
        companyId: canonicalCompanyId,
        companyNumber,
        companyName,
        skipped: true,
        reason: 'already_running',
        activeJobId: canonical.discovery_job_id,
      });
      return;
    }

    if (canonical.discovery_status === 'queued' && canonical.discovery_job_id && canonical.discovery_job_id !== job.id) {
      await logEvent(job.id as string, 'info', 'Company discovery already queued by another job', {
        companyId: canonicalCompanyId,
        companyNumber,
        discoveryStatus: canonical.discovery_status,
        discoveryJobId: canonical.discovery_job_id,
      });
      await completeJob(job.id as string, {
        companyId: canonicalCompanyId,
        companyNumber,
        companyName,
        skipped: true,
        reason: 'already_queued',
        activeJobId: canonical.discovery_job_id,
      });
      return;
    }

    if (canonical.discovery_status === 'completed' && (requestSource || '').toLowerCase() !== 'manual') {
      await logEvent(job.id as string, 'info', 'Skipping discovery (already completed)', {
        companyId: canonicalCompanyId,
        companyNumber,
      });
      await completeJob(job.id as string, {
        companyId: canonicalCompanyId,
        companyNumber,
        companyName,
        skipped: true,
        reason: 'already_completed',
      });
      return;
    }

    await query(
      `UPDATE ch_companies
          SET discovery_status = 'running',
              discovery_job_id = $2,
              discovery_error = NULL,
              updated_at = now()
        WHERE id = $1`,
      [canonicalCompanyId, job.id as string]
    );

    const cleaned = cleanCompanyName(companyName || "");
    // Gather SIC codes and director names from our DB if available
    const sicRows = await query<{ sic: string }>(
      `SELECT DISTINCT UNNEST(sic_codes) AS sic FROM ch_appointments WHERE company_number = $1 AND sic_codes IS NOT NULL`,
      [companyNumber || null]
    );
    const sicCodes = sicRows.rows.map(r => (r.sic || '').toString()).filter(Boolean);
    const keyPhrases = Array.from(new Set(sicCodes.flatMap(c => sicToPhrases.get(c) || [])));
    const dirRows = await query<{ full_name: string }>(
      `SELECT DISTINCT p.full_name FROM ch_people p JOIN ch_appointments a ON a.person_id = p.id WHERE a.company_number = $1 AND COALESCE(p.full_name,'') <> ''`,
      [companyNumber || null]
    );
    const directorNames = dirRows.rows.map(r => r.full_name).filter(Boolean);

    // Build queries per instructions
    const safeCompanyName = typeof companyName === 'string' ? companyName.trim() : '';
    const safeCleanName = cleaned.trim();
    const safeCompanyNumber = typeof companyNumber === 'string' ? companyNumber.trim() : '';
    const safePostcode = typeof postcode === 'string' ? postcode.trim() : '';
    const rawAddress = typeof address === 'string' ? address : '';
    const normalizedAddress = rawAddress.replace(/\s+/g, ' ').trim();
    const addressLine = normalizedAddress ? normalizedAddress.split(',')[0]?.trim() || '' : '';

    const queries: string[] = [];
    const seenQueries = new Set<string>();
    const append = (q: string) => {
      if (!q) return;
      const trimmed = q.trim();
      if (!trimmed || seenQueries.has(trimmed)) return;
      seenQueries.add(trimmed);
      queries.push(trimmed);
    };

    if (safeCompanyName) append(`${safeCompanyName} UK`);
    if (safeCleanName) append(`${safeCleanName} UK`);

    if (safeCompanyNumber) {
      if (safeCompanyName) {
        append(`${safeCompanyName} ${safeCompanyNumber} UK`);
        append(`${safeCompanyName} "${safeCompanyNumber}"`);
      }
      if (safeCleanName) {
        append(`${safeCleanName} ${safeCompanyNumber} UK`);
        append(`${safeCleanName} "${safeCompanyNumber}"`);
      }
      if (!safeCompanyName && !safeCleanName) append(`${safeCompanyNumber} UK`);
    }

    if (safePostcode) {
      if (safeCleanName) append(`${safeCleanName} ${safePostcode} UK`);
      if (safeCompanyName) append(`${safeCompanyName} ${safePostcode} UK`);
    }

    if (addressLine) {
      if (safeCleanName) append(`${safeCleanName} ${addressLine} UK`);
      if (safeCompanyName) append(`${safeCompanyName} ${addressLine} UK`);
      if (safeCompanyName) append(`${safeCompanyName} "${addressLine}" UK`);
      if (safeCleanName) append(`${safeCleanName} "${addressLine}" UK`);
    }
    if (normalizedAddress && normalizedAddress !== addressLine) {
      if (safeCompanyName) append(`${safeCompanyName} "${normalizedAddress}" UK`);
      if (safeCleanName) append(`${safeCleanName} "${normalizedAddress}" UK`);
    }

    for (const kp of keyPhrases.slice(0, 5)) {
      if (safeCompanyName) append(`${safeCompanyName} ${kp} UK`);
      if (safeCleanName) append(`${safeCleanName} ${kp} UK`);
    }
    await logEvent(job.id as string, 'info', 'Built queries', { queries, sicCodes, keyPhrases, directorNames });
    metadataPayload = {
      queries,
      sicCodes,
      keyPhrases,
      directorNames,
    };

    const hosts = new Set<string>();
    const genericList = Array.from(genericHosts);
    const isGeneric = (host: string) => {
      if (genericHosts.has(host)) return true;
      for (const g of genericList) {
        if (host === g || host.endsWith(`.${g}`)) return true;
      }
      return false;
    };
    const baseSimple = `${companyName} UK`;
    let serperCalls = 0;
    for (const q of queries) {
      const resp = await serperSearch(q);
      serperCalls += 1;
      const data = resp.data as any;
      let organic = asArray((data as any).organic).slice(0, 10);
      let peopleAlso = asArray((data as any).peopleAlsoSearch).slice(0, 10);
      let knowledge = asArray((data as any).knowledgeGraph).slice(0, 10);
      const allEmpty = !(organic.length || peopleAlso.length || knowledge.length);
      if (allEmpty) {
        // Emit diagnostics and retry once with a simplified query
        const topKeys = Object.keys(data || {}).slice(0, 12);
        await logEvent(job.id as string, 'debug', 'Serper zero-results (diagnostic)', {
          q,
          status: resp.status,
          headers: { 'serper-request-id': resp.headers['x-request-id'] || resp.headers['x-requestid'] || resp.headers['request-id'] || '' },
          topLevelKeys: topKeys
        });
        // Retry after short backoff with simplified query
        await sleep(1200);
        const fallbackQ = baseSimple;
        const retry = await serperSearch(fallbackQ);
        const rdata = retry.data as any;
        organic = asArray(rdata.organic).slice(0, 10);
        peopleAlso = asArray(rdata.peopleAlsoSearch).slice(0, 10);
        knowledge = asArray(rdata.knowledgeGraph).slice(0, 10);
        await logEvent(job.id as string, 'debug', 'Serper retry results', {
          q: fallbackQ,
          counts: { organic: organic.length, peopleAlsoSearch: peopleAlso.length, knowledgeGraph: knowledge.length }
        });
      }
      const items = [...organic, ...peopleAlso, ...knowledge];
      try {
        const sample = organic.slice(0, 5).map((it: any) => ({ link: it.link || it.url, title: it.title || it.name, snippet: it.snippet || it.description }));
        await logEvent(job.id as string, 'info', 'Serper fetch', {
          q,
          status: resp.status,
          headers: { 'x-request-id': resp.headers['x-request-id'] || resp.headers['x-requestid'] || resp.headers['request-id'] || '' },
          counts: { organic: organic.length, peopleAlsoSearch: peopleAlso.length, knowledgeGraph: knowledge.length },
          sample
        });
      } catch {}
      const organicUrls = organic.map((it: any) => it.link || it.url || '').filter(Boolean);
      const peopleAlsoUrls = peopleAlso.map((it: any) => it.link || it.url || '').filter(Boolean);
      const knowledgeUrls = knowledge.map((it: any) => it.link || it.url || '').filter(Boolean);
      await logEvent(job.id as string, 'debug', 'Serper results', {
        q,
        counts: { organic: organic.length, peopleAlsoSearch: peopleAlso.length, knowledgeGraph: knowledge.length },
        organicUrls,
        peopleAlsoUrls,
        knowledgeUrls
      });
      for (const it of items) {
        const url = it.link || it.url || "";
        if (!url) continue;
        const h = baseHost(url);
        if (!h || isGeneric(h)) continue;
        hosts.add(h);
      }
      const jitter = 900 + Math.floor(Math.random() * 600);
      await sleep(jitter);
    }

    // Helpers
    const buildBeeUrl = (target: string) => {
      const u = new URL("https://app.scrapingbee.com/api/v1/");
      u.searchParams.set("api_key", BEE_KEY);
      u.searchParams.set("url", target);
      u.searchParams.set("render_js", "true");
      u.searchParams.set("country_code", "gb");
      u.searchParams.set("block_resources", "true");
      u.searchParams.set("timeout", "15000");
      return u.toString();
    };
    const fetchBeeHtml = async (targetUrl: string): Promise<{ status: number; html: string }> => {
      if (!BEE_KEY) return { status: 0, html: "" };
      const res = await fetch(buildBeeUrl(targetUrl), { method: 'GET' });
      return { status: res.status, html: await res.text() };
    };
    const fetchStatic = async (targetUrl: string): Promise<{ ok: boolean; status: number; html: string; ct: string | null }> => {
      try {
        const res = await fetch(targetUrl, { method: 'GET', headers: { 'accept': 'text/html,application/xhtml+xml' } } as any);
        const text = await res.text();
        const ct = res.headers.get('content-type');
        const ok = res.status >= 200 && res.status < 400 && text.length > 100;
        return { ok, status: res.status, html: text, ct };
      } catch {
        return { ok: false, status: 0, html: '', ct: null };
      }
    };
    const stripTags = (s: string) => s.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    const parseSignals = (html: string) => {
      const titleM = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
      const title = titleM ? titleM[1].trim() : '';
      const m = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["'][^>]*>/i);
      const metaDesc = m ? m[1].trim() : '';
      const links = Array.from(new Set((html.match(/<a[^>]+href=["']([^"']+)["']/gi) || []).map(a => {
        const m = a.match(/href=["']([^"']+)["']/i); return m ? m[1] : ''; }).filter(Boolean)));
      const text = stripTags(html).slice(0, 20000);
      return { title, metaDesc, links, text };
    };
    const isCompanyLI = (u: string) => /linkedin\.com\/company\//i.test(u);
    const isPersonalLI = (u: string) => /linkedin\.com\/in\//i.test(u);
    const filterLinkedIns = (urls: string[]) => Array.from(new Set(urls.filter(u => /linkedin\.com\/(company|in)\//i.test(u))));
    const llmJudge = async (baseDomain: string, pages: Array<{ url: string; title: string; meta: string; text: string; links: string[] }>) => {
      if (!OPENAI_KEY) return null;
      try {
        const payload = {
          companyNumber: companyNumber || '',
          companyName: companyName || '',
          cleanedName: cleaned,
          postcode: postcode || '',
          address: address || '',
          sicCodes,
          keyPhrases,
          directorNames,
          baseDomain,
          pages: pages.map(p => ({ url: p.url, title: p.title, meta: p.meta, text: p.text.slice(0, 4000), linkedin_candidates: filterLinkedIns(p.links) })).slice(0, 8)
        };
        const prompt = `You are verifying if a website's base domain belongs to a given UK company. Consider company number, legal vs trading names, SIC activities, postcode/address, and directors.

Return strict JSON with both decision certainty and ownership likelihood:
{
  "verdict": "yes" | "no" | "unsure",
  "decision_confidence": number,  // 0..1: confidence in the verdict
  "company_relevance": number,    // 0..1: likelihood the base domain is owned by the company
  "decision_rationale": string,
  "url": string,
  "trading_name"?: string,
  "linkedins"?: string[]
}`;
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${OPENAI_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'gpt-4o-mini',
            temperature: 0.1,
            response_format: { type: 'json_object' },
            messages: [
              { role: 'system', content: 'You produce concise, machine-readable JSON only.' },
              { role: 'user', content: prompt },
              { role: 'user', content: JSON.stringify(payload) }
            ]
          })
        });
        if (!res.ok) return null;
        const data: any = await res.json();
        try {
          const usage = (data as any)?.usage || null;
          const model = (data as any)?.model || (data as any)?.id || 'unknown';
          // No jobId here; log on this worker's job id via outer scope if desired later.
          await logEvent((job as any).id as string, 'info', 'LLM usage', { worker: 'company-discovery', model, usage });
        } catch {}
        const txt = data?.choices?.[0]?.message?.content || '{}';
        const obj = JSON.parse(txt);
        const decision_confidence = typeof obj.decision_confidence === 'number' ? Math.max(0, Math.min(1, obj.decision_confidence)) : 0;
        const company_relevance = typeof obj.company_relevance === 'number' ? Math.max(0, Math.min(1, obj.company_relevance)) : 0;
        const linkedins: string[] = Array.isArray(obj.linkedins) ? filterLinkedIns(obj.linkedins) : [];
        const verdict = (obj.verdict || '').toString().toLowerCase();
        const trading = typeof obj.trading_name === 'string' ? obj.trading_name : '';
        const rationale = typeof obj.decision_rationale === 'string' ? obj.decision_rationale : '';
        return { verdict, decision_confidence, company_relevance, linkedins, trading_name: trading, rationale };
      } catch {
        return null;
      }
    };

    // Evaluate candidate hosts by enqueuing per-host site-fetch jobs (actual verification happens in site-fetch worker)
    const potentials = Array.from(hosts);
    await logEvent(job.id as string, 'info', 'Filtered candidate hosts', { count: potentials.length, hosts: potentials });

    let enqueued = 0;
    for (const host of potentials) {
      const jobId = canonicalCompanyId
        ? `site:company:${canonicalCompanyId}:${host}`
        : `site:${resolvedRoot}:${host}`;
      const payload = {
        host,
        companyNumber,
        companyName,
        address,
        postcode,
        rootJobId: resolvedRoot,
        parentJobId: job.id as string,
        requestSource: requestSource || 'company-discovery',
        companyId: canonicalCompanyId,
      };
      // Mark as pending in job_progress so the workflow finalizer can wait on both pending and running
      try {
        await query(
          `INSERT INTO job_progress(job_id, queue, name, status, data, root_job_id, parent_job_id, request_source)
           VALUES ($1,'site-fetch','fetch','pending',$2,$3,$4,$5)
           ON CONFLICT (job_id)
           DO UPDATE SET status='pending',
                         data=$2,
                         root_job_id=$3,
                         parent_job_id=$4,
                         request_source=$5,
                         updated_at=now()`,
          [jobId, JSON.stringify(payload), resolvedRoot, job.id as string, requestSource || 'company-discovery']
        );
      } catch {}
      await siteFetchQ.add('fetch', payload, {
        jobId,
        attempts: 5,
        backoff: { type: 'exponential', delay: 2000 },
      });
      if (resolvedRoot) {
        try {
          await query(
            `UPDATE ch_people
                SET sitefetch_job_ids = (
                      SELECT ARRAY(SELECT DISTINCT UNNEST(COALESCE(ch_people.sitefetch_job_ids,'{}') || ARRAY[$1::text]))
                 )
              WHERE root_job_id = $2`,
            [jobId, resolvedRoot]
          );
        } catch (e) {
          await logEvent(job.id as string, 'warn', 'Failed to tag site-fetch job on people', {
            error: String(e),
            jobId,
            rootJobId: resolvedRoot,
          });
        }
      }
      enqueued++;
    }
    metadataPayload = { ...metadataPayload, enqueuedHosts: potentials };
    await logEvent(job.id as string, 'info', 'Enqueued site-fetch jobs', { scope: 'summary', enqueued, hosts: potentials });
    metadataPayload = { ...metadataPayload, serperCalls };
    try { await logEvent(job.id as string, 'info', 'Usage summary', { serper_calls: serperCalls }); } catch {}

    // If this workflow uses a rootJobId, and if all discovery work is finished (no running company-discovery or site-fetch),
    // kick off person-linkedin here as a safety net (e.g., when no sites were found for any company).
    if (resolvedRoot) {
      try {
        // Load expected companies for this workflow from the ch-appointments root progress record
        const { rows: progRows } = await query<{ data: any }>(
          `SELECT data FROM job_progress WHERE job_id = $1`,
          [resolvedRoot]
        );
        const rootData = progRows?.[0]?.data || null;
        const expectedCompanies: string[] = Array.isArray(rootData?.enqueued?.companies) ? rootData.enqueued.companies : [];

        // Count any running jobs under the root
        const { rows: runningAll } = await query<{ c: string }>(
          `SELECT COUNT(*)::int AS c
             FROM job_progress
            WHERE status IN ('pending','running')
              AND (queue = 'company-discovery' OR queue = 'site-fetch')
              AND COALESCE(root_job_id, data->>'rootJobId') = $1`,
          [resolvedRoot]
        );
        const runningCount = Number(runningAll?.[0]?.c || 0);

        // Completed company-discovery jobs count vs expected
        let completedCompanies = 0;
        if (expectedCompanies.length) {
          const { rows: compRows } = await query<{ c: string }>(
            `SELECT COUNT(DISTINCT data->>'companyNumber')::int AS c
               FROM job_progress
              WHERE queue = 'company-discovery'
                AND status = 'completed'
                AND COALESCE(root_job_id, data->>'rootJobId') = $1
                AND (data->>'companyNumber') = ANY($2::text[])`,
            [resolvedRoot, expectedCompanies]
          );
          completedCompanies = Number(compRows?.[0]?.c || 0);
        }

        if (runningCount === 0 && (!expectedCompanies.length || completedCompanies === expectedCompanies.length)) {
          // Enqueue person-linkedin for each person in this workflow with aggregated context across their appointments
          const { rows: people } = await query<{ id: number; first_name: string | null; middle_names: string | null; last_name: string | null; dob_string: string | null }>(
           `SELECT DISTINCT p.id, p.first_name, p.middle_names, p.last_name, p.dob_string
              FROM ch_people p
              WHERE p.root_job_id = $1`,
            [resolvedRoot]
          );
          let enq = 0;
          for (const p of people) {
            const fullName = [p.first_name, p.middle_names, p.last_name].filter(Boolean).join(' ');
            if (!fullName.trim()) continue;
            const aggSql = `
              WITH w AS (
                SELECT j.value AS v
                  FROM ch_appointments a,
                       LATERAL jsonb_array_elements(COALESCE(a.verified_company_website, '[]'::jsonb)) AS j(value)
                 WHERE a.person_id = $1
              ),
              lc AS (
                SELECT j.value AS v
                  FROM ch_appointments a,
                       LATERAL jsonb_array_elements(COALESCE(a.verified_company_linkedIns, '[]'::jsonb)) AS j(value)
                 WHERE a.person_id = $1
              ),
              lp AS (
                SELECT j.value AS v
                  FROM ch_appointments a,
                       LATERAL jsonb_array_elements(COALESCE(a.verified_director_linkedIns, '[]'::jsonb)) AS j(value)
                 WHERE a.person_id = $1
              )
              SELECT
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM w), '[]'::jsonb) AS websites,
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM lc), '[]'::jsonb) AS company_linkedins,
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM lp), '[]'::jsonb) AS personal_linkedins`;
            const { rows: aggRows } = await query<any>(aggSql, [Number((p as any).id)]);
            const aggWebsites: string[] = Array.isArray(aggRows?.[0]?.websites) ? aggRows[0].websites : [];
            const aggCompanyLIs: string[] = Array.isArray(aggRows?.[0]?.company_linkedins) ? aggRows[0].company_linkedins : [];
            const aggPersonalLIs: string[] = Array.isArray(aggRows?.[0]?.personal_linkedins) ? aggRows[0].personal_linkedins : [];

            const { rows: cnRows } = await query<{ company_name: string }>(
              `SELECT company_name
                 FROM ch_appointments a
                WHERE a.person_id = $1 AND COALESCE(company_name,'') <> ''
                ORDER BY updated_at DESC
                LIMIT 1`,
              [Number((p as any).id)]
            );
            const companyNameForContext = (cnRows?.[0]?.company_name || '').toString();

            const pjId = `person:${(p as any).id}:${resolvedRoot}`;
            const payload = {
              person: { firstName: p.first_name || '', middleNames: p.middle_names || '', lastName: p.last_name || '', dob: p.dob_string || '' },
              context: { companyName: companyNameForContext, websites: aggWebsites, companyLinkedIns: aggCompanyLIs, personalLinkedIns: aggPersonalLIs },
              rootJobId: resolvedRoot,
              parentJobId: job.id as string,
              requestSource: requestSource || 'company-discovery',
            };
            try {
              await query(
                `INSERT INTO job_progress(job_id, queue, name, status, data, root_job_id, parent_job_id, request_source)
                 VALUES ($1,'person-linkedin','discover','pending',$2,$3,$4,$5)
                 ON CONFLICT (job_id)
                 DO UPDATE SET status='pending',
                               data=$2,
                               root_job_id=$3,
                               parent_job_id=$4,
                               request_source=$5,
                               updated_at=now()`,
                [pjId, JSON.stringify(payload), resolvedRoot, job.id as string, requestSource || 'company-discovery']
              );
            } catch {}
            await personQ.add('discover', payload, {
              jobId: pjId,
              attempts: 5,
              backoff: { type: 'exponential', delay: 2000 },
            });
            enq++;
          }
          await logEvent(job.id as string, 'info', 'Enqueued person-linkedin searches after ALL discovery complete (no site-fetch jobs)', { rootJobId: resolvedRoot, people: people.length, enqueued: enq });
        }
      } catch (e) {
        await logEvent(job.id as string, 'error', 'Finalization check failed in company-discovery', { error: String(e), rootJobId: resolvedRoot });
      }
    }

    // site-fetch worker will persist results and enqueue person-linkedin after aggregation
    const finalStatus = enqueued === 0 ? 'completed' : 'running';
    const metadataJson = metadataPayload && Object.keys(metadataPayload).length ? JSON.stringify(metadataPayload) : null;
    if (canonicalCompanyId) {
      await query(
        `UPDATE ch_companies
            SET name = COALESCE($2, name),
                registered_address = COALESCE($3, registered_address),
                registered_postcode = COALESCE($4, registered_postcode),
                sic_codes = CASE WHEN $5::text[] IS NOT NULL AND CARDINALITY($5::text[]) > 0 THEN $5::text[] ELSE sic_codes END,
                metadata = CASE WHEN $6::jsonb IS NOT NULL THEN COALESCE(metadata, '{}'::jsonb) || $6::jsonb ELSE metadata END,
                discovery_status = $7,
                discovery_job_id = $8,
                discovery_error = NULL,
                first_discovered_at = CASE WHEN first_discovered_at IS NULL AND $7 = 'completed' THEN now() ELSE first_discovered_at END,
                last_discovered_at = CASE WHEN $7 = 'completed' THEN now() ELSE last_discovered_at END,
                updated_at = now()
          WHERE id = $1`,
        [
          canonicalCompanyId,
          companyName || null,
          address || null,
          postcode || null,
          sicCodes.length ? sicCodes : null,
          metadataJson,
          finalStatus,
          job.id as string,
        ]
      );
    }

    await completeJob(job.id as string, {
      companyId: canonicalCompanyId,
      companyNumber,
      companyName,
      candidates: potentials.length,
      enqueuedSiteFetch: enqueued,
      rootJobId: resolvedRoot,
      discoveryStatus: finalStatus,
    });
    logger.info({ companyId: canonicalCompanyId, companyNumber, candidates: potentials.length, enqueuedSiteFetch: enqueued }, 'Company discovery dispatched to site-fetch');
    return;
  } catch (err) {
    if (canonicalCompanyId) {
      try {
        await query(
          `UPDATE ch_companies
              SET discovery_status = 'failed',
                  discovery_error = $2,
                  discovery_job_id = $3,
                  updated_at = now()
            WHERE id = $1`,
          [canonicalCompanyId, String(err), job.id as string]
        );
      } catch {}
    }
    await failJob(job.id as string, err);
    logger.error({ companyNumber, err: String(err) }, "Company discovery failed");
    throw err;
  }
}, { connection, concurrency: 1 });
