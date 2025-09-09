import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { personQ } from "../queues/index.js";
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
const genericHosts:Set<string> = new Set(
  (JSON.parse(fs.readFileSync(genericHostsPath, "utf-8")) as string[]).map(s => s.toLowerCase())
);
const endingsPath = path.join(process.cwd(), "config", "commonUrlEndings.json");
const COMMON_ENDINGS: string[] = JSON.parse(fs.readFileSync(endingsPath, 'utf-8'));
const sicMapPath = path.join(process.cwd(), "config", "sicCodes.json");
const sicEntries: Array<{ code: number; key_phrases?: string[]; description?: string }> = JSON.parse(fs.readFileSync(sicMapPath, 'utf-8'));
const sicToPhrases = new Map<string, string[]>(sicEntries.map(e => [String(e.code), Array.isArray(e.key_phrases) ? e.key_phrases : []]));

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
    const queries: string[] = [];
    const append = (q: string) => { if (q && q.trim()) queries.push(q.trim()); };
    append(`${companyName} UK`);
    for (const kp of keyPhrases.slice(0, 5)) append(`${companyName} ${kp} UK`);
    append(`${cleaned} UK`);
    for (const kp of keyPhrases.slice(0, 5)) append(`${cleaned} ${kp} UK`);
    if (postcode) append(`${cleaned} ${postcode} UK`);
    if (address) append(`${cleaned} ${String(address).split(',')[0]} UK`);
    await logEvent(job.id as string, 'info', 'Built queries', { queries, sicCodes, keyPhrases, directorNames });

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
      const organic = asArray((data as any).organic).slice(0, 10);
      const peopleAlso = asArray((data as any).peopleAlsoSearch).slice(0, 10);
      const knowledge = asArray((data as any).knowledgeGraph).slice(0, 10);
      const items = [...organic, ...peopleAlso, ...knowledge];
      await logEvent(job.id as string, 'debug', 'Serper results', {
        q,
        counts: { organic: organic.length, peopleAlsoSearch: peopleAlso.length, knowledgeGraph: knowledge.length }
      });
      for (const it of items) {
        const url = it.link || it.url || "";
        if (!url) continue;
        const h = baseHost(url);
        if (!h || isGeneric(h)) continue;
        hosts.add(h);
      }
      await new Promise(r => setTimeout(r, 1000));
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
        const prompt = `You are verifying if a website's base domain belongs to a given UK company. Consider company number, legal vs trading names, SIC activities, postcode/address, and directors. Decide with high precision if the base domain is owned by the company. Return strict JSON: { verdict: "yes"|"no"|"uncertain", url: string, confidence_rating: 0..1, confidence_rationale: string, trading_name?: string, linkedins?: string[] }.`;
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
        const txt = data?.choices?.[0]?.message?.content || '{}';
        const obj = JSON.parse(txt);
        const rating = typeof obj.confidence_rating === 'number' ? Math.max(0, Math.min(1, obj.confidence_rating)) : 0;
        const linkedins: string[] = Array.isArray(obj.linkedins) ? filterLinkedIns(obj.linkedins) : [];
        const verdict = (obj.verdict || '').toString().toLowerCase();
        const trading = typeof obj.trading_name === 'string' ? obj.trading_name : '';
        const rationale = typeof obj.confidence_rationale === 'string' ? obj.confidence_rationale : '';
        return { verdict, rating, linkedins, trading_name: trading, rationale };
      } catch {
        return null;
      }
    };

    // Evaluate candidate hosts
    const potentials = Array.from(hosts);
    await logEvent(job.id as string, 'info', 'Filtered candidate hosts', { count: potentials.length });

    const verifiedSites: string[] = [];
    const websiteVerifications: Array<{ url: string; confidence_rating: number; confidence_rationale: string }> = [];
    const verifiedCompanyLIs = new Set<string>();
    const verifiedPersonalLIs = new Set<string>();
    let savedTradingName: string | null = null;

    outer: for (const host of potentials) {
      const base = `https://${host}`;
      const pages: Array<{ url: string; title: string; meta: string; text: string; links: string[] }> = [];
      for (const ending of COMMON_ENDINGS) {
        const url = base + ending;
        const r = await fetchStatic(url);
        if (!r.ok) continue;
        // ScrapingBee fetch for richer content
        const bee = await fetchBeeHtml(url);
        const html = bee.status >= 200 && bee.status < 400 && bee.html.length > 200 ? bee.html : r.html;
        const sig = parseSignals(html);
        pages.push({ url, title: sig.title, meta: sig.metaDesc, text: sig.text, links: sig.links });
        // Collect LinkedIns from page
        for (const li of filterLinkedIns(sig.links)) {
          if (isCompanyLI(li)) verifiedCompanyLIs.add(li);
          if (isPersonalLI(li)) verifiedPersonalLIs.add(li);
        }
        // Evaluate via LLM once we have at least one solid page
        if (pages.length >= 1) {
          const decision = await llmJudge(host, pages);
          if (decision) {
            // Early stop conditions
            if (decision.verdict === 'yes' && decision.rating >= 0.85) {
              verifiedSites.push(host);
              websiteVerifications.push({ url: host, confidence_rating: decision.rating, confidence_rationale: decision.rationale });
              decision.linkedins.forEach(u => { if (isCompanyLI(u)) verifiedCompanyLIs.add(u); if (isPersonalLI(u)) verifiedPersonalLIs.add(u); });
              if (decision.trading_name) savedTradingName = decision.trading_name;
              await logEvent(job.id as string, 'info', 'LLM accepted domain (certain)', { host, rating: decision.rating });
              break outer;
            }
            if (decision.verdict === 'no' && decision.rating <= 0.15) {
              await logEvent(job.id as string, 'info', 'LLM rejected domain (certain)', { host, rating: decision.rating });
              break; // stop checking other endings for this host
            }
            if (decision.verdict === 'yes' && decision.rating >= 0.6) {
              verifiedSites.push(host);
              websiteVerifications.push({ url: host, confidence_rating: decision.rating, confidence_rationale: decision.rationale });
              decision.linkedins.forEach(u => { if (isCompanyLI(u)) verifiedCompanyLIs.add(u); if (isPersonalLI(u)) verifiedPersonalLIs.add(u); });
              if (decision.trading_name) savedTradingName = decision.trading_name;
              await logEvent(job.id as string, 'info', 'LLM accepted domain', { host, rating: decision.rating });
              // Keep looking at next host but we could also early exit all if you want only one
              break; // stop more endings for this host
            }
            // accumulate candidates and continue with more endings for better evidence
          }
        }
        await new Promise(r => setTimeout(r, 250));
      }
    }

    // Persist results into ch_appointments
    if (verifiedSites.length || verifiedCompanyLIs.size || verifiedPersonalLIs.size || savedTradingName || websiteVerifications.length) {
      try {
        const addSites = JSON.stringify(verifiedSites);
        const addCompanyLIs = JSON.stringify(Array.from(verifiedCompanyLIs));
        const addPersonalLIs = JSON.stringify(Array.from(verifiedPersonalLIs));
        const addVerif = JSON.stringify(websiteVerifications);
        await query(
          `UPDATE ch_appointments SET
             verified_company_website = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(verified_company_website,'[]'::jsonb))='array'
                           THEN (
                  SELECT jsonb_agg(DISTINCT j.value) FROM jsonb_array_elements(COALESCE(verified_company_website,'[]'::jsonb) || $1::jsonb) AS j(value)
                           ) ELSE $1::jsonb END),
             company_website_verification = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(company_website_verification,'[]'::jsonb))='array'
                           THEN (
                 SELECT jsonb_agg(DISTINCT j.value) FROM jsonb_array_elements(COALESCE(company_website_verification,'[]'::jsonb) || $3::jsonb) AS j(value)
                           ) ELSE $3::jsonb END),
             verified_company_linkedIns = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(verified_company_linkedIns,'[]'::jsonb))='array'
                           THEN (
                  SELECT jsonb_agg(DISTINCT j.value) FROM jsonb_array_elements(COALESCE(verified_company_linkedIns,'[]'::jsonb) || $2::jsonb) AS j(value)
                            ) ELSE $2::jsonb END),
             verified_director_linkedIns = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(verified_director_linkedIns,'[]'::jsonb))='array'
                           THEN (
                 SELECT jsonb_agg(DISTINCT j.value) FROM jsonb_array_elements(COALESCE(verified_director_linkedIns,'[]'::jsonb) || $5::jsonb) AS j(value)
                           ) ELSE $5::jsonb END),
             trading_name = COALESCE(trading_name, $4),
             updated_at = now()
           WHERE company_number = $6`,
          [addSites, addCompanyLIs, addVerif, savedTradingName || null, addPersonalLIs, companyNumber]
        );
        await logEvent(job.id as string, 'info', 'Persisted discovery results', { sites: verifiedSites.length, verifications: websiteVerifications.length, company_linkedins: verifiedCompanyLIs.size, personal_linkedins: verifiedPersonalLIs.size, trading_name: savedTradingName || null });
      } catch (e) {
        await logEvent(job.id as string, 'error', 'Failed to persist discovery results', { error: String(e) });
      }
    }

    // Enqueue person-linkedin with context when we have results
    try {
      const { rows: people } = await query<{ id: number; first_name: string | null; middle_names: string | null; last_name: string | null; dob_string: string | null }>(
        `SELECT DISTINCT p.id, p.first_name, p.middle_names, p.last_name, p.dob_string
           FROM ch_people p JOIN ch_appointments a ON a.person_id = p.id
          WHERE a.company_number = $1`,
        [companyNumber]
      );
      const companyNameCtx = (companyName || '').toString();
      let enq = 0;
      for (const p of people) {
        const fullName = [p.first_name, p.middle_names, p.last_name].filter(Boolean).join(' ');
        if (!fullName.trim()) continue;
        const jobId = `person:${p.id}:${companyNumber}`;
        await personQ.add('discover', {
          person: { firstName: p.first_name || '', middleNames: p.middle_names || '', lastName: p.last_name || '', dob: p.dob_string || '' },
          context: { companyNumber, companyName: companyNameCtx, websites: verifiedSites, companyLinkedIns: Array.from(verifiedCompanyLIs), personalLinkedIns: Array.from(verifiedPersonalLIs) }
        }, { jobId, attempts: 5, backoff: { type: 'exponential', delay: 2000 } });
        enq++;
      }
      await logEvent(job.id as string, 'info', 'Enqueued person-linkedin after discovery', { enqueued: enq });
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Failed to enqueue person-linkedin after discovery', { error: String(e) });
    }

    await completeJob(job.id as string, { companyNumber, companyName, candidates: potentials.length, verifiedSites, verifiedCompanyLinkedIns: Array.from(verifiedCompanyLIs), verifiedPersonalLinkedIns: Array.from(verifiedPersonalLIs), trading_name: savedTradingName || null });
    logger.info({ companyNumber, candidates: potentials.length, verifiedSites: verifiedSites.length }, 'Company discovery done');
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ companyNumber, err: String(err) }, "Company discovery failed");
    throw err;
  }
}, { connection, concurrency: 1 });
