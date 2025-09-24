import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { personQ, siteFetchQ } from "../queues/index.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { logger } from "../lib/logger.js";
import { fetch } from "undici";
import { cleanCompanyName, baseHost } from "../lib/normalize.js";
import { query } from "../lib/db.js";
import fs from "fs";
import path from "path";
import pLimit from "p-limit";

type JobPayload = {
  host: string;
  companyNumber: string;
  companyName: string;
  address?: string;
  postcode?: string;
  rootJobId?: string;
};

const BEE_KEY = process.env.SCRAPINGBEE_API_KEY || "";
const OPENAI_KEY = process.env.OPENAI_API_KEY || "";
const SERPER_API_KEY = process.env.SERPER_API_KEY || "";
const BEE_MAX_PAGES = Number(process.env.SITEFETCH_BEE_MAX_PAGES || 1);
const STATIC_TIMEOUT_MS = Number(process.env.SITEFETCH_STATIC_TIMEOUT_MS || 7000);
const LLM_ONLY = (process.env.LLM_ONLY_WEBSITE_VALIDATION || "").toLowerCase() === 'true';
const ACCEPT_THRESHOLD = Number(process.env.SITEFETCH_ACCEPT_THRESHOLD || 0.75);
const MAX_PAGES_FOR_LLM = Number(process.env.SITEFETCH_MAX_PAGES || 5);
const SNIPPET_CHARS = Number(process.env.SITEFETCH_MAX_SNIPPET_CHARS || 800);
const LLM_DEBUG_LOGS = (process.env.LLM_DEBUG_LOGS || "").toLowerCase() === 'true';
const SITEFETCH_MODE = (process.env.SITEFETCH_MODE || 'static_first').toLowerCase();
const USE_STATIC_FETCH = SITEFETCH_MODE !== 'bee_only';

const SITEMAP_KEYWORDS = [
  'privacy',
  'terms',
  'legal',
  'cookies',
  'policy',
  'impressum',
  'gdpr',
  'compliance',
  'regulation'
];

const DEFAULT_PATHS = [
  '/',
  '/privacy',
  '/privacy-policy',
  '/terms',
  '/terms-of-service',
  '/terms-and-conditions',
  '/legal',
  '/impressum',
  '/cookies',
  '/data-policy',
  '/contact',
  '/about'
];

const SHOPIFY_PATHS = [
  '/',
  '/policies/privacy-policy',
  '/policies/terms-of-service',
  '/policies/refund-policy',
  '/policies/shipping-policy',
  '/pages/privacy-policy',
  '/pages/terms-of-service',
  '/pages/contact',
  '/pages/about-us'
];

await initDb();

function buildBeeUrl(target: string, opts?: { blockResources?: boolean; waitMs?: number; waitFor?: string }) {
  const u = new URL("https://app.scrapingbee.com/api/v1/");
  u.searchParams.set("api_key", BEE_KEY);
  u.searchParams.set("url", target);
  u.searchParams.set("render_js", "true");
  u.searchParams.set("country_code", "gb");
  const block = typeof opts?.blockResources === 'boolean' ? opts.blockResources : true;
  u.searchParams.set("block_resources", block ? "true" : "false");
  // modest timeout
  u.searchParams.set("timeout", "15000");
  if (opts?.waitMs && opts.waitMs > 0) u.searchParams.set("wait", String(opts.waitMs));
  if (opts?.waitFor && opts.waitFor.trim()) u.searchParams.set("wait_for", opts.waitFor.trim());
  return u.toString();
}

async function fetchBeeHtml(targetUrl: string, preferFull = false, waitMs?: number, waitFor?: string): Promise<{ status: number; html: string }> {
  if (!BEE_KEY) throw new Error("Missing SCRAPINGBEE_API_KEY");
  const beeUrl = buildBeeUrl(targetUrl, { blockResources: !preferFull, waitMs, waitFor });
  const res = await fetch(beeUrl, { method: "GET" });
  const html = await res.text();
  return { status: res.status, html };
}

async function fetchStaticHtml(targetUrl: string): Promise<{ status: number; html: string; contentType: string | null }> {
  try {
    const res = await fetch(targetUrl, {
      method: "GET",
      redirect: "follow" as any,
      headers: { "accept": "text/html,application/xhtml+xml" },
      signal: (AbortSignal as any).timeout ? (AbortSignal as any).timeout(STATIC_TIMEOUT_MS) : undefined
    } as any);
    const ct = res.headers.get("content-type");
    const text = await res.text();
    return { status: res.status, html: text || "", contentType: ct };
  } catch {
    return { status: 0, html: "", contentType: null };
  }
}

async function fetchText(url: string): Promise<{ ok: boolean; status: number; text: string; contentType: string | null }> {
  try {
    const res = await fetch(url, { method: 'GET', redirect: 'follow' } as any);
    const text = await res.text();
    return { ok: res.status >= 200 && res.status < 400, status: res.status, text, contentType: res.headers.get('content-type') };
  } catch {
    return { ok: false, status: 0, text: '', contentType: null };
  }
}

function extractSitemapUrlsFromRobots(robots: string): string[] {
  const urls: string[] = [];
  const lines = robots.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    if (trimmed.toLowerCase().startsWith('sitemap:')) {
      const value = trimmed.slice(8).trim();
      if (value) urls.push(value);
    }
  }
  return urls;
}

function looksLikeShopify(html: string): boolean {
  const lower = html.toLowerCase();
  return (
    lower.includes('name="generator" content="shopify"') ||
    lower.includes('cdn.shopify.com') ||
    lower.includes('shopify-checkout-api-token') ||
    lower.includes('shopify-digital-wallet') ||
    /id="shopify-section-[^"]+"/.test(lower) ||
    lower.includes('window.shopify')
  );
}

function extractLocs(xml: string): string[] {
  const locs: string[] = [];
  const regex = /<loc>([^<]+)<\/loc>/gi;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(xml)) !== null) {
    locs.push(match[1].trim());
  }
  return locs;
}

async function gatherSitemapCandidates(host: string, opts: { jobId: string | number; useSerper: boolean }): Promise<{ urls: string[]; source: string }> {
  const discovered = new Set<string>();
  const queue: string[] = [];
  const origins = [`https://${host}`, `http://${host}`];

  for (const origin of origins) {
    const robotsUrl = `${origin}/robots.txt`;
    const res = await fetchText(robotsUrl);
    if (res.ok && res.text) {
      for (const url of extractSitemapUrlsFromRobots(res.text)) {
        if (!discovered.has(url)) {
          discovered.add(url);
          queue.push(url);
        }
      }
    }
    if (queue.length) return { urls: queue.slice(0, 10), source: 'robots' };
  }

  const defaults = ['/sitemap.xml', '/sitemap_index.xml', '/sitemap-index.xml', '/sitemap1.xml', '/sitemap/sitemap.xml'];
  for (const path of defaults) {
    const candidate = `https://${host}${path}`;
    if (!discovered.has(candidate)) {
      discovered.add(candidate);
      queue.push(candidate);
    }
    if (queue.length >= 5) break;
  }
  if (queue.length) return { urls: queue.slice(0, 10), source: 'defaults' };

  if (opts.useSerper && SERPER_API_KEY) {
    const query = `site:${host} filetype:xml`;
    const data = await serperSearch(query);
    const items: any[] = Array.isArray(data.data?.organic) ? data.data.organic : [];
    const xmlLinks = items
      .map(item => (item?.link || item?.url || '').toString())
      .filter(link => /\.xml(\?.*)?$/.test(link));
    for (const link of xmlLinks) {
      if (!discovered.has(link)) {
        discovered.add(link);
        queue.push(link);
      }
      if (queue.length >= 5) break;
    }
    await logEvent(opts.jobId as string, 'info', 'Serper sitemap search', {
      query,
      candidates: queue.length,
      sample: queue.slice(0, 3)
    });
    if (queue.length) return { urls: queue.slice(0, 10), source: 'serper_xml' };
  }

  return { urls: [], source: 'none' };
}

async function collectSitemapUrls(host: string, opts: { jobId: string | number; useSerper: boolean } ): Promise<{ paths: string[]; source: string }> {
  const visited = new Set<string>();
  const gathered = await gatherSitemapCandidates(host, opts);
  const queue = [...gathered.urls];
  const results = new Set<string>();
  const maxSitemaps = 5;
  const maxUrls = 100;

  while (queue.length && visited.size < maxSitemaps && results.size < maxUrls) {
    const sitemapUrl = queue.shift()!;
    if (visited.has(sitemapUrl)) continue;
    visited.add(sitemapUrl);
    const res = await fetchText(sitemapUrl);
    if (!res.ok || !res.text) continue;

    const xml = res.text;
    const locs = extractLocs(xml);
    if (!locs.length) continue;

    if (/<sitemapindex/i.test(xml)) {
      for (const loc of locs.slice(0, 5)) {
        if (!visited.has(loc) && queue.length < maxSitemaps) queue.push(loc);
      }
      continue;
    }

    for (const loc of locs) {
      if (results.size >= maxUrls) break;
      try {
        const url = new URL(loc);
        const normalizedHost = url.hostname.replace(/^www\./, '');
        if (normalizedHost !== host.replace(/^www\./, '')) continue;
        results.add(url.pathname || '/');
      } catch {
        continue;
      }
    }
  }

  return { paths: Array.from(results), source: gathered.source };
}

function filterSitemapPaths(paths: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const path of paths) {
    const lower = path.toLowerCase();
    if (!SITEMAP_KEYWORDS.some(k => lower.includes(k))) continue;
    if (!seen.has(lower)) {
      seen.add(lower);
      out.push(path);
    }
  }
  return out;
}

function dedupePaths(paths: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const path of paths) {
    const norm = path.startsWith('/') ? path : `/${path}`;
    const clean = norm.replace(/\/+/g, '/');
    if (!seen.has(clean)) {
      seen.add(clean);
      out.push(clean);
    }
  }
  return out;
}

function extractBetween(html: string, re: RegExp): string[] {
  const out: string[] = [];
  let m: RegExpExecArray | null;
  const r = new RegExp(re.source, re.flags.includes('g') ? re.flags : re.flags + 'g');
  while ((m = r.exec(html))) {
    out.push((m[1] || "").trim());
  }
  return out;
}

async function serperSearch(q: string): Promise<{ status: number; headers: Record<string,string>; data: any }> {
  if (!SERPER_API_KEY) return { status: 0, headers: {}, data: {} };
  const res = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ q, gl: "uk", hl: "en", autocorrect: true, num: 10 })
  });
  const status = res.status;
  const headers: Record<string,string> = {};
  try { res.headers.forEach((v,k)=>{ headers[k]=v; }); } catch {}
  const data = status >= 200 && status < 400 ? await res.json() : {};
  return { status, headers, data };
}

function stripTags(s: string): string {
  return s.replace(/<script[\s\S]*?<\/script>/gi, " ")
          .replace(/<style[\s\S]*?<\/style>/gi, " ")
          .replace(/<[^>]+>/g, " ")
          .replace(/\s+/g, " ")
          .trim();
}

function parseHtmlSignals(html: string) {
  const title = extractBetween(html, /<title[^>]*>([\s\S]*?)<\/title>/i)[0] || "";
  const metaDesc = extractBetween(html, /<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["'][^>]*>/i)[0] || "";
  const h1s = extractBetween(html, /<h1[^>]*>([\s\S]*?)<\/h1>/gi).map(stripTags).filter(Boolean);
  const jsonldRaw = extractBetween(html, /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi);
  const jsonld: any[] = [];
  for (const j of jsonldRaw) {
    try {
      const obj = JSON.parse(j);
      if (Array.isArray(obj)) jsonld.push(...obj);
      else jsonld.push(obj);
    } catch {}
  }
  const textChunk = stripTags(html).slice(0, 20000).toLowerCase();
  const textStrict = textChunk.replace(/[^a-z0-9]/g, "");
  const emails = Array.from(new Set((html.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || []).map(e => e.toLowerCase())));
  const linkedins = Array.from(new Set((html.match(/https?:\/\/(?:[\w.-]*\.)?linkedin\.com\/[\w\-\/\?=&#.%]+/gi) || [])));
  return { title, metaDesc, h1s, jsonld, textChunk, textStrict, emails, linkedins };
}

function normalize(s: string): string {
  return (s || "").toString().toLowerCase().replace(/\s+/g, ' ').trim();
}

function normalizeStrict(s: string): string {
  return (s || "").toString().toLowerCase().replace(/[^a-z0-9]/g, '').trim();
}

function extractCompanyNumbers(text: string): string[] {
  // Capture CRNs in common forms: 8 digits, or 2 letters + 6 digits, allowing spaces
  const out = new Set<string>();
  const re = /([A-Z]{2}\s*\d[\d\s]{5,7}|\b\d[\d\s]{7,9}\b)/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text))) {
    const raw = (m[1] || m[0]).toUpperCase().replace(/\s+/g, '');
    if (/^[A-Z]{2}\d{6}$/.test(raw) || /^\d{8}$/.test(raw)) out.add(raw);
  }
  return Array.from(out);
}

function nameLike(a: string, b: string) {
  const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const na = norm(a);
  const nb = norm(b);
  if (!na || !nb) return false;
  // require all tokens of cleaned company name to appear in b
  const tokens = na.split(/\s+/).filter(Boolean);
  return tokens.every(t => nb.includes(t));
}

function apexOf(host: string) {
  // simple heuristic: keep last two labels; fine for common UK/COM, not perfect
  const parts = host.toLowerCase().split(".");
  if (parts.length <= 2) return host.toLowerCase();
  return parts.slice(-2).join(".");
}

function scoreOwnership(opts: {
  host: string;
  companyName: string;
  companyNumber?: string;
  postcode?: string;
  signals: ReturnType<typeof parseHtmlSignals>;
}) {
  const { host, companyName, companyNumber, postcode, signals } = opts;
  const cleanedName = cleanCompanyName(companyName);
  const titleHit = nameLike(cleanedName, signals.title) ? 1 : 0;
  const h1Hit = signals.h1s.some(h => nameLike(cleanedName, h)) ? 1 : 0;
  const metaHit = nameLike(cleanedName, signals.metaDesc) ? 1 : 0;
  const numHit = companyNumber ? (signals.textStrict.includes((companyNumber || '').replace(/[^a-z0-9]/gi, '').toLowerCase()) ? 1 : 0) : 0;
  const pcHit = postcode ? (signals.textChunk.includes(postcode.toLowerCase()) ? 1 : 0) : 0;
  const bodyNameHit = nameLike(cleanedName, signals.textChunk) ? 1 : 0;
  const apex = apexOf(baseHost(host));
  const emailHit = signals.emails.some(e => {
    const dom = e.split("@")[1] || "";
    return dom === apex || dom.endsWith("." + apex);
  }) ? 1 : 0;
  let orgHit = 0;
  for (const j of signals.jsonld) {
    const t = (j && (j["@type"] || j["@type"])) || null;
    const types = Array.isArray(t) ? t : (t ? [t] : []);
    if (types.map(String).map(s=>s.toLowerCase()).includes("organization")) {
      const nm = String((j.name || j["legalName"] || ""));
      if (nameLike(cleanedName, nm)) { orgHit = 1; break; }
    }
  }

  // weights sum up to <= 1.0
  const score = (
    0.22 * titleHit +
    0.18 * h1Hit +
    0.12 * metaHit +
    0.30 * numHit +
    0.10 * pcHit +
    0.10 * emailHit +
    0.25 * orgHit +
    0.08 * bodyNameHit
  );
  const matches: string[] = [];
  if (titleHit) matches.push("name_in_title");
  if (h1Hit) matches.push("name_in_h1");
  if (metaHit) matches.push("name_in_meta");
  if (numHit) matches.push("company_number");
  if (pcHit) matches.push("postcode");
  if (emailHit) matches.push("email_domain_match");
  if (orgHit) matches.push("jsonld_organization_name");
  if (bodyNameHit) matches.push("name_in_body");
  const rationale = matches.length ? `Matched: ${matches.join(", ")}` : "No strong ownership signals found";
  return { score: Math.min(1, score), rationale };
}

function findLabeledCompanyNumber(text: string): string | null {
  // Look for label + CRN with spaces allowed in number
  const label = /(company\s*(registration|reg\.?|number|no\.?))\b|registered\s*(number|no\.?)/i;
  const idx = text.search(label);
  if (idx === -1) return null;
  const tail = text.slice(idx, idx + 200);
  const m = tail.match(/([A-Z]{2}\s*\d[\d\s]{5,7}|\b\d[\d\s]{7,9}\b)/i);
  if (!m) return null;
  const raw = (m[1] || m[0]).toUpperCase().replace(/\s+/g, '');
  if (/^[A-Z]{2}\d{6}$/.test(raw) || /^\d{8}$/.test(raw)) return raw;
  return null;
}

async function llmFallback(host: string, htmlSample: string, companyName: string): Promise<{ score: number; rationale: string } | null> {
  if (!OPENAI_KEY) return null;
  try {
    const prompt = `You are verifying if a website likely belongs to a given UK company.\nCompany name: ${companyName}\nHostname: ${host}\nHTML sample (truncated):\n${htmlSample.slice(0, 4000)}\n\nRespond with a JSON object: {"score": number from 0 to 1, "rationale": string}. Score > 0.6 means likely owned by the company.`;
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENAI_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You produce concise, machine-readable answers." },
          { role: "user", content: prompt }
        ],
        temperature: 0.2,
        response_format: { type: "json_object" }
      })
    });
    if (!res.ok) return null;
    const data: any = await res.json();
    const txt = data?.choices?.[0]?.message?.content || "";
    const obj = JSON.parse(txt);
    const score = typeof obj.score === 'number' ? Math.max(0, Math.min(1, obj.score)) : 0;
    const rationale = typeof obj.rationale === 'string' ? obj.rationale : "";
    return { score, rationale };
  } catch {
    return null;
  }
}

async function llmEvaluateHost(params: {
  host: string;
  companyName: string;
  companyNumber?: string;
  postcode?: string;
  jobId?: string;
  pages: Array<{
    url: string;
    title: string;
    meta: string;
    h1: string[];
    jsonld_org_name?: string;
    company_numbers: string[];
    postcodes: string[];
    emails: string[];
    linkedins: string[];
    text_snippet: string;
  }>;
}): Promise<{ verdict: string; decision_confidence: number; company_relevance: number; rationale: string; linkedins: string[]; trading_name?: string } | null> {
  if (!OPENAI_KEY) return null;
  const payload = {
    company: {
      name: params.companyName,
      number: params.companyNumber || '',
      postcode: params.postcode || ''
    },
    host: params.host,
    evidence: params.pages
  };
  const prompt = `You judge if the website belongs to the given UK company. Use the structured evidence only.

Strict rules:
- Do NOT answer "yes" based only on brand/name similarity or the mere presence of a LinkedIn page.
- Prefer hard evidence such as: exact Companies House number match; exact legal name in JSON-LD; full registered address; matching email domain on the site; or a clear on‑site statement tying the legal entity to the domain.
- If hard evidence is absent, prefer "no" or "unsure" even if the names look similar.

Return strict JSON with both decision certainty and ownership likelihood:
{
  "verdict": "yes" | "no" | "unsure",
  "decision_confidence": number,  // 0..1: how confident you are in the verdict
  "company_relevance": number,    // 0..1: likelihood the domain is owned by the company
  "decision_rationale": string,   // concise reasoning citing strongest evidence
  "linkedins": string[],          // any related LinkedIn URLs found in evidence
  "trading_name"?: string         // present if a trading name is more appropriate
}`;
  try {
    if (LLM_DEBUG_LOGS && params.jobId) {
      await logEvent(params.jobId, 'debug', 'LLM evaluate request', {
        host: params.host,
        company: { name: params.companyName, number: params.companyNumber || '', postcode: params.postcode || '' },
        pages: params.pages.map(p => ({ url: p.url, title: p.title, jsonld_org_name: p.jsonld_org_name || '',
                                        nums: p.company_numbers.length, postcodes: p.postcodes.length,
                                        emails: p.emails.length, linkedins: p.linkedins.length }))
      });
    }
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": `Bearer ${OPENAI_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0.1,
        response_format: { type: "json_object" },
        messages: [
          { role: "system", content: "You produce concise, machine-readable answers." },
          { role: "user", content: prompt },
          { role: "user", content: JSON.stringify(payload) }
        ]
      })
    });
    if (!res.ok) return null;
    const data: any = await res.json();
    try {
      const usage = (data as any)?.usage || null;
      const model = (data as any)?.model || (data as any)?.id || 'unknown';
      if (usage && params.jobId) {
        await logEvent(params.jobId, 'info', 'LLM usage', { worker: 'site-fetch', model, usage });
      }
    } catch {}
    const txt = data?.choices?.[0]?.message?.content || '{}';
    const obj = JSON.parse(txt);
    const decision_confidence = typeof obj.decision_confidence === 'number' ? Math.max(0, Math.min(1, obj.decision_confidence)) : 0;
    const company_relevance = typeof obj.company_relevance === 'number' ? Math.max(0, Math.min(1, obj.company_relevance)) : 0;
    const linkedins: string[] = Array.isArray(obj.linkedins) ? obj.linkedins.filter((s: any) => typeof s === 'string') : [];
    const verdict = (obj.verdict || '').toString().toLowerCase();
    const trading_name = typeof obj.trading_name === 'string' ? obj.trading_name : undefined;
    const rationale = typeof obj.decision_rationale === 'string' ? obj.decision_rationale : '';
    if (LLM_DEBUG_LOGS && params.jobId) {
      await logEvent(params.jobId, 'debug', 'LLM evaluate response', {
        host: params.host,
        verdict, decision_confidence, company_relevance,
        rationale: rationale.slice(0, 400),
        linkedins_count: linkedins.length,
        trading_name: trading_name || ''
      });
    }
    return { verdict, decision_confidence, company_relevance, rationale, linkedins, trading_name };
  } catch {
    return null;
  }
}

export default new Worker("site-fetch", async job => {
  const { host, companyNumber, companyName, address, postcode } = job.data as JobPayload;
  await startJob({ jobId: job.id as string, queue: 'site-fetch', name: job.name, payload: job.data });
  try {
    if (companyNumber) {
      await query(
        `UPDATE ch_people AS p
            SET sitefetch_job_ids = (
                  SELECT ARRAY(SELECT DISTINCT UNNEST(COALESCE(p.sitefetch_job_ids,'{}') || ARRAY[$1::text]))
             ),
                updated_at = now()
          FROM ch_appointments a
         WHERE a.person_id = p.id AND a.company_number = $2`,
        [job.id as string, companyNumber]
      );
    }
  } catch (e) {
    await logEvent(job.id as string, 'warn', 'Failed to tag site-fetch job on people', { error: String(e), companyNumber });
  }
  try {
    const httpsBase = `https://${host}`;
    const homeUrl = `${httpsBase}/`;
    let candidatePaths: string[] = [];
    let cachedHome: { status: number; html: string; contentType: string | null } | null = null;
    let pathSource: string = 'default';

    try {
      const sitemapResult = await collectSitemapUrls(host, { jobId: job.id as string, useSerper: true });
      const filtered = filterSitemapPaths(sitemapResult.paths);
      if (filtered.length) {
        candidatePaths = dedupePaths(['/', ...filtered.slice(0, 50)]);
        pathSource = `sitemap-${sitemapResult.source}`;
      }
    } catch (e) {
      await logEvent(job.id as string, 'debug', 'Sitemap discovery failed', { host, error: String(e) });
    }

    if (!candidatePaths.length && SERPER_API_KEY) {
      try {
        const query = `site:${host}`;
        const data = await serperSearch(query);
        const items: any[] = Array.isArray(data.data?.organic) ? data.data.organic : [];
        const legalLinks: string[] = [];
        for (const item of items.slice(0, 20)) {
          const link = (item?.link || item?.url || '').toString();
          if (!link) continue;
          try {
            const u = new URL(link);
            if (u.hostname.replace(/^www\./, '') !== host.replace(/^www\./, '')) continue;
            const path = u.pathname || '/';
            if (SITEMAP_KEYWORDS.some(k => path.toLowerCase().includes(k)) && legalLinks.length < 30) {
              legalLinks.push(path);
            }
          } catch {}
        }
        if (legalLinks.length) {
          candidatePaths = dedupePaths(['/', ...legalLinks]);
          pathSource = 'serper_site';
        }
        await logEvent(job.id as string, 'info', 'Serper site link discovery', { host, query, collected: legalLinks.length, sample: legalLinks.slice(0, 3) });
      } catch (e) {
        await logEvent(job.id as string, 'debug', 'Serper site link failed', { host, error: String(e) });
      }
    }

    if (!candidatePaths.length) {
      if (USE_STATIC_FETCH) {
        cachedHome = await fetchStaticHtml(homeUrl);
        const isShopify = cachedHome.status >= 200 && cachedHome.status < 400 && cachedHome.html ? looksLikeShopify(cachedHome.html) : false;
        candidatePaths = dedupePaths(isShopify ? SHOPIFY_PATHS : DEFAULT_PATHS);
        pathSource = isShopify ? 'shopify-fallback' : 'default-fallback';
      } else {
        candidatePaths = dedupePaths(DEFAULT_PATHS);
        pathSource = 'default-fallback';
      }
    }

    const urls = candidatePaths.map(path => {
      const clean = path === '/' ? '/' : path.startsWith('/') ? path : `/${path}`;
      return `${httpsBase}${clean}`;
    });

    await logEvent(job.id as string, 'info', 'Selected candidate paths', { host, source: pathSource, count: urls.length });

    let bestScore = 0;
    let bestRationale = "";
    const foundCompanyLIs = new Set<string>();
    const foundPersonalLIs = new Set<string>();
    let beeCalls = 0;
    const staticOk: Array<{ url: string; status: number; html: string }> = [];
    const targetNum = (companyNumber || "").toUpperCase();
    let abortHost = false;
    let earlyAccepted = false;
    const pageEvidence: Array<{ url: string; title: string; meta: string; h1: string[]; jsonld_org_name?: string; company_numbers: string[]; postcodes: string[]; emails: string[]; linkedins: string[]; text_snippet: string }> = [];
    let lastDecisionConfidence: number | null = null;
    let tradingName: string | null = null;

    if (USE_STATIC_FETCH) {
      // First pass: cheap static fetch to avoid ScrapingBee credits on obvious 404/robots/soft-404 pages
      const useStaticSignals = !BEE_KEY || BEE_MAX_PAGES <= 0; // if Bee unavailable, fall back to static signals
      const isSoft404 = (html: string) => {
        const t = stripTags(html || '').toLowerCase();
        return /page not found|not found|404|doesn't exist|does not exist|we can't find|we cannot find|page cannot be found|oops/i.test(t);
      };
      const cachedResponses = new Map<string, { status: number; html: string; contentType: string | null }>();
      if (cachedHome) cachedResponses.set(homeUrl, cachedHome);
      for (const url of urls) {
        let r = cachedResponses.get(url);
        if (!r) {
          r = await fetchStaticHtml(url);
          cachedResponses.set(url, r);
        }
        // Log raw static fetch (status + size + short snippet) for diagnostics
        try {
          await logEvent(job.id as string, 'info', 'Static fetch', {
            url,
            status: r.status,
            bytes: (r.html || '').length,
            contentType: r.contentType || '',
            snippet: stripTags(r.html || '').slice(0, 600)
          });
        } catch {}
        if (r.status >= 200 && r.status < 400 && r.html.length >= 200 && !isSoft404(r.html)) {
          staticOk.push({ url, status: r.status, html: r.html });
          // Early decision: if page explicitly declares a different company number, abort further checks for this host (only when static content provides a labeled CRN)
          const sigEarly = parseHtmlSignals(r.html);
          const labeled = findLabeledCompanyNumber(sigEarly.textChunk);
          if (labeled) {
            if (targetNum && labeled !== targetNum) {
              abortHost = true;
              bestScore = 0;
              bestRationale = `Company number mismatch: found ${labeled}`;
              await logEvent(job.id as string, 'info', 'Early reject (mismatched company number)', { url, found: labeled, expected: targetNum });
              break;
            }
            if (targetNum && labeled === targetNum) {
              bestScore = 0.9;
              bestRationale = 'Company number match';
              earlyAccepted = true;
              await logEvent(job.id as string, 'info', 'Early accept (company number match)', { url, found: labeled });
              break;
            }
          }
          // Only if Bee is not available, use static signals for evidence + scoring
          if (useStaticSignals) {
            const sig = sigEarly;
            for (const li of sig.linkedins) {
              if (/linkedin\.com\/company\//i.test(li)) foundCompanyLIs.add(li);
              if (/linkedin\.com\/in\//i.test(li)) foundPersonalLIs.add(li);
            }
            const nums = extractCompanyNumbers(sig.textChunk);
            const pcs = Array.from(new Set((sig.textChunk.match(/[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}/gi) || []).map(s => s.toUpperCase())));
            const emails = sig.emails || [];
            pageEvidence.push({
              url,
              title: sig.title || '',
              meta: sig.metaDesc || '',
              h1: sig.h1s || [],
              jsonld_org_name: (()=>{
                for (const j of sig.jsonld) {
                  const t = (j && (j["@type"] || j["@type"])) || null;
                  const types = Array.isArray(t) ? t : (t ? [t] : []);
                  if (types.map(String).map(s=>s.toLowerCase()).includes("organization")) {
                    const nm = String((j.name || j["legalName"] || ""));
                    if (nm) return nm;
                  }
                }
                return undefined;
              })(),
              company_numbers: nums,
              postcodes: pcs,
              emails,
              linkedins: sig.linkedins || [],
              text_snippet: stripTags(r.html).slice(0, SNIPPET_CHARS)
            });
            if (!LLM_ONLY) {
              const { score, rationale } = scoreOwnership({ host, companyName, companyNumber, postcode, signals: sig });
              await logEvent(job.id as string, 'debug', 'Scored static page', { url, status: r.status, score, rationale });
              if (score > bestScore) { bestScore = score; bestRationale = rationale; }
            }
          }
        }
        await new Promise(r => setTimeout(r, 250));
        if (abortHost || earlyAccepted) break;
      }
    } else {
      await logEvent(job.id as string, 'info', 'Static fetch skipped', { host, mode: SITEFETCH_MODE });
    }

    if (USE_STATIC_FETCH && !staticOk.length) {
      await logEvent(job.id as string, 'info', 'No reachable pages from static fetch', { host });
    }

    // Optional escalation to ScrapingBee: only if we have a ScrapingBee key and candidates
    if (!abortHost && BEE_KEY && BEE_MAX_PAGES > 0 && urls.length) {
      // Prioritize legal pages (privacy, privacy-policy, impressum, terms, about, contact) for JS-rendered content
      const legalWeight = (u: string) => {
        try {
          const p = new URL(u).pathname.toLowerCase();
          if (/\/privacy\/?$/.test(p)) return 100;
          if (/\/privacy-policy\/?$/.test(p)) return 95;
          if (/\/impressum\/?$/.test(p)) return 90;
          if (/\/terms|\/terms-of-service|\/terms-and-conditions/.test(p)) return 85;
          if (/\/about|\/about-us|\/company|\/our-company/.test(p)) return 70;
          if (/\/contact\/?$/.test(p)) return 60;
          if (p === '/' || p === '') return 50;
          return 10;
        } catch { return 10; }
      };
      const isLegal = (u: string) => {
        try { const p = new URL(u).pathname.toLowerCase(); return /\/privacy(\-policy)?\/?$|\/impressum\/?$/.test(p); } catch { return false; }
      };

      const baseBeeCandidates = (staticOk.length ? staticOk : urls.map(url => ({ url, status: 0, html: '' })));
      const beeCandidates = baseBeeCandidates
        .slice(0)
        .sort((a, b) => legalWeight(b.url) - legalWeight(a.url));

      const reasonMap = new Map<string, 'crn-site' | 'candidate'>();
      const finalBeeList: string[] = [];
      const seenBee = new Set<string>();

      const tryAddUrl = (url: string, reason: 'crn-site' | 'candidate') => {
        if (!url) return;
        if (seenBee.has(url)) return;
        seenBee.add(url);
        if (finalBeeList.length < BEE_MAX_PAGES) {
          finalBeeList.push(url);
          reasonMap.set(url, reason);
        }
      };

      const crnUrls: string[] = [];
      if (targetNum && SERPER_API_KEY) {
        try {
          const q = `${targetNum} site:${host}`;
          const resp = await serperSearch(q);
          const organic = Array.isArray((resp.data as any)?.organic) ? (resp.data as any).organic : [];
          const sample = organic.slice(0, 5).map((it: any) => ({ link: it.link, title: it.title, snippet: it.snippet }));
          await logEvent(job.id as string, 'info', 'Serper fetch (crn-site)', {
            q,
            status: resp.status,
            headers: { 'x-request-id': resp.headers['x-request-id'] || resp.headers['x-requestid'] || resp.headers['request-id'] || '' },
            counts: { organic: organic.length },
            sample
          });
          for (const it of organic) {
            const link = String(it?.link || it?.url || '');
            if (!link) continue;
            try {
              const u = new URL(link);
              if (u.hostname.replace(/^www\./, '') === host.replace(/^www\./, '')) {
                crnUrls.push(u.toString());
              }
            } catch {}
          }
        } catch {}
      }

      for (const u of crnUrls) {
        if (finalBeeList.length >= BEE_MAX_PAGES) break;
        tryAddUrl(u, 'crn-site');
      }

      for (const candidate of beeCandidates) {
        if (finalBeeList.length >= BEE_MAX_PAGES) break;
        tryAddUrl(candidate.url, 'candidate');
      }

      const limit = pLimit(Math.min(10, Math.max(1, BEE_MAX_PAGES)));

      const processBeeUrl = async (url: string, reason: 'crn-site' | 'candidate') => {
        try {
          const preferFull = legalWeight(url) >= 90;
          const waitMs = isLegal(url) ? 3000 : undefined;
          const waitFor = isLegal(url) ? 'p span' : undefined;
          const res = await fetchBeeHtml(url, preferFull, waitMs, waitFor);
          const eventName = reason === 'crn-site' ? 'Bee fetch (crn-site)' : 'Bee fetch';
          try {
            await logEvent(job.id as string, 'info', eventName, {
              url,
              status: res.status,
              bytes: (res.html || '').length,
              preferFull,
              waitMs: waitMs || 0,
              waitFor: waitFor || '',
              snippet: stripTags(res.html || '').slice(0, 600)
            });
          } catch {}
          if (res.status >= 200 && res.status < 400 && (res.html || '').length >= 200) {
            beeCalls += 1;
            const sig = parseHtmlSignals(res.html);
            for (const li of sig.linkedins) {
              if (/linkedin\.com\/company\//i.test(li)) foundCompanyLIs.add(li);
              if (/linkedin\.com\/in\//i.test(li)) foundPersonalLIs.add(li);
            }
            const nums = extractCompanyNumbers(sig.textChunk);
            const pcs = Array.from(new Set((sig.textChunk.match(/[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}/gi) || []).map(s => s.toUpperCase())));
            pageEvidence.push({
              url,
              title: sig.title || '',
              meta: sig.metaDesc || '',
              h1: sig.h1s || [],
              jsonld_org_name: (()=>{
                for (const j of sig.jsonld) {
                  const t = (j && (j["@type"] || j["@type"])) || null;
                  const types = Array.isArray(t) ? t : (t ? [t] : []);
                  if (types.map(String).map(s=>s.toLowerCase()).includes("organization")) {
                    const nm = String((j.name || j["legalName"] || ""));
                    if (nm) return nm;
                  }
                }
                return undefined;
              })(),
              company_numbers: nums,
              postcodes: pcs,
              emails: sig.emails || [],
              linkedins: sig.linkedins || [],
              text_snippet: stripTags(res.html).slice(0, SNIPPET_CHARS)
            });
            if (!LLM_ONLY) {
              const { score, rationale } = scoreOwnership({ host, companyName, companyNumber, postcode, signals: sig });
              await logEvent(job.id as string, 'debug', 'Scored bee page', { url, score, rationale, numbers_found: nums.length });
              if (score > bestScore) { bestScore = score; bestRationale = `Bee: ${rationale}`; }
            }
          }
        } catch (err) {
          await logEvent(job.id as string, 'warn', 'Bee fetch failed', { url, error: String(err), reason });
        }
      };

      await Promise.all(
        finalBeeList.map(url => limit(() => processBeeUrl(url, reasonMap.get(url) || 'candidate')))
      );
    }

    // Deterministic acceptance: exact CH number, exact company name, or exact postcode
    if (!abortHost && !earlyAccepted) {
      const normCoName = normalizeStrict(companyName || '');
      const targetPc = (postcode || '').toUpperCase().replace(/\s+/g, '');
      let pcExact = false, nameMatch = false, numMatch = false;
      for (const p of pageEvidence) {
        if (targetNum && p.company_numbers.includes(targetNum)) numMatch = true;
        const jsn = p.jsonld_org_name ? normalizeStrict(p.jsonld_org_name) : '';
        if (normCoName && jsn && jsn === normCoName) nameMatch = true;
        if (targetPc && Array.isArray(p.postcodes) && p.postcodes.some(pc => (pc || '').toString().toUpperCase().replace(/\s+/g, '') === targetPc)) pcExact = true;
        if (numMatch || nameMatch || pcExact) break;
      }
      if (numMatch) {
        bestScore = Math.max(bestScore, 0.95);
        bestRationale = 'Deterministic: company number match across pages';
      } else if (nameMatch) {
        bestScore = Math.max(bestScore, 0.9);
        bestRationale = 'Deterministic: JSON-LD organization name matches company name exactly';
      } else if (pcExact) {
        bestScore = Math.max(bestScore, 0.9);
        bestRationale = 'Deterministic: exact postcode found on pages';
      }
    }

    // LLM-only or as final arbiter
    if (!abortHost && (LLM_ONLY || bestScore < ACCEPT_THRESHOLD)) {
      // Trim evidence to a reasonable size
      const usedPages = pageEvidence.slice(0, MAX_PAGES_FOR_LLM);
      const decision = await llmEvaluateHost({ host, companyName, companyNumber, postcode, pages: usedPages, jobId: job.id as string });
      if (decision) {
        // token usage logged inside llmEvaluateHost (debug + info)
        // Use company_relevance to score ownership likelihood. Keep rationale prefixed.
        bestScore = decision.company_relevance;
        bestRationale = `LLM: ${decision.rationale}`;
        lastDecisionConfidence = typeof decision.decision_confidence === 'number' ? decision.decision_confidence : null;
        if (typeof decision.trading_name === 'string' && decision.trading_name.trim()) {
          tradingName = decision.trading_name.trim();
        }
        for (const li of decision.linkedins || []) {
          if (/linkedin\.com\/company\//i.test(li)) foundCompanyLIs.add(li);
          if (/linkedin\.com\/in\//i.test(li)) foundPersonalLIs.add(li);
        }
      }
    }

    const validated_websites = bestScore >= ACCEPT_THRESHOLD ? [host] : [];
    const validationEntry: any = {
      url: host,
      company_relevance: Number(bestScore.toFixed(3)),
      decision_confidence: (lastDecisionConfidence != null ? Number(lastDecisionConfidence.toFixed(3)) : undefined),
      decision_rationale: bestRationale
    };
    if (tradingName) {
      validationEntry.trading_name = tradingName;
    }
    const website_validations = [validationEntry];

    // If we used LLM decision last, try to enrich the validation record with its decision_confidence (best effort)
    // Note: This keeps existing structure minimal while capturing both signals when available.
    // We set the field on the first (and only) validation entry.
    // (Non-LLM paths will simply leave decision_confidence undefined.)

    const siteFetchLog: any = {
      scope: 'summary',
      host,
      score: bestScore,
      validated_websites,
      website_validations,
      linkedins: Array.from(new Set([...foundCompanyLIs, ...foundPersonalLIs])).slice(0, 50)
    };
    if (tradingName) siteFetchLog.trading_name = tradingName;
    await logEvent(job.id as string, 'info', 'SiteFetch results', siteFetchLog);

    // If we have an absolute match (LLM confidence == 1 or deterministic CRN match), cancel other pending site-fetch jobs under this workflow to save credits
    try {
      const root = (job.data as any)?.rootJobId || null;
      const absolute = (validated_websites.length > 0) && ((lastDecisionConfidence != null && lastDecisionConfidence >= 1) || /Deterministic: company number match/i.test(bestRationale));
      if (root && absolute) {
        const { rows: pend } = await query<{ job_id: string }>(
          `SELECT job_id FROM job_progress
             WHERE queue = 'site-fetch' AND status = 'pending' AND data->>'rootJobId' = $1 AND job_id <> $2`,
          [root, job.id as string]
        );
        let cancelled = 0;
        for (const row of pend) {
          try { await siteFetchQ.remove(row.job_id); } catch {}
          try {
            await query(`UPDATE job_progress SET status='cancelled', data = COALESCE(data,'{}'::jsonb) || $2::jsonb, updated_at=now() WHERE job_id=$1`,
              [row.job_id, JSON.stringify({ cancelled: true, reason: 'absolute_match', winner: host })]);
          } catch {}
          cancelled++;
        }
        if (cancelled) {
          await logEvent(job.id as string, 'info', 'Cancelled pending site-fetch jobs due to confident match', { rootJobId: root, cancelled, winner: host });
        }
      }
    } catch {}

    await completeJob(job.id as string, {
      companyNumber,
      companyName,
      host,
      validated_websites,
      website_validations,
      linkedins: Array.from(new Set([...foundCompanyLIs, ...foundPersonalLIs])),
      trading_name: tradingName || null,
      rootJobId: (job.data as any)?.rootJobId || null
    });
    try { await logEvent(job.id as string, 'info', 'Usage summary', { scrapingbee_calls: beeCalls }); } catch {}

    // Persist results into ch_appointments for this company
    try {
      const addWebsites = JSON.stringify(validated_websites);
      const addVerifications = JSON.stringify(website_validations);
      // Only persist LinkedIn URLs when the host has been validated as owned by the company
      const persistLIs = validated_websites.length > 0;
      const addCompanyLIs = JSON.stringify(persistLIs ? Array.from(foundCompanyLIs) : []);
      const addPersonalLIs = JSON.stringify(persistLIs ? Array.from(foundPersonalLIs) : []);
      await query(
        `UPDATE ch_appointments
           SET
             verified_company_website = (
              SELECT CASE WHEN jsonb_typeof(COALESCE(verified_company_website, '[]'::jsonb)) = 'array'
                          THEN (
                            SELECT jsonb_agg(DISTINCT j.value)
                              FROM jsonb_array_elements(COALESCE(verified_company_website, '[]'::jsonb) || $1::jsonb) AS j(value)
                          )
                          ELSE $1::jsonb
                     END
             ),
             company_website_verification = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(company_website_verification, '[]'::jsonb)) = 'array'
                           THEN (
                             SELECT jsonb_agg(DISTINCT j.value)
                               FROM jsonb_array_elements(COALESCE(company_website_verification, '[]'::jsonb) || $2::jsonb) AS j(value)
                           )
                           ELSE $2::jsonb
                      END
             ),
             verified_company_linkedIns = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(verified_company_linkedIns, '[]'::jsonb)) = 'array'
                           THEN (
                             SELECT jsonb_agg(DISTINCT j.value)
                               FROM jsonb_array_elements(COALESCE(verified_company_linkedIns, '[]'::jsonb) || $3::jsonb) AS j(value)
                           )
                           ELSE $3::jsonb
                      END
            ),
            verified_director_linkedIns = (
              SELECT CASE WHEN jsonb_typeof(COALESCE(verified_director_linkedIns, '[]'::jsonb)) = 'array'
                          THEN (
                            SELECT jsonb_agg(DISTINCT j.value)
                              FROM jsonb_array_elements(COALESCE(verified_director_linkedIns, '[]'::jsonb) || $4::jsonb) AS j(value)
                          )
                          ELSE $4::jsonb
                     END
            ),
            trading_name = CASE
              WHEN COALESCE($6::text, '') <> '' THEN $6::text
              ELSE trading_name
            END,
            updated_at = now()
        WHERE company_number = $5`,
        [addWebsites, addVerifications, addCompanyLIs, addPersonalLIs, companyNumber, tradingName]
      );
      await logEvent(job.id as string, 'info', 'Updated ch_appointments with siteFetch results', {
        scope: 'summary',
        companyNumber,
        websites_added: validated_websites.length,
        company_linkedins_added: persistLIs ? Array.from(foundCompanyLIs).length : 0,
        personal_linkedins_added: persistLIs ? Array.from(foundPersonalLIs).length : 0,
        linkedins_persisted: persistLIs
      });
      if (tradingName) {
        const root = (job.data as any)?.rootJobId || null;
        if (root) {
          try {
            const { rows: rootRows } = await query<{ data: any }>(
              `SELECT data FROM job_progress WHERE job_id = $1 LIMIT 1`,
              [root]
            );
            const rootData = rootRows?.[0]?.data;
            if (rootData && typeof rootData === 'object') {
              let changed = false;
              const enriched = Array.isArray((rootData as any).enriched) ? (rootData as any).enriched : [];
              for (const person of enriched) {
                if (!person || typeof person !== 'object') continue;
                const appointments = Array.isArray((person as any).appointments) ? (person as any).appointments : [];
                for (const appt of appointments) {
                  if (!appt || typeof appt !== 'object') continue;
                  const num = (appt.company_number || appt.companyNumber || '').toString();
                  if (num === companyNumber && (appt.trading_name || '') !== tradingName) {
                    appt.trading_name = tradingName;
                    changed = true;
                  }
                }
              }
              if (changed) {
                await query(
                  `UPDATE job_progress SET data = $2, updated_at = now() WHERE job_id = $1`,
                  [root, JSON.stringify(rootData)]
                );
                await logEvent(root, 'info', 'Updated trading name from siteFetch', { companyNumber, trading_name: tradingName });
              }
            }
          } catch (e) {
            await logEvent(job.id as string, 'warn', 'Failed to merge trading name into root job progress', {
              error: String(e),
              rootJobId: root,
              companyNumber,
              trading_name: tradingName
            });
          }
        }
      }
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Failed to update ch_appointments with siteFetch results', { error: String(e), companyNumber });
    }

    // After this job is marked completed, decide whether to kick off person-linkedin
    try {
      const root = (job.data as any)?.rootJobId || null;
      if (root) {
        // If we have a workflow root, only continue once ALL company-discovery and site-fetch jobs for the root are finished
        const { rows: progRows } = await query<{ data: any }>(
          `SELECT data FROM job_progress WHERE job_id = $1`,
          [root]
        );
        const rootData = progRows?.[0]?.data || null;
        const expectedCompanies: string[] = Array.isArray(rootData?.enqueued?.companies) ? rootData.enqueued.companies : [];

        // Count running jobs under the root across both queues
        const { rows: runningAll } = await query<{ c: string }>(
          `SELECT COUNT(*)::int AS c
             FROM job_progress
            WHERE status IN ('pending','running')
              AND (queue = 'company-discovery' OR queue = 'site-fetch')
              AND data->>'rootJobId' = $1`,
          [root]
        );
        const runningCount = Number(runningAll?.[0]?.c || 0);

        // How many company-discovery jobs completed for the expected companies
        let completedCompanies = 0;
        if (expectedCompanies.length) {
          const { rows: compRows } = await query<{ c: string }>(
            `SELECT COUNT(DISTINCT data->>'companyNumber')::int AS c
               FROM job_progress
              WHERE queue = 'company-discovery'
                AND status = 'completed'
                AND data->>'rootJobId' = $1
                AND (data->>'companyNumber') = ANY($2::text[])`,
            [root, expectedCompanies]
          );
          completedCompanies = Number(compRows?.[0]?.c || 0);
        }

        if (runningCount === 0) {
          // Finalize: enqueue person-linkedin for each person in this workflow using aggregated context across ALL their appointments
          const { rows: people } = await query<{ id: number; first_name: string | null; middle_names: string | null; last_name: string | null; dob_string: string | null }>(
            `SELECT DISTINCT p.id, p.first_name, p.middle_names, p.last_name, p.dob_string
               FROM ch_people p
              WHERE p.job_id = $1`,
            [root]
          );

          let enqueued = 0;
          for (const p of people) {
            const fullName = [p.first_name, p.middle_names, p.last_name].filter(Boolean).join(' ');
            if (!fullName.trim()) continue;

            // Aggregate context for this person across all their appointments
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
              lp_appt AS (
                SELECT j.value AS v
                  FROM ch_appointments a,
                       LATERAL jsonb_array_elements(COALESCE(a.verified_director_linkedIns, '[]'::jsonb)) AS j(value)
                 WHERE a.person_id = $1
              ),
              lp_person AS (
                SELECT jsonb_array_elements(COALESCE(p.verified_director_linkedIns, '[]'::jsonb)) AS v
                  FROM ch_people p
                 WHERE p.id = $1
              ),
              tn AS (
                SELECT to_jsonb(TRIM(a.trading_name)) AS v
                  FROM ch_appointments a
                 WHERE a.person_id = $1 AND COALESCE(a.trading_name, '') <> ''
              )
              SELECT
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM w), '[]'::jsonb) AS websites,
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM lc), '[]'::jsonb) AS company_linkedins,
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM (
                  SELECT v FROM lp_appt
                  UNION ALL
                  SELECT v FROM lp_person
                ) all_lp), '[]'::jsonb) AS personal_linkedins,
                COALESCE((SELECT jsonb_agg(DISTINCT v) FROM tn), '[]'::jsonb) AS trading_names`;
            const { rows: aggRows } = await query<any>(aggSql, [Number((p as any).id)]);
            const aggWebsites: string[] = Array.isArray(aggRows?.[0]?.websites) ? aggRows[0].websites : [];
            const aggCompanyLIs: string[] = Array.isArray(aggRows?.[0]?.company_linkedins) ? aggRows[0].company_linkedins : [];
            const aggPersonalLIs: string[] = Array.isArray(aggRows?.[0]?.personal_linkedins) ? aggRows[0].personal_linkedins : [];
            const aggTradingNames: string[] = Array.isArray(aggRows?.[0]?.trading_names)
              ? Array.from(new Set((aggRows[0].trading_names as any[]).map(v => (v ?? '').toString().trim()).filter(Boolean)))
              : [];

            const { rows: companyRows } = await query<{ company_name: string | null; company_status: string | null; trading_name: string | null }>(
              `SELECT company_name, company_status, trading_name
                 FROM ch_appointments
                WHERE person_id = $1`,
              [Number((p as any).id)]
            );
            const allTradingSet = new Set<string>(aggTradingNames);
            const activeTradingSet = new Set<string>();
            const allRegisteredSet = new Set<string>();
            const activeRegisteredSet = new Set<string>();
            for (const row of companyRows) {
              const status = (row.company_status || '').toString().trim().toLowerCase();
              const isActive = status === 'active';
              const registered = (row.company_name || '').toString().trim();
              if (registered) {
                allRegisteredSet.add(registered);
                if (isActive) activeRegisteredSet.add(registered);
              }
              const tname = (row.trading_name || '').toString().trim();
              if (tname) {
                allTradingSet.add(tname);
                if (isActive) activeTradingSet.add(tname);
              }
            }
            const allTradingNames = Array.from(allTradingSet);
            const activeTradingNames = Array.from(activeTradingSet);
            const allRegisteredNames = Array.from(allRegisteredSet);
            const activeRegisteredNames = Array.from(activeRegisteredSet);

            const companyNameForContext = (() => {
              const activeTrading = companyRows.find(r => (r.company_status || '').toString().trim().toLowerCase() === 'active' && (r.trading_name || '').toString().trim());
              if (activeTrading) return activeTrading.trading_name!.toString().trim();
              const activeRegistered = companyRows.find(r => (r.company_status || '').toString().trim().toLowerCase() === 'active' && (r.company_name || '').toString().trim());
              if (activeRegistered) return activeRegistered.company_name!.toString().trim();
              const anyTrading = companyRows.find(r => (r.trading_name || '').toString().trim());
              if (anyTrading) return anyTrading.trading_name!.toString().trim();
              const anyRegistered = companyRows.find(r => (r.company_name || '').toString().trim());
              if (anyRegistered) return anyRegistered.company_name!.toString().trim();
              return '';
            })();

            const pjId = `person:${(p as any).id}:${root}`;
            const payload = {
              person: {
                firstName: p.first_name || '',
                middleNames: p.middle_names || '',
                lastName: p.last_name || '',
                dob: p.dob_string || ''
              },
              context: {
                companyName: companyNameForContext,
                tradingNames: allTradingNames,
                activeTradingNames,
                registeredNames: allRegisteredNames,
                activeRegisteredNames,
                websites: aggWebsites,
                companyLinkedIns: aggCompanyLIs,
                personalLinkedIns: aggPersonalLIs
              },
              rootJobId: root
            };
            try {
              await query(
                `INSERT INTO job_progress(job_id, queue, name, status, data)
                 VALUES ($1,'person-linkedin','discover','pending',$2)
                 ON CONFLICT (job_id)
                 DO UPDATE SET status='pending', data=$2, updated_at=now()`,
                [pjId, JSON.stringify(payload)]
              );
            } catch {}
            await personQ.add('discover', payload, { jobId: pjId, attempts: 5, backoff: { type: 'exponential', delay: 2000 } });
            enqueued++;
          }
          await logEvent(job.id as string, 'info', 'Enqueued person-linkedin searches after ALL discovery complete', { scope: 'summary', rootJobId: root, people: people.length, enqueued });
        }
      } else {
        // Backward-compatible behavior: if no workflow root, fallback to per-company aggregation
        const { rows: runningRows } = await query<{ c: string }>(
          `SELECT COUNT(*)::int AS c
             FROM job_progress
            WHERE queue = 'site-fetch'
              AND data->>'companyNumber' = $1
              AND status = 'running'`,
          [companyNumber]
        );
        const running = Number(runningRows?.[0]?.c || 0);
        if (running === 0) {
          const aggSql = `
            WITH w AS (
              SELECT j.value AS v
                FROM ch_appointments,
                     LATERAL jsonb_array_elements(COALESCE(verified_company_website, '[]'::jsonb)) AS j(value)
               WHERE company_number = $1
            ),
            lc AS (
              SELECT j.value AS v
                FROM ch_appointments,
                     LATERAL jsonb_array_elements(COALESCE(verified_company_linkedIns, '[]'::jsonb)) AS j(value)
               WHERE company_number = $1
            ),
            lp_appt AS (
              SELECT j.value AS v
                FROM ch_appointments,
                     LATERAL jsonb_array_elements(COALESCE(verified_director_linkedIns, '[]'::jsonb)) AS j(value)
               WHERE company_number = $1
            ),
            lp_person AS (
              SELECT jsonb_array_elements(COALESCE(p.verified_director_linkedIns, '[]'::jsonb)) AS v
                FROM ch_people p
                JOIN ch_appointments a ON a.person_id = p.id
               WHERE a.company_number = $1
            ),
            tn AS (
              SELECT to_jsonb(TRIM(trading_name)) AS v
                FROM ch_appointments
               WHERE company_number = $1 AND COALESCE(trading_name, '') <> ''
            )
            SELECT
              COALESCE((SELECT jsonb_agg(DISTINCT v) FROM w), '[]'::jsonb) AS websites,
              COALESCE((SELECT jsonb_agg(DISTINCT v) FROM lc), '[]'::jsonb) AS company_linkedins,
              COALESCE((SELECT jsonb_agg(DISTINCT v) FROM (
                SELECT v FROM lp_appt
                UNION ALL
                SELECT v FROM lp_person
              ) all_lp), '[]'::jsonb) AS personal_linkedins,
              COALESCE((SELECT jsonb_agg(DISTINCT v) FROM tn), '[]'::jsonb) AS trading_names`;
          const { rows: aggRows } = await query<any>(aggSql, [companyNumber]);
          const aggWebsites: string[] = Array.isArray(aggRows?.[0]?.websites) ? aggRows[0].websites : [];
          const aggCompanyLIs: string[] = Array.isArray(aggRows?.[0]?.company_linkedins) ? aggRows[0].company_linkedins : [];
          const aggPersonalLIs: string[] = Array.isArray(aggRows?.[0]?.personal_linkedins) ? aggRows[0].personal_linkedins : [];
          const aggTradingNames: string[] = Array.isArray(aggRows?.[0]?.trading_names)
            ? Array.from(new Set((aggRows[0].trading_names as any[]).map(v => (v ?? '').toString().trim()).filter(Boolean)))
            : [];

          const { rows: people } = await query<{ id: number; first_name: string | null; middle_names: string | null; last_name: string | null; dob_string: string | null }>(
            `SELECT DISTINCT p.id, p.first_name, p.middle_names, p.last_name, p.dob_string
               FROM ch_people p
               JOIN ch_appointments a ON a.person_id = p.id
              WHERE a.company_number = $1`,
            [companyNumber]
          );
          const { rows: nameRows } = await query<{ company_name: string | null; trading_name: string | null; company_status: string | null }>(
            `SELECT company_name, trading_name, company_status
               FROM ch_appointments
              WHERE company_number = $1`,
            [companyNumber]
          );
          const allRegisteredSet = new Set<string>();
          const activeRegisteredSet = new Set<string>();
          const allTradingSet = new Set<string>(aggTradingNames);
          const activeTradingSet = new Set<string>();
          for (const row of nameRows) {
            const status = (row.company_status || '').toString().trim().toLowerCase();
            const isActive = status === 'active';
            const reg = (row.company_name || '').toString().trim();
            if (reg) {
              allRegisteredSet.add(reg);
              if (isActive) activeRegisteredSet.add(reg);
            }
            const tname = (row.trading_name || '').toString().trim();
            if (tname) {
              allTradingSet.add(tname);
              if (isActive) activeTradingSet.add(tname);
            }
          }
          const allRegisteredNames = Array.from(allRegisteredSet);
          const activeRegisteredNames = Array.from(activeRegisteredSet);
          const allTradingNames = Array.from(allTradingSet);
          const activeTradingNames = Array.from(activeTradingSet);
          const companyNameForContext = (() => {
            const activeTrading = nameRows.find(r => (r.company_status || '').toString().trim().toLowerCase() === 'active' && (r.trading_name || '').toString().trim());
            if (activeTrading) return activeTrading.trading_name!.toString().trim();
            const activeRegistered = nameRows.find(r => (r.company_status || '').toString().trim().toLowerCase() === 'active' && (r.company_name || '').toString().trim());
            if (activeRegistered) return activeRegistered.company_name!.toString().trim();
            const anyTrading = nameRows.find(r => (r.trading_name || '').toString().trim());
            if (anyTrading) return anyTrading.trading_name!.toString().trim();
            const anyRegistered = nameRows.find(r => (r.company_name || '').toString().trim());
            if (anyRegistered) return anyRegistered.company_name!.toString().trim();
            return '';
          })();

          let enqueued = 0;
          for (const p of people) {
            const fullName = [p.first_name, p.middle_names, p.last_name].filter(Boolean).join(' ');
            if (!fullName.trim()) continue;
            const jobId = `person:${p.id}:${companyNumber}`;
            await personQ.add(
              'discover',
              {
                person: {
                  firstName: p.first_name || '',
                  middleNames: p.middle_names || '',
                  lastName: p.last_name || '',
                  dob: p.dob_string || ''
                },
                context: {
                  companyNumber,
                  companyName: companyNameForContext,
                  tradingNames: allTradingNames,
                  activeTradingNames,
                  registeredNames: allRegisteredNames,
                  activeRegisteredNames,
                  websites: aggWebsites,
                  companyLinkedIns: aggCompanyLIs,
                  personalLinkedIns: aggPersonalLIs
                }
              },
              { jobId, attempts: 5, backoff: { type: 'exponential', delay: 2000 } }
            );
            enqueued++;
          }
          await logEvent(job.id as string, 'info', 'Enqueued person-linkedin searches after siteFetch aggregation', { scope: 'summary', companyNumber, people: people.length, enqueued });
        }
      }
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Aggregation/queue after siteFetch failed', { error: String(e), companyNumber });
    }

    logger.info({ host, score: bestScore, validates: validated_websites.length }, 'Site fetch done');
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ host, err: String(err) }, 'Site fetch failed');
  }
}, { connection, concurrency: 1 });
