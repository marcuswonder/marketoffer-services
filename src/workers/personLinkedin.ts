import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { fetch } from "undici";
import { logger } from "../lib/logger.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { base } from "../lib/airtable.js";
const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";
const LLM_DEBUG_LOGS = (process.env.LLM_DEBUG_LOGS || "").toLowerCase() === 'true';
const PERSON_SERPER_JITTER_MS = 900;
const PERSON_MAX_TRADING = Number(process.env.PERSON_MAX_TRADING || 5);
const PERSON_MAX_COMPANY = Number(process.env.PERSON_MAX_COMPANY || 5);
const PERSON_MAX_PERSONAL_CANDIDATES = Number(process.env.PERSON_MAX_PERSONAL_CANDIDATES || 10);
const PERSON_MAX_COMPANY_CANDIDATES = Number(process.env.PERSON_MAX_COMPANY_CANDIDATES || 6);

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

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

function canonLinkedIn(u: string): string {
  try {
    const url = new URL(u);
    if (!/linkedin\.com$/i.test(url.hostname) && !/\.linkedin\.com$/i.test(url.hostname)) return '';
    url.search = '';
    url.hash = '';
    let s = url.toString();
    if (s.endsWith('/')) s = s.slice(0, -1);
    return s;
  } catch { return ''; }
}

function isPersonalLI(u: string) { return /linkedin\.com\/in\//i.test(u); }
function isCompanyLI(u: string) { return /linkedin\.com\/company\//i.test(u) && !/\/admin(\/|$)/i.test(u); }

function hostLabel(h: string): string {
  const host = h.replace(/^https?:\/\//i, '').replace(/\/$/, '').toLowerCase();
  const apex = host.split('/')[0].split('?')[0];
  const parts = apex.split('.');
  const core = parts.length > 2 ? parts.slice(-2, -1)[0] : parts[0];
  return core.replace(/[-_]+/g, ' ').trim();
}

async function scorePersonalCandidate(opts: {
  fullName: string;
  dob?: string;
  companies: string[];
  trading: string[];
  websites: string[];
  candidate: { url: string; title?: string; snippet?: string };
  jobId: string;
}) {
  const payload = {
    task: 'score_personal_linkedin',
    person: { name: opts.fullName, dob: opts.dob || '' },
    companies: opts.companies,
    trading_names: opts.trading,
    websites: opts.websites,
    candidate: opts.candidate
  };
  const prompt = `Goal: Assess whether the LinkedIn URL belongs to the target person. Return strict JSON { relevance: 0..1, confidence: 0..1, rationale: string }.
Rules: Do not rely on name similarity alone. Prefer multiple supporting cues such as company/trading-name match, role/title alignment, location, tenure/dates. If evidence is weak/ambiguous, lower relevance and/or confidence.`;
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${process.env.OPENAI_API_KEY || ''}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      temperature: 0.2,
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
  try {
    const obj = JSON.parse(txt);
    const relevance = typeof obj.relevance === 'number' ? Math.max(0, Math.min(1, obj.relevance)) : 0;
    const confidence = typeof obj.confidence === 'number' ? Math.max(0, Math.min(1, obj.confidence)) : 0;
    const rationale = typeof obj.rationale === 'string' ? obj.rationale : '';
    if (LLM_DEBUG_LOGS) {
      await logEvent(opts.jobId, 'debug', 'LLM person score', { url: opts.candidate.url, title: opts.candidate.title || '', relevance, confidence, rationale: rationale.slice(0, 400) });
    }
    return { relevance, confidence, rationale };
  } catch {
    return null;
  }
}

async function scoreCompanyCandidate(opts: {
  companyNames: string[];
  trading: string[];
  candidate: { url: string; title?: string; snippet?: string };
  jobId: string;
}) {
  const payload = {
    task: 'score_company_linkedin',
    company_names: opts.companyNames,
    trading_names: opts.trading,
    candidate: opts.candidate
  };
  const prompt = `Goal: Assess whether the LinkedIn URL is the company page for the target business (legal or trading name). Return JSON { relevance: 0..1, confidence: 0..1, rationale: string }. Do not accept based on name similarity alone; prefer supporting cues (about text, industry fit, geography).`;
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${process.env.OPENAI_API_KEY || ''}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      temperature: 0.2,
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
  try {
    const obj = JSON.parse(txt);
    const relevance = typeof obj.relevance === 'number' ? Math.max(0, Math.min(1, obj.relevance)) : 0;
    const confidence = typeof obj.confidence === 'number' ? Math.max(0, Math.min(1, obj.confidence)) : 0;
    const rationale = typeof obj.rationale === 'string' ? obj.rationale : '';
    if (LLM_DEBUG_LOGS) {
      await logEvent(opts.jobId, 'debug', 'LLM company score', { url: opts.candidate.url, title: opts.candidate.title || '', relevance, confidence, rationale: rationale.slice(0, 400) });
    }
    return { relevance, confidence, rationale };
  } catch {
    return null;
  }
}

await initDb();
type PersonSearchPayload = {
  personId?: string;
  person?: { firstName: string; middleNames?: string; lastName: string; dob?: string };
  context?: { companyNumber?: string; companyName?: string; websites?: string[]; companyLinkedIns?: string[]; personalLinkedIns?: string[] };
  rootJobId?: string;
};

export default new Worker("person-linkedin", async job => {
  const { personId, person, context, rootJobId } = job.data as PersonSearchPayload;
  await startJob({ jobId: job.id as string, queue: 'person-linkedin', name: job.name, payload: job.data });
  try {
    let fullName = "";
    let dob: string | undefined;
    let first = "";
    let middle = "";
    let last = "";

    if (personId) {
      const rec = await base("People").find(personId);
      first = String(rec.get("first_name") || "");
      middle = String(rec.get("middle_names") || "");
      last = String(rec.get("last_name") || "");
      fullName = [first, middle, last].filter(Boolean).join(" ");
      dob = rec.get("dob_string") as string | undefined;
      await logEvent(job.id as string, 'info', 'Loaded person (Airtable)', { personId, fullName, first, middle, last, hasDob: Boolean(dob) });
    } else if (person) {
      first = person.firstName || "";
      middle = person.middleNames || "";
      last = person.lastName || "";
      fullName = [first, middle, last].filter(Boolean).join(" ");
      dob = person.dob || undefined;
      await logEvent(job.id as string, 'info', 'Loaded person (payload)', { fullName, first, middle, last, hasDob: Boolean(dob), context });
    } else {
      throw new Error('Missing personId or person payload');
    }
    const shortName = [first, last].filter(Boolean).join(' ').trim();

    // Build query seeds
    const websites: string[] = Array.isArray(context?.websites) ? (context!.websites as string[]) : [];
    const siteHosts = Array.from(new Set(websites.map(w => (w || '').toString().replace(/^https?:\/\//i, '').replace(/\/$/, '')))).filter(Boolean);
    const tradingNames = Array.from(new Set(siteHosts.map(h => hostLabel(h)))).filter(Boolean).slice(0, PERSON_MAX_TRADING);
    const companyNameCtx: string = (context?.companyName || '').toString();
    const companyNames = Array.from(new Set([companyNameCtx, ...tradingNames])).filter(Boolean).slice(0, PERSON_MAX_COMPANY);
    const personalSeeds: string[] = Array.isArray(context?.personalLinkedIns) ? (context!.personalLinkedIns as string[]) : [];
    const companySeeds: string[] = Array.isArray(context?.companyLinkedIns) ? (context!.companyLinkedIns as string[]) : [];

    const personalQueries: string[] = [];
    const companyQueries: string[] = [];
    if (fullName.trim()) {
      personalQueries.push(`${fullName} UK site:linkedin.com/in`);
      if (dob) personalQueries.push(`${fullName} ${dob} site:linkedin.com/in`);
      if (tradingNames.length) personalQueries.push(`${fullName} ${tradingNames.join(' ')} UK site:linkedin.com/in`);
      if (companyNames.length) personalQueries.push(`${fullName} ${companyNames.join(' ')} UK site:linkedin.com/in`);
      for (const t of tradingNames) personalQueries.push(`${fullName} ${t} UK site:linkedin.com/in`);
      for (const c of companyNames) personalQueries.push(`${fullName} ${c} UK site:linkedin.com/in`);
    }
    // Also run searches excluding middle names (first + last only)
    if (shortName && shortName !== fullName) {
      personalQueries.push(`${shortName} UK site:linkedin.com/in`);
      if (dob) personalQueries.push(`${shortName} ${dob} site:linkedin.com/in`);
      if (tradingNames.length) personalQueries.push(`${shortName} ${tradingNames.join(' ')} UK site:linkedin.com/in`);
      if (companyNames.length) personalQueries.push(`${shortName} ${companyNames.join(' ')} UK site:linkedin.com/in`);
      for (const t of tradingNames) personalQueries.push(`${shortName} ${t} UK site:linkedin.com/in`);
      for (const c of companyNames) personalQueries.push(`${shortName} ${c} UK site:linkedin.com/in`);
    }
    // De-duplicate queries to keep Serper usage efficient
    const dedupPersonal = Array.from(new Set(personalQueries));
    personalQueries.length = 0;
    personalQueries.push(...dedupPersonal);
    if (companyNames.length) {
      for (const c of companyNames) companyQueries.push(`${c} UK site:linkedin.com/company`);
    }
    await logEvent(job.id as string, 'info', 'Built queries', {
      personalQueries,
      companyQueries,
      seeds: { personal: personalSeeds.length, company: companySeeds.length },
      tradingNames,
      companyNames
    });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI built queries', { fullName, personalQueries: personalQueries.slice(0, 3), companyQueries: companyQueries.slice(0, 3) }); } catch {}
    }

    type Hit = { url: string; title?: string; snippet?: string };
    const personalHits: Hit[] = [];
    const companyHits: Hit[] = [];

    for (const q of [...personalQueries, ...companyQueries]) {
      const data = await serperSearch(q);
      const organic = Array.isArray((data as any).organic) ? (data as any).organic : [];
      for (const it of organic) {
        const link: string = it.link || '';
        const title: string = it.title || '';
        const snippet: string = it.snippet || '';
        if (!link) continue;
        if (isPersonalLI(link)) personalHits.push({ url: link, title, snippet });
        else if (isCompanyLI(link)) companyHits.push({ url: link, title, snippet });
      }
      await logEvent(job.id as string, 'debug', 'Query processed', {
        q,
        found: { personal: personalHits.length, company: companyHits.length }
      });
      await sleep(PERSON_SERPER_JITTER_MS);
    }

    // Normalize, dedupe, and seed with siteFetch links
    const personalSet = new Map<string, Hit>();
    const companySet = new Map<string, Hit>();
    for (const seed of personalSeeds) {
      const c = canonLinkedIn(seed);
      if (c && isPersonalLI(c)) personalSet.set(c, { url: c });
    }
    for (const seed of companySeeds) {
      const c = canonLinkedIn(seed);
      if (c && isCompanyLI(c)) companySet.set(c, { url: c });
    }
    for (const h of personalHits) {
      const c = canonLinkedIn(h.url);
      if (c && isPersonalLI(c) && !personalSet.has(c)) personalSet.set(c, { url: c, title: h.title, snippet: h.snippet });
    }
    for (const h of companyHits) {
      const c = canonLinkedIn(h.url);
      if (c && isCompanyLI(c) && !companySet.has(c)) companySet.set(c, { url: c, title: h.title, snippet: h.snippet });
    }

    await logEvent(job.id as string, 'info', 'Aggregated LinkedIn candidates', {
      counts: { personal: personalSet.size, company: companySet.size },
      seeds: { personal: personalSeeds.length, company: companySeeds.length }
    });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI aggregated candidates', { counts: { personal: personalSet.size, company: companySet.size } }); } catch {}
    }

    // Score candidates with LLM
    const personalCandidates = Array.from(personalSet.values()).slice(0, PERSON_MAX_PERSONAL_CANDIDATES);
    const companyCandidates = Array.from(companySet.values()).slice(0, PERSON_MAX_COMPANY_CANDIDATES);

    const personalScored: Array<{ url: string; relevance: number; confidence: number; rationale: string }> = [];
    for (const c of personalCandidates) {
      const s = await scorePersonalCandidate({
        fullName,
        dob,
        companies: companyNames,
        trading: tradingNames,
        websites: siteHosts,
        candidate: c,
        jobId: job.id as string,
      });
      if (s) personalScored.push({ url: c.url, ...s });
      await sleep(500);
    }

    const companyScored: Array<{ url: string; relevance: number; confidence: number; rationale: string }> = [];
    for (const c of companyCandidates) {
      const s = await scoreCompanyCandidate({ companyNames, trading: tradingNames, candidate: c, jobId: job.id as string });
      if (s) companyScored.push({ url: c.url, ...s });
      await sleep(400);
    }

    personalScored.sort((a,b)=> (b.relevance - a.relevance) || (b.confidence - a.confidence));
    companyScored.sort((a,b)=> (b.relevance - a.relevance) || (b.confidence - a.confidence));

    await logEvent(job.id as string, 'info', 'Scored LinkedIn candidates', {
      personal_top: personalScored.slice(0,3),
      company_top: companyScored.slice(0,3)
    });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI scored candidates', { personal_top: personalScored.slice(0,3), company_top: companyScored.slice(0,3) }); } catch {}
    }

    // Airtable write (optional): store top personal candidates
    if (personId && WRITE_TO_AIRTABLE) {
      try {
        await base("People").update(personId, {
          potential_linkedins: personalScored.map(x => x.url) as any
        });
        await logEvent(job.id as string, 'info', 'Airtable write (top personal candidates)', { count: personalScored.length });
      } catch (e) {
        await logEvent(job.id as string, 'error', 'Airtable write failed', { error: String(e) });
      }
    }

    await completeJob(job.id as string, {
      personId: personId || null,
      fullName,
      context,
      results: {
        personal: personalScored,
        company: companyScored
      }
    });
    logger.info({ personId: personId || null, personal: personalScored.length, company: companyScored.length }, 'Person LinkedIn discovery done');
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ err: String(err) }, 'Person LinkedIn worker failed');
    throw err;
  }
}, { connection, concurrency: 1 });
