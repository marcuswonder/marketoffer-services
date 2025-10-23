import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { fetch } from "undici";
import { logger } from "../lib/logger.js";
import { query } from "../lib/db.js";
import { normalizeWord, nameMatches } from "../lib/normalize.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { base } from "../lib/airtable.js";
const WRITE_TO_AIRTABLE = (process.env.WRITE_TO_AIRTABLE || "").toLowerCase() === 'true';

const SERPER_API_KEY = process.env.SERPER_API_KEY || "";
const LLM_DEBUG_LOGS = (process.env.LLM_DEBUG_LOGS || "").toLowerCase() === 'true';
const PERSON_SERPER_JITTER_MS = 900;
const PERSON_LI_ACCEPT_THRESHOLD = Number(process.env.PERSON_LI_ACCEPT_THRESHOLD || 0.7);

async function serperSearch(q: string): Promise<{ status: number; headers: Record<string, string>; data: any }> {
  const res = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ q, gl: "uk", hl: "en", autocorrect: true })
  });
  const status = res.status;
  const headers: Record<string, string> = {};
  try { res.headers.forEach((v, k) => { headers[k] = v; }); } catch {}
  if (!res.ok) throw new Error(`Serper ${res.status}`);
  const data = await res.json();
  return { status, headers, data };
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

function dedupeStrings(values: any[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const v of values || []) {
    if (v == null) continue;
    const str = typeof v === 'string' ? v : String(v);
    const trimmed = str.trim();
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(trimmed);
  }
  return out;
}

type CompanyDetail = {
  name?: string;
  number?: string;
  status?: string;
  postcode?: string;
  address?: string;
  role?: string;
  sicCodes?: string[];
};

async function scorePersonalCandidate(opts: {
  fullName: string;
  dob?: string;
  companies: string[];
  companyNumbers: string[];
  companyDetails: CompanyDetail[];
  locations: string[];
  trading: string[];
  websites: string[];
  candidate: { url: string; title?: string; snippet?: string };
  jobId: string;
}) {
  const payload = {
    task: 'score_personal_linkedin',
    person: { name: opts.fullName, dob: opts.dob || '' },
    companies: opts.companies,
    company_numbers: opts.companyNumbers,
    company_details: opts.companyDetails,
    locations: opts.locations,
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
  if (!res.ok) {
    await logEvent(opts.jobId, 'debug', 'LLM res failed', { url: opts.candidate.url, title: opts.candidate.title, status: res.status, statusText: res.statusText });
    return null;
  }
  const data: any = await res.json();
  try {
    const usage = (data as any)?.usage || null;
    const model = (data as any)?.model || (data as any)?.id || 'unknown';
    if (usage) {
      await logEvent(opts.jobId, 'info', 'LLM usage', { worker: 'person', kind: 'personal', model, usage });
    }
  } catch {}
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
  if (!res.ok) { 
    await logEvent(opts.jobId, 'debug', 'LLM res failed', { url: opts.candidate.url, title: opts.candidate.title || '', status: res.status, statusText: res.statusText });
    return null;
  }
  const data: any = await res.json();
  try {
    const usage = (data as any)?.usage || null;
    const model = (data as any)?.model || (data as any)?.id || 'unknown';
    if (usage) {
      await logEvent(opts.jobId, 'info', 'LLM usage', { worker: 'person', kind: 'company', model, usage });
    }
  } catch {}
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
  context?: {
    companyNumber?: string;
    companyName?: string;
    tradingNames?: string[];
    activeTradingNames?: string[];
    registeredNames?: string[];
    activeRegisteredNames?: string[];
    websites?: string[];
    companyLinkedIns?: string[];
    personalLinkedIns?: string[];
    appointments?: Array<{
      companyNumber?: string;
      companyName?: string;
      companyStatus?: string;
      registeredAddress?: string;
      registeredPostcode?: string;
      role?: string;
    }>;
    propertyAddress?: string;
    postcode?: string;
    city?: string;
    unit?: string;
    openRegister?: {
      source?: string | null;
      address?: string | null;
      town?: string | null;
      postcode?: string | null;
      firstSeenYear?: number | null;
      lastSeenYear?: number | null;
    };
    ownerDiscovery?: {
      jobId?: string;
      score?: number | null;
      rank?: number | null;
      dataSources?: string[];
      indicators?: string[];
      companyNumbers?: string[];
      companyNames?: string[];
      latestSaleYear?: number | null;
      reason?: string | null;
    };
  };
  rootJobId?: string;
};

export default new Worker("person-linkedin", async job => {
  const { personId, person, context, rootJobId, parentJobId, requestSource } = job.data as PersonSearchPayload & {
    parentJobId?: string;
    requestSource?: string;
  };
  const resolvedRoot = rootJobId || (job.id as string);
  await startJob({
    jobId: job.id as string,
    queue: 'person-linkedin',
    name: job.name,
    payload: job.data,
    rootJobId: resolvedRoot,
    parentJobId: parentJobId || null,
    requestSource: requestSource || 'person-linkedin',
  });
  try {
    // High-level: record job receipt for timeline visibility
    await logEvent(job.id as string, 'info', 'Person LI job received', {
      hasPersonId: Boolean(personId),
      hasPersonPayload: Boolean(person),
      hasContext: Boolean(context),
      rootJobId: rootJobId || null
    });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI: job received', { childJobId: job.id, hasPersonId: Boolean(personId) }); } catch {}
    }
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
  const websites: string[] = Array.isArray(context?.websites) ? (context.websites as string[]) : [];
  const siteHosts = Array.from(new Set(websites.map(w => (w || '').toString().replace(/^https?:\/\//i, '').replace(/\/$/, '')))).filter(Boolean);
  const hostTradingNames = dedupeStrings(siteHosts.map(h => hostLabel(h)).filter(Boolean));
  const tradingNamesAll = dedupeStrings([
    ...(Array.isArray(context?.tradingNames) ? (context.tradingNames as any[]) : []),
    ...hostTradingNames
  ]);
  const tradingNamesActive = dedupeStrings(Array.isArray(context?.activeTradingNames) ? (context.activeTradingNames as any[]) : []);
  const registeredNamesAll = dedupeStrings([
    ...(Array.isArray(context?.registeredNames) ? (context.registeredNames as any[]) : []),
    context?.companyName || ''
  ]);
  const registeredNamesActive = dedupeStrings(Array.isArray(context?.activeRegisteredNames) ? (context.activeRegisteredNames as any[]) : []);
  const companyNameCtx: string = (context?.companyName || '').toString().trim();
  const companyNames = Array.from(new Set([companyNameCtx, ...registeredNamesAll, ...tradingNamesAll])).filter(Boolean);
  const appointmentsRaw = Array.isArray(context?.appointments) ? (context.appointments as any[]) : [];
  const companyDetails = appointmentsRaw
    .map((app: any) => {
      const name = (app?.companyName || app?.company_name || '').toString().trim();
      const number = (app?.companyNumber || app?.company_number || '').toString().trim();
      const status = (app?.companyStatus || app?.company_status || '').toString().trim();
      const postcode = (app?.registeredPostcode || app?.registered_postcode || '').toString().trim();
      const address = (app?.registeredAddress || app?.registered_address || '').toString().trim();
      const role = (app?.role || app?.appointmentRole || app?.appointment_role || '').toString().trim();
      const sicCodesRaw = Array.isArray(app?.sic_codes) ? app.sic_codes : Array.isArray(app?.sicCodes) ? app.sicCodes : [];
      const sicCodes = sicCodesRaw
        .map((code: any) => (code == null ? '' : String(code).trim()))
        .filter(Boolean);
      if (!name && !number && !address && !postcode && !status && !role && !sicCodes.length) return null;
      return {
        name: name || undefined,
        number: number || undefined,
        status: status || undefined,
        postcode: postcode || undefined,
        address: address || undefined,
        role: role || undefined,
        sicCodes: sicCodes.length ? sicCodes : undefined
      };
    })
    .filter(Boolean) as CompanyDetail[];
  const ownerDiscoveryCtx = context?.ownerDiscovery;
  const companyNumbers = dedupeStrings([
    ...(Array.isArray(ownerDiscoveryCtx?.companyNumbers) ? (ownerDiscoveryCtx?.companyNumbers as any[]) : []),
    ...companyDetails.map(d => d.number || '').filter(Boolean)
  ]);
  const locationHintsSet = new Set<string>();
  const addLocation = (val?: string | null) => {
    if (val == null) return;
    const str = val.toString().trim();
    if (!str) return;
    locationHintsSet.add(str);
  };
  addLocation(context?.propertyAddress);
  addLocation(context?.city);
  addLocation(context?.postcode);
  addLocation(context?.unit);
  const openRegisterCtx = context?.openRegister;
  if (openRegisterCtx) {
    addLocation(openRegisterCtx.address ?? null);
    addLocation(openRegisterCtx.town ?? null);
    addLocation(openRegisterCtx.postcode ?? null);
  }
  for (const detail of companyDetails) {
    addLocation(detail.address);
    addLocation(detail.postcode);
    const companyName = detail.name;
    if (companyName) addLocation(companyName);
  }
  const locationHints = dedupeStrings(Array.from(locationHintsSet));
  const personalSeeds: string[] = Array.isArray(context?.personalLinkedIns) ? (context.personalLinkedIns as string[]) : [];
  const companySeeds: string[] = Array.isArray(context?.companyLinkedIns) ? (context.companyLinkedIns as string[]) : [];

  const personalQueries: string[] = [];
  const companyQueries: string[] = [];
  const addPersonalQuery = (base: string, tokens: string[] = []) => {
    if (!base.trim()) return;
    const parts = [base.trim(), ...tokens].filter(Boolean);
    const query = `${parts.join(' ')} UK site:linkedin.com/in`;
    personalQueries.push(query.replace(/\s+/g, ' ').trim());
  };

  const tradingActiveQuoted = tradingNamesActive.map(n => `"${n}"`);
  const registeredAllQuoted = registeredNamesAll.map(n => `"${n}"`);
  const tradingActiveLower = new Set(tradingNamesActive.map(n => n.toLowerCase()));
  const inactiveTradingNames = tradingNamesAll
    .filter(t => !tradingActiveLower.has(t.toLowerCase()))
    .map(n => `"${n}"`);

  if (fullName.trim()) {
    addPersonalQuery(fullName);
    if (dob) addPersonalQuery(fullName, [dob]);
    if (tradingNamesActive.length > 1) addPersonalQuery(fullName, tradingActiveQuoted);
    for (const t of tradingNamesActive) addPersonalQuery(fullName, [`"${t}"`]);
    for (const t of inactiveTradingNames) addPersonalQuery(fullName, [t]);
    for (const c of registeredAllQuoted) addPersonalQuery(fullName, [c]);
    if (companyNames.length) addPersonalQuery(fullName, companyNames.map(c => `"${c}"`));
  }
  // Also run searches excluding middle names (first + last only)
  if (shortName && shortName !== fullName) {
    addPersonalQuery(shortName);
    if (dob) addPersonalQuery(shortName, [dob]);
    if (tradingNamesActive.length > 1) addPersonalQuery(shortName, tradingActiveQuoted);
    for (const t of tradingNamesActive) addPersonalQuery(shortName, [`"${t}"`]);
    for (const t of inactiveTradingNames) addPersonalQuery(shortName, [t]);
    for (const c of registeredAllQuoted) addPersonalQuery(shortName, [c]);
    if (companyNames.length) addPersonalQuery(shortName, companyNames.map(c => `"${c}"`));
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
      tradingNames: tradingNamesAll,
      tradingNamesActive,
      registeredNames: registeredNamesAll,
      registeredNamesActive,
      companyNames
    });
    if (rootJobId) {
      try {
        const personalFull = personalQueries.filter(q => fullName && q.startsWith(`${fullName} `));
        const personalShort = shortName && shortName !== fullName ? personalQueries.filter(q => q.startsWith(`${shortName} `)) : [];
        await logEvent(rootJobId, 'info', 'Person LI built queries', {
          fullName,
          shortName,
          counts: { personal_total: personalQueries.length, full: personalFull.length, short: personalShort.length },
          samples: { full: personalFull.slice(0, 3), short: personalShort.slice(0, 3) },
          companyQueries: companyQueries.slice(0, 3),
          tradingNamesActive,
          registeredNames: registeredNamesAll
        });
      } catch {}
    }

    type Hit = { url: string; title?: string; snippet?: string };
    const personalHits: Hit[] = [];
    const companyHits: Hit[] = [];
    // Track first-seen Serper organic item per canonical URL for later logging
    const serperMetaByUrl = new Map<string, any>();
    const totalQueries = personalQueries.length + companyQueries.length;
    let serperCalls = 0;
    if (totalQueries === 0) {
      await logEvent(job.id as string, 'warn', 'No queries to run', { reason: 'empty_name_or_context' });
      if (rootJobId) {
        try { await logEvent(rootJobId, 'warn', 'Person LI: no queries to run', { childJobId: job.id }); } catch {}
      }
    } else {
      await logEvent(job.id as string, 'info', 'Executing search queries', { total: totalQueries, personal: personalQueries.length, company: companyQueries.length });
      if (rootJobId) {
        try { await logEvent(rootJobId, 'info', 'Person LI: executing search queries', { childJobId: job.id, total: totalQueries }); } catch {}
      }
    }
    let processed = 0;
    const infoEvery = Math.max(1, Math.floor(totalQueries / 5));
    for (const q of [...personalQueries, ...companyQueries]) {
      const resp = await serperSearch(q);
      serperCalls += 1;
      const organic = Array.isArray((resp.data as any).organic) ? (resp.data as any).organic : [];
      // Log Serper HTTP response meta + sample results
      try {
        const sample = organic.slice(0, 5).map((it: any) => ({ link: it.link, title: it.title, snippet: it.snippet }));
        const hdr = resp.headers || {};
        await logEvent(job.id as string, 'info', 'Serper fetch', {
          query: q,
          status: resp.status,
          headers: { 'x-request-id': hdr['x-request-id'] || hdr['x-requestid'] || hdr['request-id'] || '' },
          counts: { organic: organic.length },
          sample
        });
      } catch {}
      // Log Serper raw results (trimmed) so we can assess query quality
      try {
        const sample = organic.slice(0, 5).map((it: any) => ({
          link: it.link,
          title: it.title,
          snippet: it.snippet
        }));
        await logEvent(job.id as string, 'info', 'Serper organic results', {
          query: q,
          count: organic.length,
          sample
        });
      } catch {}
      const pBefore = personalHits.length;
      const cBefore = companyHits.length;
      for (const it of organic) {
        const link: string = it.link || '';
        const title: string = it.title || '';
        const snippet: string = it.snippet || '';
        if (!link) continue;
        if (isPersonalLI(link)) personalHits.push({ url: link, title, snippet });
        else if (isCompanyLI(link)) companyHits.push({ url: link, title, snippet });

        // Save Serper metadata keyed by canonical URL (first seen wins)
        const c = canonLinkedIn(link);
        if (c && !serperMetaByUrl.has(c)) {
          serperMetaByUrl.set(c, { ...it, query: q });
        }
      }
      const pThis = personalHits.length - pBefore;
      const cThis = companyHits.length - cBefore;
      processed += 1;
      await logEvent(job.id as string, 'debug', 'Query processed', {
        q,
        kind: /site:linkedin\.com\/in\b/i.test(q) ? 'personal' : (/site:linkedin\.com\/company\b/i.test(q) ? 'company' : 'unknown'),
        hitsThisQuery: { personal: pThis, company: cThis },
        found: { personal: personalHits.length, company: companyHits.length },
        processed,
        total: totalQueries
      });
      // Emit periodic info-level progress for UI visibility
      if (processed === totalQueries || processed % infoEvery === 0) {
        await logEvent(job.id as string, 'info', 'Search progress', {
          processed,
          total: totalQueries,
          lastQuery: q,
          kind: /site:linkedin\.com\/in\b/i.test(q) ? 'personal' : (/site:linkedin\.com\/company\b/i.test(q) ? 'company' : 'unknown'),
          hitsThisQuery: { personal: pThis, company: cThis },
          found: { personal: personalHits.length, company: companyHits.length }
        });
        if (rootJobId) {
          try {
            await logEvent(rootJobId, 'info', 'Person LI: search progress', {
              childJobId: job.id,
              processed,
              total: totalQueries,
              lastQuery: q,
              kind: /site:linkedin\.com\/in\b/i.test(q) ? 'personal' : (/site:linkedin\.com\/company\b/i.test(q) ? 'company' : 'unknown'),
              hitsThisQuery: { personal: pThis, company: cThis },
              found: { personal: personalHits.length, company: companyHits.length }
            });
          } catch {}
        }
      }
      await sleep(PERSON_SERPER_JITTER_MS);
    }

    // Normalize, dedupe, and seed with siteFetch links
    const personalSet = new Map<string, Hit>();
    const companySet = new Map<string, Hit>();
    await logEvent(job.id as string, 'info', 'Seeding candidate sets from context', { personalSeedCount: personalSeeds.length, companySeedCount: companySeeds.length });
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
    // Log candidate URL lists for visibility
    const personalUrls = Array.from(personalSet.keys());
    const companyUrls = Array.from(companySet.keys());
    await logEvent(job.id as string, 'info', 'Candidate URLs', {
      personal: personalUrls,
      company: companyUrls
    });
    // Also include Serper metadata (raw organic items where available)
    const personalSerper = personalUrls.map(u => ({ url: u, serper: serperMetaByUrl.get(u) || { source: 'seed' } }));
    const companySerper = companyUrls.map(u => ({ url: u, serper: serperMetaByUrl.get(u) || { source: 'seed' } }));
    await logEvent(job.id as string, 'info', 'Serper returns for candidates', {
      personal: personalSerper,
      company: companySerper
    });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI aggregated candidates', { counts: { personal: personalSet.size, company: companySet.size } }); } catch {}
    }

    // Score candidates with LLM
    const personalCandidates = Array.from(personalSet.values());
    const companyCandidates = Array.from(companySet.values());
    await logEvent(job.id as string, 'info', 'Scoring phase started', { personal: personalCandidates.length, company: companyCandidates.length });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI: scoring started', { childJobId: job.id, personal: personalCandidates.length, company: companyCandidates.length }); } catch {}
    }

    const personalScored: Array<{ url: string; relevance: number; confidence: number; rationale: string }> = [];
    let psProcessed = 0;
    for (const c of personalCandidates) {
      const s = await scorePersonalCandidate({
        fullName,
        dob,
        companies: companyNames,
        companyNumbers,
        companyDetails,
        locations: locationHints,
        trading: tradingNamesAll,
        websites: siteHosts,
        candidate: c,
        jobId: job.id as string,
      });
      if (s) {
        personalScored.push({ url: c.url, ...s });
        await logEvent(job.id as string, 'info', 'AI score (personal)', { url: c.url, relevance: s.relevance, confidence: s.confidence });
      }
      psProcessed += 1;
      await logEvent(job.id as string, 'debug', 'Scored personal candidate', { i: psProcessed, n: personalCandidates.length, url: c.url });
      await sleep(500);
    }

    const companyScored: Array<{ url: string; relevance: number; confidence: number; rationale: string }> = [];
    let csProcessed = 0;
    for (const c of companyCandidates) {
      const s = await scoreCompanyCandidate({ companyNames, trading: tradingNamesAll, candidate: c, jobId: job.id as string });
      if (s) {
        companyScored.push({ url: c.url, ...s });
        await logEvent(job.id as string, 'info', 'AI score (company)', { url: c.url, relevance: s.relevance, confidence: s.confidence });
      }
      csProcessed += 1;
      await logEvent(job.id as string, 'debug', 'Scored company candidate', { i: csProcessed, n: companyCandidates.length, url: c.url });
      await sleep(400);
    }

    personalScored.sort((a,b)=> (b.relevance - a.relevance) || (b.confidence - a.confidence));
    companyScored.sort((a,b)=> (b.relevance - a.relevance) || (b.confidence - a.confidence));

    await logEvent(job.id as string, 'info', 'Scored LinkedIn candidates', {
      scope: 'summary',
      personal_top: personalScored.slice(0,3),
      company_top: companyScored.slice(0,3)
    });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI scored candidates', { personal_top: personalScored.slice(0,3), company_top: companyScored.slice(0,3) }); } catch {}
    }

    // Persist top-rated LinkedIns into ch_appointments for the matching person under this workflow
    try {
      if (rootJobId) {
        const acceptedPersonal = personalScored.filter(x => (x.relevance ?? 0) >= PERSON_LI_ACCEPT_THRESHOLD);
        const acceptedCompany = companyScored.filter(x => (x.relevance ?? 0) >= PERSON_LI_ACCEPT_THRESHOLD);
        if (acceptedPersonal.length || acceptedCompany.length) {
          const { rows: ppl } = await query<{ id: number; first_name: string; last_name: string; full_name: string }>(
            `SELECT id, first_name, last_name, full_name FROM ch_people WHERE job_id = $1`,
            [rootJobId]
          );
          let targetPersonId: number | null = null;
          for (const r of ppl) {
            const cand = { first: r.first_name || '', last: r.last_name || '' };
            if (nameMatches({ first, last }, cand)) { targetPersonId = r.id; break; }
            if (normalizeWord(r.first_name || '') === normalizeWord(first) && normalizeWord(r.last_name || '') === normalizeWord(last)) { targetPersonId = r.id; break; }
          }
          if (!targetPersonId && ppl.length === 1) targetPersonId = ppl[0].id;

          if (targetPersonId) {
            const personalUrlMap = new Map<string, { url: string; relevance: number; confidence: number; rationale: string }>();
            for (const cand of acceptedPersonal) {
              const url = (cand.url || '').trim();
              if (!url) continue;
              if (!personalUrlMap.has(url)) {
                personalUrlMap.set(url, { url, relevance: cand.relevance, confidence: cand.confidence, rationale: cand.rationale });
              }
            }
            const companyUrlMap = new Map<string, { url: string; relevance: number; confidence: number; rationale: string }>();
            for (const cand of acceptedCompany) {
              const url = (cand.url || '').trim();
              if (!url) continue;
              if (!companyUrlMap.has(url)) {
                companyUrlMap.set(url, { url, relevance: cand.relevance, confidence: cand.confidence, rationale: cand.rationale });
              }
            }
            const addPersonalUrls = JSON.stringify(Array.from(personalUrlMap.keys()));
            const addPersonalVerif = JSON.stringify(Array.from(personalUrlMap.values()));
            const addCompanyUrls = JSON.stringify(Array.from(companyUrlMap.keys()));
            const addCompanyVerif = JSON.stringify(Array.from(companyUrlMap.values()));
            await query(
              `UPDATE ch_appointments
                 SET
                   verified_director_linkedIns = (
                     SELECT CASE WHEN jsonb_typeof(COALESCE(verified_director_linkedIns, '[]'::jsonb)) = 'array'
                                 THEN (
                                   SELECT jsonb_agg(DISTINCT j.value)
                                     FROM jsonb_array_elements(COALESCE(verified_director_linkedIns, '[]'::jsonb) || $1::jsonb) AS j(value)
                                 )
                                 ELSE $1::jsonb
                            END
                   ),
                   director_linkedIn_verification = (
                     SELECT CASE WHEN jsonb_typeof(COALESCE(director_linkedIn_verification, '[]'::jsonb)) = 'array'
                                 THEN (
                                   SELECT jsonb_agg(DISTINCT j.value)
                                     FROM jsonb_array_elements(COALESCE(director_linkedIn_verification, '[]'::jsonb) || $2::jsonb) AS j(value)
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
                   company_linkedIn_verification = (
                     SELECT CASE WHEN jsonb_typeof(COALESCE(company_linkedIn_verification, '[]'::jsonb)) = 'array'
                                 THEN (
                                   SELECT jsonb_agg(DISTINCT j.value)
                                     FROM jsonb_array_elements(COALESCE(company_linkedIn_verification, '[]'::jsonb) || $4::jsonb) AS j(value)
                                 )
                                 ELSE $4::jsonb
                            END
                   ),
                   updated_at = now()
               WHERE person_id = $5`,
              [addPersonalUrls, addPersonalVerif, addCompanyUrls, addCompanyVerif, targetPersonId]
            );
            await query(
              `UPDATE ch_people
                 SET
                   verified_director_linkedIns = (
                     SELECT CASE WHEN jsonb_typeof(COALESCE(verified_director_linkedIns, '[]'::jsonb)) = 'array'
                                 THEN (
                                   SELECT jsonb_agg(DISTINCT j.value)
                                     FROM jsonb_array_elements(COALESCE(verified_director_linkedIns, '[]'::jsonb) || $1::jsonb) AS j(value)
                                 )
                                 ELSE $1::jsonb
                            END
                   ),
                   director_linkedIn_verification = (
                     SELECT CASE WHEN jsonb_typeof(COALESCE(director_linkedIn_verification, '[]'::jsonb)) = 'array'
                                 THEN (
                                   SELECT jsonb_agg(DISTINCT j.value)
                                     FROM jsonb_array_elements(COALESCE(director_linkedIn_verification, '[]'::jsonb) || $2::jsonb) AS j(value)
                                 )
                                 ELSE $2::jsonb
                            END
                   ),
                   person_linkedin_job_ids = (
                     SELECT ARRAY(SELECT DISTINCT UNNEST(COALESCE(person_linkedin_job_ids,'{}') || ARRAY[$3::text]))
                   ),
                   updated_at = now()
               WHERE id = $4`,
              [addPersonalUrls, addPersonalVerif, job.id as string, targetPersonId]
            );
            await logEvent(job.id as string, 'info', 'Persisted LinkedIns to ch_people', { person_id: targetPersonId, personal_total: personalUrlMap.size });
            await logEvent(job.id as string, 'info', 'Persisted LinkedIns to ch_appointments', { person_id: targetPersonId, personal_added: acceptedPersonal.length, company_added: acceptedCompany.length, threshold: PERSON_LI_ACCEPT_THRESHOLD });
            if (rootJobId) {
              try { await logEvent(rootJobId, 'info', 'Person LI: persisted LinkedIns', { childJobId: job.id, person_id: targetPersonId, personal_added: acceptedPersonal.length, company_added: acceptedCompany.length }); } catch {}
            }
          } else {
            await logEvent(job.id as string, 'warn', 'Could not match person for persistence', { rootJobId, fullName, first, last });
          }
        } else {
          await logEvent(job.id as string, 'info', 'No LinkedIns met threshold for persistence', { threshold: PERSON_LI_ACCEPT_THRESHOLD });
        }
      }
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Failed to persist LinkedIns to ch_appointments', { error: String(e) });
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
    try { await logEvent(job.id as string, 'info', 'Usage summary', { serper_calls: serperCalls }); } catch {}
    await logEvent(job.id as string, 'info', 'Person LI completed', { scope: 'summary', personal: personalScored.length, company: companyScored.length });
    if (rootJobId) {
      try { await logEvent(rootJobId, 'info', 'Person LI: completed', { childJobId: job.id, personal: personalScored.length, company: companyScored.length }); } catch {}
    }
    logger.info({ personId: personId || null, personal: personalScored.length, company: companyScored.length }, 'Person LinkedIn discovery done');
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ err: String(err) }, 'Person LinkedIn worker failed');
  }
}, { connection, concurrency: 1 });
