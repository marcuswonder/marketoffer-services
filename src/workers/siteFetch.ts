import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { logger } from "../lib/logger.js";
import { fetch } from "undici";
import { cleanCompanyName, baseHost } from "../lib/normalize.js";

type JobPayload = {
  host: string;
  companyNumber: string;
  companyName: string;
  address?: string;
  postcode?: string;
};

const BEE_KEY = process.env.SCRAPINGBEE_API_KEY || "";
const OPENAI_KEY = process.env.OPENAI_API_KEY || "";
const BEE_MAX_PAGES = Number(process.env.SITEFETCH_BEE_MAX_PAGES || 1); // cap Bee calls per job
const STATIC_TIMEOUT_MS = Number(process.env.SITEFETCH_STATIC_TIMEOUT_MS || 7000);

const COMMON_PATHS = [
  "/",
  "/about",
  "/about-us",
  "/company",
  "/our-company",
  "/team",
  "/contact",
  "/privacy",
  "/privacy-policy",
  "/impressum"
];

await initDb();

function buildBeeUrl(target: string) {
  const u = new URL("https://app.scrapingbee.com/api/v1/");
  u.searchParams.set("api_key", BEE_KEY);
  u.searchParams.set("url", target);
  u.searchParams.set("render_js", "true");
  u.searchParams.set("country_code", "gb");
  u.searchParams.set("block_resources", "true");
  // modest timeout
  u.searchParams.set("timeout", "15000");
  return u.toString();
}

async function fetchBeeHtml(targetUrl: string): Promise<{ status: number; html: string }> {
  if (!BEE_KEY) throw new Error("Missing SCRAPINGBEE_API_KEY");
  const beeUrl = buildBeeUrl(targetUrl);
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

function extractBetween(html: string, re: RegExp): string[] {
  const out: string[] = [];
  let m: RegExpExecArray | null;
  const r = new RegExp(re.source, re.flags.includes('g') ? re.flags : re.flags + 'g');
  while ((m = r.exec(html))) {
    out.push((m[1] || "").trim());
  }
  return out;
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
  const emails = Array.from(new Set((html.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || []).map(e => e.toLowerCase())));
  const linkedins = Array.from(new Set((html.match(/https?:\/\/(?:[\w.-]*\.)?linkedin\.com\/[\w\-\/\?=&#.%]+/gi) || [])));
  return { title, metaDesc, h1s, jsonld, textChunk, emails, linkedins };
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
  const numHit = companyNumber ? (signals.textChunk.includes(companyNumber.toLowerCase()) ? 1 : 0) : 0;
  const pcHit = postcode ? (signals.textChunk.includes(postcode.toLowerCase()) ? 1 : 0) : 0;
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
    0.25 * titleHit +
    0.20 * h1Hit +
    0.15 * metaHit +
    0.30 * numHit +
    0.10 * pcHit +
    0.15 * emailHit +
    0.25 * orgHit
  );
  const matches: string[] = [];
  if (titleHit) matches.push("name_in_title");
  if (h1Hit) matches.push("name_in_h1");
  if (metaHit) matches.push("name_in_meta");
  if (numHit) matches.push("company_number");
  if (pcHit) matches.push("postcode");
  if (emailHit) matches.push("email_domain_match");
  if (orgHit) matches.push("jsonld_organization_name");
  const rationale = matches.length ? `Matched: ${matches.join(", ")}` : "No strong ownership signals found";
  return { score: Math.min(1, score), rationale };
}

function findLabeledCompanyNumber(text: string): string | null {
  // Look for phrases like "company number", "company no", "registered number" followed by an 8-digit or 2 letters + 6 digits id
  const re = /(company\s*(number|no)\b|registered\s*(number|no)\b|reg(istration)?\s*(no|number)\b)[^A-Za-z0-9]{0,10}([A-Z]{2}\d{6}|\d{8})/i;
  const m = text.match(re);
  return m ? (m[6] || "").toUpperCase() : null;
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

export default new Worker("site-fetch", async job => {
  const { host, companyNumber, companyName, address, postcode } = job.data as JobPayload;
  await startJob({ jobId: job.id as string, queue: 'site-fetch', name: job.name, payload: job.data });
  try {
    const urls: string[] = [];
    const httpsBase = `https://${host}`;
    for (const p of COMMON_PATHS) urls.push(httpsBase + p);

    let bestScore = 0;
    let bestRationale = "";
    const foundLinkedIns = new Set<string>();
    const staticOk: Array<{ url: string; status: number; html: string }> = [];
    const targetNum = (companyNumber || "").toUpperCase();
    let abortHost = false;
    let earlyAccepted = false;

    // First pass: cheap static fetch to avoid ScrapingBee credits on obvious 404/robots/generic pages
    for (const url of urls) {
      const r = await fetchStaticHtml(url);
      if (r.status >= 200 && r.status < 400 && r.html.length >= 200) {
        staticOk.push({ url, status: r.status, html: r.html });
        const sig = parseHtmlSignals(r.html);
        for (const li of sig.linkedins) foundLinkedIns.add(li);
        // Early decision: if page explicitly declares a different company number, abort further checks for this host
        const labeled = findLabeledCompanyNumber(sig.textChunk);
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
        const { score, rationale } = scoreOwnership({ host, companyName, companyNumber, postcode, signals: sig });
        if (score > bestScore) { bestScore = score; bestRationale = rationale; }
      }
      await new Promise(r => setTimeout(r, 250));
      if (abortHost || earlyAccepted) break;
    }

    if (!staticOk.length) {
      await logEvent(job.id as string, 'info', 'No static pages reachable; skipping Bee', { host });
    }

    // Optional escalation to ScrapingBee: only if static confidence is low and we have a reachable candidate
    if (!abortHost && bestScore < 0.6 && BEE_KEY && staticOk.length && BEE_MAX_PAGES > 0) {
      let used = 0;
      for (const candidate of staticOk.slice(0, BEE_MAX_PAGES)) {
        const r = await fetchBeeHtml(candidate.url);
        if (r.status >= 200 && r.status < 400 && r.html.length >= 200) {
          const sig = parseHtmlSignals(r.html);
          for (const li of sig.linkedins) foundLinkedIns.add(li);
          const { score, rationale } = scoreOwnership({ host, companyName, companyNumber, postcode, signals: sig });
          if (score > bestScore) { bestScore = score; bestRationale = `Bee: ${rationale}`; }
        }
        used++;
        if (used >= BEE_MAX_PAGES) break;
        await new Promise(r => setTimeout(r, 400));
      }
    }

    // Optional LLM if still unclear
    if (!abortHost && bestScore < 0.5) {
      const sampleUrl = staticOk[0]?.url || `https://${host}/`;
      try {
        const rr = staticOk[0]?.html ? { html: staticOk[0].html } : await fetchStaticHtml(sampleUrl);
        const txt = stripTags((rr as any).html || "").slice(0, 8000);
        const ai = await llmFallback(host, txt, companyName);
        if (ai && ai.score > bestScore) { bestScore = ai.score; bestRationale = `LLM: ${ai.rationale}`; }
      } catch {}
    }

    const validated_websites = bestScore >= 0.6 ? [host] : [];
    const website_validations = [
      { url: host, confidence_rating: Number(bestScore.toFixed(3)), confidence_rationale: bestRationale }
    ];

    await logEvent(job.id as string, 'info', 'SiteFetch results', {
      host,
      score: bestScore,
      linkedins: Array.from(foundLinkedIns).slice(0, 50)
    });

    await completeJob(job.id as string, {
      companyNumber,
      companyName,
      host,
      validated_websites,
      website_validations,
      linkedins: Array.from(foundLinkedIns)
    });
    logger.info({ host, score: bestScore, validates: validated_websites.length }, 'Site fetch done');
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ host, err: String(err) }, 'Site fetch failed');
    throw err;
  }
}, { connection, concurrency: 1 });
