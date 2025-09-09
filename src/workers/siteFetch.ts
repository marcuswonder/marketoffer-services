import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { personQ } from "../queues/index.js";
import { initDb, startJob, logEvent, completeJob, failJob } from "../lib/progress.js";
import { logger } from "../lib/logger.js";
import { fetch } from "undici";
import { cleanCompanyName, baseHost } from "../lib/normalize.js";
import { query } from "../lib/db.js";

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

    // Persist results into ch_appointments for this company
    try {
      const addWebsites = JSON.stringify(validated_websites);
      const addVerifications = JSON.stringify(website_validations);
      const addLinkedIns = JSON.stringify(Array.from(foundLinkedIns));
      await query(
        `UPDATE ch_appointments
           SET
             verified_company_website = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(verified_company_website, '[]'::jsonb)) = 'array'
                           THEN (
                             SELECT jsonb_agg(DISTINCT e)
                               FROM jsonb_array_elements(COALESCE(verified_company_website, '[]'::jsonb) || $1::jsonb) AS e
                           )
                           ELSE $1::jsonb
                      END
             ),
             company_website_verification = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(company_website_verification, '[]'::jsonb)) = 'array'
                           THEN (
                             SELECT jsonb_agg(DISTINCT e)
                               FROM jsonb_array_elements(COALESCE(company_website_verification, '[]'::jsonb) || $2::jsonb) AS e
                           )
                           ELSE $2::jsonb
                      END
             ),
             verified_company_linkedIns = (
               SELECT CASE WHEN jsonb_typeof(COALESCE(verified_company_linkedIns, '[]'::jsonb)) = 'array'
                           THEN (
                             SELECT jsonb_agg(DISTINCT e)
                               FROM jsonb_array_elements(COALESCE(verified_company_linkedIns, '[]'::jsonb) || $3::jsonb) AS e
                           )
                           ELSE $3::jsonb
                      END
             ),
             updated_at = now()
         WHERE company_number = $4`,
        [addWebsites, addVerifications, addLinkedIns, companyNumber]
      );
      await logEvent(job.id as string, 'info', 'Updated ch_appointments with siteFetch results', {
        companyNumber,
        websites_added: validated_websites.length,
        linkedins_added: Array.from(foundLinkedIns).length
      });
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Failed to update ch_appointments with siteFetch results', { error: String(e), companyNumber });
    }

    // After this job is marked completed, check if any site-fetch jobs for this company are still running
    try {
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
        // Aggregate websites/linkedin across all appointments for this company
        const aggSql = `
          WITH w AS (
            SELECT jsonb_array_elements(COALESCE(verified_company_website, '[]'::jsonb)) AS v
              FROM ch_appointments
             WHERE company_number = $1
          ),
          l AS (
            SELECT jsonb_array_elements(COALESCE(verified_company_linkedIns, '[]'::jsonb)) AS v
              FROM ch_appointments
             WHERE company_number = $1
          )
          SELECT
            COALESCE((SELECT jsonb_agg(DISTINCT v) FROM w), '[]'::jsonb) AS websites,
            COALESCE((SELECT jsonb_agg(DISTINCT v) FROM l), '[]'::jsonb) AS linkedins`;
        const { rows: aggRows } = await query<any>(aggSql, [companyNumber]);
        const aggWebsites: string[] = Array.isArray(aggRows?.[0]?.websites) ? aggRows[0].websites : [];
        const aggLinkedIns: string[] = Array.isArray(aggRows?.[0]?.linkedins) ? aggRows[0].linkedins : [];

        // Find distinct people with appointments in this company
        const { rows: people } = await query<{ id: number; first_name: string | null; middle_names: string | null; last_name: string | null; dob_string: string | null }>(
          `SELECT DISTINCT p.id, p.first_name, p.middle_names, p.last_name, p.dob_string
             FROM ch_people p
             JOIN ch_appointments a ON a.person_id = p.id
            WHERE a.company_number = $1`,
          [companyNumber]
        );

        // Grab a representative company name for context (latest entry wins)
        const { rows: cnRows } = await query<{ company_name: string }>(
          `SELECT company_name
             FROM ch_appointments
            WHERE company_number = $1 AND COALESCE(company_name,'') <> ''
            ORDER BY updated_at DESC
            LIMIT 1`,
          [companyNumber]
        );
        const companyNameForContext = (cnRows?.[0]?.company_name || '').toString();

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
                websites: aggWebsites,
                companyLinkedIns: aggLinkedIns
              }
            },
            { jobId, attempts: 5, backoff: { type: 'exponential', delay: 2000 } }
          );
          enqueued++;
        }
        await logEvent(job.id as string, 'info', 'Enqueued person-linkedin searches after siteFetch aggregation', { companyNumber, people: people.length, enqueued });
      }
    } catch (e) {
      await logEvent(job.id as string, 'error', 'Aggregation/queue after siteFetch failed', { error: String(e), companyNumber });
    }

    logger.info({ host, score: bestScore, validates: validated_websites.length }, 'Site fetch done');
  } catch (err) {
    await failJob(job.id as string, err);
    logger.error({ host, err: String(err) }, 'Site fetch failed');
    throw err;
  }
}, { connection, concurrency: 1 });
