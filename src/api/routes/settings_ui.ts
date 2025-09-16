import { Router } from 'express';
import fs from 'fs';
import path from 'path';

export const router = Router();

function num(v: any, d: number) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function bool(v: any, d = false) { const s = (v || '').toString().toLowerCase(); if (s === 'true') return true; if (s === 'false') return false; return d; }

function defaultsFromEnv() {
  // SiteFetch
  const sitefetch = {
    acceptance_threshold: num(process.env.SITEFETCH_ACCEPT_THRESHOLD, 0.75),
    llm_only: bool(process.env.LLM_ONLY_WEBSITE_VALIDATION, false),
    max_pages_for_llm: num(process.env.SITEFETCH_MAX_PAGES, 5),
    snippet_chars: num(process.env.SITEFETCH_MAX_SNIPPET_CHARS, 800)
  };
  // Scraping
  let legalPriority: string[] = [
    '/privacy', '/privacy-policy', '/impressum',
    '/terms', '/terms-of-service', '/terms-and-conditions',
    '/about', '/about-us', '/company', '/our-company', '/contact', '/'
  ];
  try {
    const p = path.join(process.cwd(), 'config', 'commonUrlEndings.json');
    const arr = JSON.parse(fs.readFileSync(p, 'utf-8')) as string[];
    if (Array.isArray(arr) && arr.length) {
      const legalFirst = arr.filter(x => /privacy|impressum|terms/i.test(x));
      const rest = arr.filter(x => !/privacy|impressum|terms/i.test(x));
      legalPriority = Array.from(new Set([...legalFirst, ...rest]));
    }
  } catch {}
  const scraping = {
    bee_max_pages: num(process.env.SITEFETCH_BEE_MAX_PAGES, 1),
    legal_page_priority: legalPriority,
    deterministic_rules: { exact_crn: true, jsonld_exact_name: true, exact_postcode: true },
    weights: { email_domain_match: 0.10 }
  };
  const llm = {
    models: { 'site-fetch': 'gpt-4o-mini', 'person-linkedin': 'gpt-4o-mini', 'company-discovery': 'gpt-4o-mini' },
    temperature: { 'site-fetch': 0.1, 'person-linkedin': 0.2, 'company-discovery': 0.1 }
  };
  const serper = { gl: 'uk', hl: 'en', per_query_result_cap: 10 };
  const company_discovery = { candidates_cap: 50 };
  const person = {
    personal_candidate_cap: num(process.env.PERSON_MAX_PERSONAL_CANDIDATES, 10),
    company_candidate_cap: num(process.env.PERSON_MAX_COMPANY_CANDIDATES, 6),
    acceptance_threshold_for_persistence: num(process.env.PERSON_LI_ACCEPT_THRESHOLD, 0.7)
  };
  const persistence = { write_to_airtable: bool(process.env.WRITE_TO_AIRTABLE, false) };
  const pricing = { openai: { 'gpt-4o-mini': { prompt_per_1k: 0, completion_per_1k: 0 } }, serper: { per_call: 0 }, scrapingbee: { per_call: 0 } };
  return { sitefetch, scraping, llm, serper, company_discovery, person, persistence, pricing };
}

function renderPage(defaultsJson: string) {
  return [
  '<!doctype html>',
  '<html lang="en">',
  '<head>',
  '  <meta charset="utf-8" />',
  '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
  '  <title>Settings</title>',
  '  <style>',
  '    :root { --bg:#0f172a; --panel:#0b1220; --muted:#9ca3af; --text:#f3f4f6; --border:#243041; --accent:#60a5fa; }',
  '    html, body { height:100%; margin:0; padding:0; background:var(--bg); color:var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Inter, Arial; }',
  '    .wrap { max-width:980px; margin:24px auto; padding:20px; }',
  '    .card { background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:16px; margin-bottom:16px; }',
  '    h1 { margin:0 0 16px 0; font-size:20px; }',
  '    h2 { margin:0 0 12px 0; font-size:16px; }',
  '    .grid { display:grid; grid-template-columns: repeat(2, minmax(260px, 1fr)); gap:12px 16px; }',
  '    label { display:block; font-size:12px; color:var(--muted); margin:0 0 6px 0; }',
  '    input[type="text"], input[type="number"], textarea, select { width:100%; box-sizing:border-box; background:#0a0f1d; color:var(--text); border:1px solid var(--border); padding:8px 10px; border-radius:8px; }',
  '    textarea { min-height:90px; }',
  '    .row { display:flex; gap:10px; align-items:center; }',
  '    .topbar { display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }',
  '    button { padding:8px 12px; border-radius:8px; border:1px solid var(--border); background:#101a2d; color:var(--text); cursor:pointer; }',
  '    .actions { display:flex; gap:8px; }',
  '    pre { white-space:pre-wrap; background:#0a0f1d; color:#cbd5e1; border:1px solid var(--border); padding:10px; border-radius:8px; font-size:12px; }',
  '    a { color:var(--accent); text-decoration:none; }',
  '    .hint { color:var(--muted); font-size:11px; margin-top:6px; }',
  '  </style>',
  '</head>',
  '<body>',
  '  <div class="wrap">',
  '    <div class="topbar">',
  '      <h1>Settings (not persisted)</h1>',
  '      <div class="actions">',
  '        <a href="/admin/request"><button type="button">Back to Request</button></a>',
  '        <button id="resetBtn" type="button">Reset to defaults</button>',
  '        <button id="copyBtn" type="button">Copy JSON</button>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>SiteFetch</h2>',
  '      <div class="grid">',
  '        <div><label>Acceptance Threshold</label><input id="sf_threshold" type="number" step="0.01" min="0" max="1" /><div class="hint">0–1; >= threshold validates domain</div></div>',
  '        <div><div class="row"><input id="sf_llm_only" type="checkbox" /><label for="sf_llm_only">LLM Only</label></div><div class="hint">If enabled, always consult LLM (ignores heuristic acceptance)</div></div>',
  '        <div><label>Max Pages For LLM</label><input id="sf_max_pages" type="number" min="1" /><div class="hint">Pages to include as evidence (typical 3–8)</div></div>',
  '        <div><label>Snippet Chars</label><input id="sf_snippet" type="number" min="200" /><div class="hint">Characters per page snippet (200–4000 recommended)</div></div>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>Scraping</h2>',
  '      <div class="grid">',
  '        <div><label>Bee Max Pages</label><input id="scr_bee_max" type="number" min="0" /><div class="hint">Total Bee fetches per host (includes iframes)</div></div>',
  '        <div><label>Legal Page Priority</label><textarea id="scr_legal_priority"></textarea><div class="hint">Higher-first. One path per line (e.g. /privacy, /terms)</div></div>',
  '        <div><div class="row"><input id="scr_rule_crn" type="checkbox" /><label for="scr_rule_crn">Deterministic: Exact CRN</label></div><div class="hint">Accept if Companies House number matches</div></div>',
  '        <div><div class="row"><input id="scr_rule_jsonld" type="checkbox" /><label for="scr_rule_jsonld">Deterministic: JSON‑LD Exact Name</label></div><div class="hint">Accept if Organization.legalName exactly matches</div></div>',
  '        <div><div class="row"><input id="scr_rule_pc" type="checkbox" /><label for="scr_rule_pc">Deterministic: Exact Postcode</label></div><div class="hint">Accept if site contains the exact target postcode</div></div>',
  '        <div><label>Email‑Domain Weight</label><input id="scr_weight_email" type="number" step="0.01" min="0" max="1" /><div class="hint">Heuristic weight for matching email domain (0–1)</div></div>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>LLM</h2>',
  '      <div class="grid">',
  '        <div><label>Site‑Fetch Model</label><input id="llm_model_sf" type="text" /><div class="hint">e.g., gpt-4o-mini</div></div>',
  '        <div><label>Person‑LinkedIn Model</label><input id="llm_model_person" type="text" /><div class="hint">e.g., gpt-4o-mini</div></div>',
  '        <div><label>Company‑Discovery Model</label><input id="llm_model_company" type="text" /><div class="hint">e.g., gpt-4o-mini</div></div>',
  '        <div><label>Site‑Fetch Temperature</label><input id="llm_temp_sf" type="number" min="0" max="1" step="0.01" /><div class="hint">0–1; lower = more deterministic</div></div>',
  '        <div><label>Person‑LinkedIn Temperature</label><input id="llm_temp_person" type="number" min="0" max="1" step="0.01" /><div class="hint">0–1; lower = more deterministic</div></div>',
  '        <div><label>Company‑Discovery Temperature</label><input id="llm_temp_company" type="number" min="0" max="1" step="0.01" /><div class="hint">0–1; lower = more deterministic</div></div>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>Serper</h2>',
  '      <div class="grid">',
  '        <div><label>gl (Region)</label><input id="serper_gl" type="text" /><div class="hint">e.g., uk, us</div></div>',
  '        <div><label>hl (Language)</label><input id="serper_hl" type="text" /><div class="hint">e.g., en, fr</div></div>',
  '        <div><label>Per‑Query Result Cap</label><input id="serper_cap" type="number" min="1" /><div class="hint">Max organic results to consider (typical 10)</div></div>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>Company Discovery</h2>',
  '      <div class="grid">',
  '        <div><label>Candidates Cap</label><input id="cd_cap" type="number" min="1" /><div class="hint">Max candidate hosts to evaluate/enqueue</div></div>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>Person LinkedIn</h2>',
  '      <div class="grid">',
  '        <div><label>Personal Candidate Cap</label><input id="person_personal_cap" type="number" min="1" /><div class="hint">Max personal LinkedIn candidates to score</div></div>',
  '        <div><label>Company Candidate Cap</label><input id="person_company_cap" type="number" min="1" /><div class="hint">Max company LinkedIn candidates to score</div></div>',
  '        <div><label>Acceptance Threshold For Persistence</label><input id="person_accept" type="number" min="0" max="1" step="0.01" /><div class="hint">0–1; stored when >= threshold</div></div>',
  '      </div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>Persistence</h2>',
  '      <div><div class="row"><input id="persist_airtable" type="checkbox" /><label for="persist_airtable">WRITE_TO_AIRTABLE</label></div><div class="hint">If enabled, writes results to Airtable People/related tables</div></div>',
  '    </div>',
  '    <div class="card">',
  '      <h2>JSON Preview</h2>',
  '      <pre id="preview">{}</pre>',
  '    </div>',
  '  </div>',
  '  <script>',
  `    const DEFAULTS = ${defaultsJson};`,
  '    const $ = (id) => document.getElementById(id);',
  '    function currentValues() {',
  '      const legal = ($("scr_legal_priority").value || "").split(/\n+/).map(s => s.trim()).filter(Boolean);',
  '      return {',
  '        sitefetch: {',
  '          acceptance_threshold: Number($("sf_threshold").value || 0),',
  '          llm_only: $("sf_llm_only").checked,',
  '          max_pages_for_llm: Number($("sf_max_pages").value || 0),',
  '          snippet_chars: Number($("sf_snippet").value || 0)',
  '        },',
  '        scraping: {',
  '          bee_max_pages: Number($("scr_bee_max").value || 0),',
  '          legal_page_priority: legal,',
  '          deterministic_rules: {',
  '            exact_crn: $("scr_rule_crn").checked,',
  '            jsonld_exact_name: $("scr_rule_jsonld").checked,',
  '            exact_postcode: $("scr_rule_pc").checked',
  '          },',
  '          weights: { email_domain_match: Number($("scr_weight_email").value || 0) }',
  '        },',
  '        llm: {',
  '          models: {',
  '            "site-fetch": $("llm_model_sf").value || "",',
  '            "person-linkedin": $("llm_model_person").value || "",',
  '            "company-discovery": $("llm_model_company").value || ""',
  '          },',
  '          temperature: {',
  '            "site-fetch": Number($("llm_temp_sf").value || 0),',
  '            "person-linkedin": Number($("llm_temp_person").value || 0),',
  '            "company-discovery": Number($("llm_temp_company").value || 0)',
  '          }',
  '        },',
  '        serper: {',
  '          gl: $("serper_gl").value || "",',
  '          hl: $("serper_hl").value || "",',
  '          per_query_result_cap: Number($("serper_cap").value || 0)',
  '        },',
  '        company_discovery: { candidates_cap: Number($("cd_cap").value || 0) },',
  '        person: {',
  '          personal_candidate_cap: Number($("person_personal_cap").value || 0),',
  '          company_candidate_cap: Number($("person_company_cap").value || 0),',
  '          acceptance_threshold_for_persistence: Number($("person_accept").value || 0)',
  '        },',
  '        persistence: { write_to_airtable: $("persist_airtable").checked }',
  '      };',
  '    }',
  '    function setValues(s){',
  '      $("sf_threshold").value = s.sitefetch.acceptance_threshold;',
  '      $("sf_llm_only").checked = !!s.sitefetch.llm_only;',
  '      $("sf_max_pages").value = s.sitefetch.max_pages_for_llm;',
  '      $("sf_snippet").value = s.sitefetch.snippet_chars;',
  '      $("scr_bee_max").value = s.scraping.bee_max_pages;',
  '      $("scr_legal_priority").value = (s.scraping.legal_page_priority || []).join("\n");',
  '      $("scr_rule_crn").checked = !!s.scraping.deterministic_rules.exact_crn;',
  '      $("scr_rule_jsonld").checked = !!s.scraping.deterministic_rules.jsonld_exact_name;',
  '      $("scr_rule_pc").checked = !!s.scraping.deterministic_rules.exact_postcode;',
  '      $("scr_weight_email").value = s.scraping.weights.email_domain_match;',
  '      $("llm_model_sf").value = s.llm.models["site-fetch"];',
  '      $("llm_model_person").value = s.llm.models["person-linkedin"];',
  '      $("llm_model_company").value = s.llm.models["company-discovery"];',
  '      $("llm_temp_sf").value = s.llm.temperature["site-fetch"];',
  '      $("llm_temp_person").value = s.llm.temperature["person-linkedin"];',
  '      $("llm_temp_company").value = s.llm.temperature["company-discovery"];',
  '      $("serper_gl").value = s.serper.gl;',
  '      $("serper_hl").value = s.serper.hl;',
  '      $("serper_cap").value = s.serper.per_query_result_cap;',
  '      $("cd_cap").value = s.company_discovery.candidates_cap;',
  '      $("person_personal_cap").value = s.person.personal_candidate_cap;',
  '      $("person_company_cap").value = s.person.company_candidate_cap;',
  '      $("person_accept").value = s.person.acceptance_threshold_for_persistence;',
  '      $("persist_airtable").checked = !!s.persistence.write_to_airtable;',
  '      $("preview").textContent = JSON.stringify(s, null, 2);',
  '    }',
  '    function clamp(id, min, max) {',
  '      const el = $(id); if (!el) return; const v = Number(el.value); if (!Number.isFinite(v)) { el.value = String(min); return; }',
  '      if (max != null && v > max) el.value = String(max); else if (min != null && v < min) el.value = String(min);',
  '    }',
  '    function attachValidation() {',
  '      ["sf_threshold","scr_weight_email","llm_temp_sf","llm_temp_person","llm_temp_company","person_accept"].forEach(id=>{',
  '        $(id)?.addEventListener("input", ()=> clamp(id, 0, 1));
      });',
  '      ["sf_max_pages","serper_cap","cd_cap","person_personal_cap","person_company_cap"].forEach(id=>{',
  '        $(id)?.addEventListener("input", ()=> { const el=$(id); const v=Number(el.value); if (!Number.isFinite(v) || v<1) el.value = "1"; });',
  '      });',
  '      ["sf_snippet"].forEach(id=>{ $(id)?.addEventListener("input", ()=> { const el=$(id); const v=Number(el.value); if (!Number.isFinite(v) || v<200) el.value = "200"; }); });',
  '      ["scr_bee_max"].forEach(id=>{ $(id)?.addEventListener("input", ()=> { const el=$(id); const v=Number(el.value); if (!Number.isFinite(v) || v<0) el.value = "0"; }); });',
  '    }',
  '    $("resetBtn").addEventListener("click", ()=>{ setValues(DEFAULTS); });',
  '    $("copyBtn").addEventListener("click", ()=>{',
  '      const s = currentValues();',
  '      $("preview").textContent = JSON.stringify(s, null, 2);',
  '      try { navigator.clipboard.writeText(JSON.stringify(s, null, 2)); } catch {}',
  '    });',
  '    document.addEventListener("input", ()=>{ const s = currentValues(); $("preview").textContent = JSON.stringify(s, null, 2); });',
  '    setValues(DEFAULTS); attachValidation();',
  '  </script>',
  '</body>',
  '</html>'
].join('\n');
}

router.get('/', (_req, res) => {
  const d = defaultsFromEnv();
  const html = renderPage(JSON.stringify(d));
  res.type('html').send(html);
});
