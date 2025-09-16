import { Router } from 'express';
import fs from 'fs';
import path from 'path';

export const router = Router();

function num(v: any, d: number) { const n = Number(v); return Number.isFinite(n) ? n : d; }
function bool(v: any, d = false) { const s = (v || '').toString().toLowerCase(); if (s === 'true') return true; if (s === 'false') return false; return d; }

router.get('/settings/defaults', (_req, res) => {
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
    // Derive from shared endings if present, prioritizing legal-ish first
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
    deterministic_rules: {
      exact_crn: true,
      jsonld_exact_name: true,
      exact_postcode: true
    },
    weights: {
      email_domain_match: 0.10
    }
  };

  // LLM
  const llm = {
    models: {
      'site-fetch': 'gpt-4o-mini',
      'person-linkedin': 'gpt-4o-mini',
      'company-discovery': 'gpt-4o-mini'
    },
    temperature: {
      'site-fetch': 0.1,
      'person-linkedin': 0.2,
      'company-discovery': 0.1
    }
  };

  // Serper
  const serper = {
    gl: 'uk',
    hl: 'en',
    per_query_result_cap: 10
  };

  // Company Discovery
  const company_discovery = {
    candidates_cap: 50
  };

  // Person LinkedIn
  const person = {
    personal_candidate_cap: num(process.env.PERSON_MAX_PERSONAL_CANDIDATES, 10),
    company_candidate_cap: num(process.env.PERSON_MAX_COMPANY_CANDIDATES, 6),
    acceptance_threshold_for_persistence: num(process.env.PERSON_LI_ACCEPT_THRESHOLD, 0.7)
  };

  // Persistence
  const persistence = {
    write_to_airtable: bool(process.env.WRITE_TO_AIRTABLE, false)
  };

  // Pricing placeholders (editable later)
  const pricing = {
    openai: {
      'gpt-4o-mini': { prompt_per_1k: 0, completion_per_1k: 0 }
    },
    serper: { per_call: 0 },
    scrapingbee: { per_call: 0 }
  };

  res.json({ sitefetch, scraping, llm, serper, company_discovery, person, persistence, pricing });
});

