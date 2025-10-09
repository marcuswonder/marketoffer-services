import { Buffer } from 'node:buffer';
import { logger } from './logger.js';
import { httpGetJson } from './http.js';
import { AddressInput, normalizePostcode } from './address.js';

const CH_BASE = process.env.CH_API_BASE || 'https://api.company-information.service.gov.uk';
const CH_KEY = process.env.CH_API_KEY || '';

function chHeaders() {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  const auth = Buffer.from(`${CH_KEY}:`).toString('base64');
  return { Authorization: `Basic ${auth}` };
}

function tokenize(value: string): string[] {
  if (!value) return [];
  const cleaned = value
    .toLowerCase()
    .replace(/[\u2013\u2014\-\/]/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return [];
  const tokens = cleaned.split(' ').filter(Boolean);
  const stop = new Set(['ltd', 'limited', 'uk', 'unit', 'the']);
  return Array.from(new Set(tokens.filter((token) => !stop.has(token))));
}

function buildTokenSet(...parts: Array<string | null | undefined>): Set<string> {
  const tokens = new Set<string>();
  for (const part of parts) {
    tokenize(String(part || '')).forEach((token) => tokens.add(token));
  }
  return tokens;
}

function normalizeTown(value?: string | null): string {
  return (value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

type AddressMatchDetail = {
  confidence: number;
  matched: boolean;
  reasons: string[];
  candidateAddress: {
    line1?: string;
    line2?: string;
    locality?: string;
    region?: string;
    postcode?: string;
  };
  targetTokens: string[];
  candidateTokens: string[];
};

function scoreAddressMatch(target: AddressInput, candidate: any): AddressMatchDetail {
  if (!candidate) {
    return {
      confidence: 0,
      matched: false,
      reasons: [],
      candidateAddress: {},
      targetTokens: [],
      candidateTokens: [],
    };
  }

  const candidateAddress = {
    line1: candidate.address_line_1 || candidate.premises || candidate.care_of || '',
    line2: candidate.address_line_2 || candidate.address_line_3 || candidate.street_address || candidate.address_snippet || '',
    locality: candidate.locality || candidate.town || candidate.post_town || '',
    region: candidate.region || candidate.county || '',
    postcode: candidate.postal_code || candidate.postcode || candidate.post_code || '',
  };

  const targetTokens = Array.from(
    buildTokenSet(target.line1, target.line2, target.city)
  );
  const candidateTokens = Array.from(
    buildTokenSet(
      candidateAddress.line1,
      candidateAddress.line2,
      candidateAddress.locality,
      candidateAddress.region,
      candidate.address_snippet,
      candidate.snippet,
      candidate.premises
    )
  );

  const reasons: string[] = [];
  let confidence = 0;

  const targetPostcode = normalizePostcode(target.postcode);
  const candidatePostcode = normalizePostcode(candidateAddress.postcode);
  if (targetPostcode) {
    if (!candidatePostcode || candidatePostcode !== targetPostcode) {
      return {
        confidence: 0,
        matched: false,
        reasons: [],
        candidateAddress,
        targetTokens,
        candidateTokens,
      };
    }
    confidence += 0.35;
    reasons.push('postcode_match');
  }

  const tokenIntersection = targetTokens.filter((token) => candidateTokens.includes(token));
  const tokenCoverage = targetTokens.length ? tokenIntersection.length / targetTokens.length : 0;
  if (tokenCoverage >= 0.75 && tokenIntersection.length >= Math.min(targetTokens.length, 3)) {
    confidence += 0.45;
    reasons.push('address_token_match');
  } else if (tokenCoverage >= 0.5 && tokenIntersection.length >= 2) {
    confidence += 0.25;
    reasons.push('partial_address_token_match');
  } else if (targetTokens.length) {
    return {
      confidence,
      matched: false,
      reasons,
      candidateAddress,
      targetTokens,
      candidateTokens,
    };
  }

  const numericTokens = targetTokens.filter((token) => /\d/.test(token));
  if (numericTokens.length) {
    const allNumericPresent = numericTokens.every((token) => candidateTokens.includes(token));
    if (!allNumericPresent) {
      return {
        confidence,
        matched: false,
        reasons,
        candidateAddress,
        targetTokens,
        candidateTokens,
      };
    }
    confidence += 0.1;
    reasons.push('number_match');
  }

  if (candidateTokens.length) {
    const excessRatio = candidateTokens.filter((token) => !targetTokens.includes(token)).length / candidateTokens.length;
    if (excessRatio <= 0.4) {
      confidence += 0.1;
      reasons.push('candidate_tokens_similar');
    }
  }

  const targetTown = normalizeTown(target.city);
  const candidateTown = normalizeTown(candidateAddress.locality || candidateAddress.region);
  if (targetTown && candidateTown && targetTown === candidateTown) {
    confidence += 0.1;
    reasons.push('town_match');
  }

  confidence = Math.min(confidence, 1);
  const matched = confidence >= 0.6 && tokenIntersection.length >= 2;

  return {
    confidence,
    matched,
    reasons,
    candidateAddress,
    targetTokens,
    candidateTokens,
  };
}

export type CompanyAddressHit = {
  companyNumber: string;
  companyName: string;
  status?: string;
  matchConfidence: number;
  matchReasons: string[];
  matched: boolean;
  candidateAddress: {
    line1?: string;
    line2?: string;
    locality?: string;
    region?: string;
    postcode?: string;
  };
  raw: any;
};

export type OfficerAddressHit = {
  name: string;
  officerId?: string;
  address?: any;
  matchConfidence: number;
  matchReasons: string[];
  matched: boolean;
  raw: any;
};

export async function searchCompaniesByAddress(address: AddressInput): Promise<CompanyAddressHit[]> {
  if (!CH_KEY) {
    logger.debug('Skipping CH company search; no API key');
    return [];
  }
  const q = encodeURIComponent(`${address.line1} ${address.postcode}`.trim());
  const url = `${CH_BASE}/search/companies?q=${q}&items_per_page=50`;
  try {
    const json = await httpGetJson<any>(url, { headers: chHeaders(), retries: 2 });
    const items = Array.isArray(json?.items) ? json.items : [];
    const scored: CompanyAddressHit[] = items
      .map((item: any): CompanyAddressHit => {
        const candidateAddr = item?.address || item?.registered_office_address;
        const score = scoreAddressMatch(address, candidateAddr);
        return {
          companyNumber: item.company_number,
          companyName: item.title || item.company_name,
          status: item.company_status,
          matchConfidence: score.confidence,
          matchReasons: score.reasons,
          matched: score.matched,
          candidateAddress: score.candidateAddress,
          raw: item,
        };
      })
      .filter((hit: CompanyAddressHit): hit is CompanyAddressHit => hit.matched && hit.matchConfidence >= 0.6)
      .sort((a: CompanyAddressHit, b: CompanyAddressHit) => b.matchConfidence - a.matchConfidence);
    return scored;
  } catch (err) {
    logger.warn({ q, err: String(err) }, 'Companies House search by address failed');
    return [];
  }
}

export async function searchOfficersByAddress(address: AddressInput): Promise<OfficerAddressHit[]> {
  if (!CH_KEY) {
    logger.debug('Skipping CH officer search; no API key');
    return [];
  }
  const q = encodeURIComponent(`${address.line1} ${address.postcode}`.trim());
  const url = `${CH_BASE}/search/officers?q=${q}&items_per_page=50`;
  try {
    const json = await httpGetJson<any>(url, { headers: chHeaders(), retries: 2 });
    const items = Array.isArray(json?.items) ? json.items : [];
    const scored: OfficerAddressHit[] = items
      .map((item: any): OfficerAddressHit => {
        const score = scoreAddressMatch(address, item?.address);
        return {
          name: item.title || item.name,
          officerId: item.links?.officer?.self?.split('/').pop(),
          address: item.address,
          matchConfidence: score.confidence,
          matchReasons: score.reasons,
          matched: score.matched,
          raw: item,
        };
      })
      .filter((hit: OfficerAddressHit): hit is OfficerAddressHit => hit.matched && hit.matchConfidence >= 0.6)
      .sort((a: OfficerAddressHit, b: OfficerAddressHit) => b.matchConfidence - a.matchConfidence);
    return scored;
  } catch (err) {
    logger.warn({ q, err: String(err) }, 'Companies House officer search by address failed');
    return [];
  }
}

export async function fetchCompany(companyNumber: string): Promise<any> {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  const url = `${CH_BASE}/company/${companyNumber}`;
  return httpGetJson<any>(url, { headers: chHeaders(), retries: 2 });
}

export async function fetchOfficerAppointments(officerId: string): Promise<any> {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  const url = `${CH_BASE}/officers/${officerId}/appointments`; // default page size 20
  return httpGetJson<any>(url, { headers: chHeaders(), retries: 2 });
}

export function summarizeOfficer(officer: OfficerAddressHit) {
  return {
    name: officer.name,
    officerId: officer.officerId,
    address: officer.address,
    matched: officer.matched,
    matchConfidence: Number(officer.matchConfidence?.toFixed?.(3) ?? officer.matchConfidence ?? 0),
    matchReasons: officer.matchReasons,
  };
}

export function summarizeCompany(hit: CompanyAddressHit) {
  return {
    companyNumber: hit.companyNumber,
    companyName: hit.companyName,
    status: hit.status,
    matched: hit.matched,
    matchConfidence: Number(hit.matchConfidence?.toFixed?.(3) ?? hit.matchConfidence ?? 0),
    matchReasons: hit.matchReasons,
    candidateAddress: hit.candidateAddress,
  };
}

export type CompanyOfficerRecord = {
  name: string;
  appointedOn?: string;
  resignedOn?: string;
  officerRole?: string;
  officerId?: string;
  raw?: any;
};

export type CompanyPscRecord = {
  name: string;
  ceasedOn?: string;
  natureOfControl?: string[];
  raw?: any;
};

export async function listCompanyDirectors(companyNumber: string): Promise<CompanyOfficerRecord[]> {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  const url = `${CH_BASE}/company/${companyNumber}/officers?items_per_page=100`;
  try {
    const json = await httpGetJson<any>(url, { headers: chHeaders(), retries: 2 });
    const items = Array.isArray(json?.items) ? json.items : [];
    return items
      .filter((item: any) => {
        const role = (item?.officer_role || '').toLowerCase();
        if (!role.includes('director')) return false;
        if (item.resigned_on) return false;
        return Boolean(item?.name);
      })
      .map((item: any) => ({
        name: item.name,
        appointedOn: item.appointed_on || undefined,
        officerRole: item.officer_role || undefined,
        officerId: item.links?.officer?.appointments?.split('/').pop(),
        raw: item,
      }));
  } catch (err) {
    logger.warn({ companyNumber, err: String(err) }, 'Failed to list company directors');
    return [];
  }
}

export async function listCompanyPscs(companyNumber: string): Promise<CompanyPscRecord[]> {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  const url = `${CH_BASE}/company/${companyNumber}/persons-with-significant-control`;
  try {
    const json = await httpGetJson<any>(url, { headers: chHeaders(), retries: 2 });
    const items = Array.isArray(json?.items) ? json.items : [];
    return items
      .map((item: any) => {
        let name = item?.name;
        if (!name && item?.name_elements) {
          const ne = item.name_elements;
          name = [ne.forename, ne.middle_name, ne.surname]
            .filter(Boolean)
            .join(' ');
        }
        if (!name) return null;
        return {
          name,
          ceasedOn: item.ceased_on || undefined,
          natureOfControl: Array.isArray(item.natures_of_control) ? item.natures_of_control : [],
          raw: item,
        } as CompanyPscRecord;
      })
      .filter(Boolean) as CompanyPscRecord[];
  } catch (err) {
    // PSC endpoint often 404s for small companies; treat as empty
    if (typeof err === 'object' && err && 'status' in (err as any) && (err as any).status === 404) {
      return [];
    }
    logger.warn({ companyNumber, err: String(err) }, 'Failed to list company PSCs');
    return [];
  }
}
