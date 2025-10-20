import { logger } from './logger.js';
import { AddressInput, normalizePostcode } from './address.js';
import { chGetJson } from './companiesHouseClient.js';
import { logEvent } from './progress.js';

const HAS_CH_API_KEY = Boolean((process.env.CH_API_KEY || '').trim());

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
  const stop = new Set(['ltd', 'limited', 'uk', 'the']);
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
    unit?: string;
    buildingName?: string;
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
    unit: candidate.saon || candidate.sub_building_name || candidate.po_box || candidate.care_of || '',
    buildingName: candidate.building_name || candidate.premises || candidate.organisation_name || '',
    line1: candidate.address_line_1 || candidate.premises || candidate.care_of || '',
    line2: candidate.address_line_2 || candidate.address_line_3 || candidate.street_address || candidate.address_snippet || '',
    locality: candidate.locality || candidate.town || candidate.post_town || '',
    region: candidate.region || candidate.county || '',
    postcode: candidate.postal_code || candidate.postcode || candidate.post_code || '',
  };

  const targetTokens = Array.from(
    buildTokenSet(target.unit, target.buildingName, target.line1, target.line2, target.city)
  );
  const candidateTokens = Array.from(
    buildTokenSet(
      candidateAddress.unit,
      candidateAddress.buildingName,
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
    unit?: string;
    buildingName?: string;
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
  firstName?: string;
  middleName?: string;
  lastName?: string;
  officerId?: string;
  address?: any;
  addresses?: any[];
  matchConfidence: number;
  matchReasons: string[];
  matched: boolean;
  raw: any;
  officerRaw?: any;
  linksSelf?: string;
  officerAppointmentsUrl?: string;
  officerApiPath?: string;
};

export async function searchCompaniesByAddress(jobId: string, address: AddressInput): Promise<CompanyAddressHit[]> {
  if (!HAS_CH_API_KEY) {
    logger.debug('Skipping CH company search; no API key');
    return [];
  }
  const queryParts = [address.unit, address.buildingName, address.line1, address.line2, address.city, address.postcode].filter((part) => typeof part === 'string' && part.trim().length);
  const q = encodeURIComponent(queryParts.join(' ').trim());
  const endpoint = `/search/companies?q=${q}&items_per_page=50`;
  try {
    const json = await chGetJson<any>(endpoint, { retries: 2 });
    const items = Array.isArray(json?.items) ? json.items : [];

    // await logEvent(jobId, 'info', 'Search CH Companies by address', {
    //   address,
    //   itemsCount: items.length,
    //   items
    // });

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

function extractOfficerIdFromLink(selfLink?: string): string | undefined {
  // examples: "/officers/{id}/appointments" or "/officers/{id}"
  if (!selfLink) return undefined;
  const parts = selfLink.split('/').filter(Boolean);
  const idx = parts.indexOf('officers');
  if (idx >= 0 && parts[idx + 1]) return parts[idx + 1];
  return undefined;
}

function extractOfficerNameParts(off: any): { firstName?: string; middleName?: string; lastName?: string } {
  if (!off) return {};
  const ne = off.name_elements || off.nameElements || off.name_elements_json;
  if (ne) {
    const first = ne.forename || ne.given_name || ne.givenName || ne.forenames || undefined;
    const middle = ne.other_forenames || ne.middle_name || undefined;
    const last = ne.surname || undefined;
    return { firstName: first || undefined, middleName: middle || undefined, lastName: last || undefined };
  }
  // fallback: split full name if needed
  const raw = (off.name || '').trim();
  if (!raw) return {};
  const parts = raw.replace(/\s+/g, ' ').split(' ');
  if (parts.length === 1) return { firstName: parts[0] };
  if (parts.length === 2) return { firstName: parts[0], lastName: parts[1] };
  return { firstName: parts[0], middleName: parts.slice(1, -1).join(' ') || undefined, lastName: parts[parts.length - 1] };
}

function isDeterministicAddressMatch(detail: ReturnType<typeof scoreAddressMatch>): boolean {
  // Require postcode and number match, plus decent token coverage
  const hasPostcode = detail.reasons.includes('postcode_match');
  const hasNumber = detail.reasons.includes('number_match');
  const hasTokens = detail.reasons.includes('address_token_match') || detail.confidence >= 0.8;
  return detail.matched && hasPostcode && hasNumber && hasTokens;
}

type PrelimHit = OfficerAddressHit & { __detail: AddressMatchDetail };

export async function searchOfficersByAddress(jobId: string, address: AddressInput): Promise<OfficerAddressHit[]> {
  if (!HAS_CH_API_KEY) {
    logger.debug('Skipping CH officer search; no API key');
    return [];
  }
  const queryParts = [address.unit, address.buildingName, address.line1, address.line2, address.city, address.postcode].filter((part) => typeof part === 'string' && part.trim().length);
  const q = encodeURIComponent(queryParts.join(' ').trim());
  const endpoint = `/search/officers?q=${q}&items_per_page=50`;
  try {
    const json = await chGetJson<any>(endpoint, { retries: 2 });
    const items = Array.isArray(json?.items) ? json.items : [];

    // await logEvent(jobId, 'info', 'Search CH officers by address', {
    //   address,
    //   itemsCount: items.length,
    //   items
    // });

    // First pass: score deterministically by the address we were given
    const prelim: PrelimHit[] = items.map((item: any): PrelimHit => {
      const score = scoreAddressMatch(address, item?.address);
      const officerId = extractOfficerIdFromLink(item?.links?.self || item?.links?.officer?.self);
      const linksSelf: string | undefined = item?.links?.self || item?.links?.officer?.self;
      const officerApiPath = officerId ? `/officers/${officerId}/appointments` : undefined;
      const officerAppointmentsUrl = officerApiPath ? `${(process.env.CH_API_BASE || 'https://api.company-information.service.gov.uk').replace(/\/$/, '')}${officerApiPath}` : undefined;
      return {
        name: item.title || item.name,
        officerId,
        address: item.address,
        matchConfidence: score.confidence,
        matchReasons: score.reasons,
        matched: score.matched,
        raw: item,
        linksSelf,
        officerApiPath,
        officerAppointmentsUrl,
        __detail: score,
      } as PrelimHit;
    });

    // Filter to strong, deterministic matches
    const strong: PrelimHit[] = prelim.filter((p: PrelimHit) => isDeterministicAddressMatch(p.__detail));

    // Enrich strong matches using the officer appointments endpoint to get actual CH name parts + addresses
    const enrichedStrong = await Promise.all(
      strong.map(async (p: PrelimHit) => {
        let officerRaw: any = null;
        let firstName: string | undefined;
        let middleName: string | undefined;
        let lastName: string | undefined;
        let addresses: any[] | undefined;
        let officerApiPath: string | undefined = p.officerApiPath;
        let officerAppointmentsUrl: string | undefined = p.officerAppointmentsUrl;

        try {
          // Use links.self if present and matches /officers/{id}/appointments, else fallback to officerId path
          const linksSelf: string | undefined = p.linksSelf;
          const apiPath = linksSelf && /\/officers\/.+\/appointments$/.test(linksSelf)
            ? linksSelf
            : (p.officerId ? `/officers/${p.officerId}/appointments` : undefined);
          if (apiPath) {
            officerRaw = await chGetJson<any>(apiPath, { retries: 2 });
            officerApiPath = apiPath;
            officerAppointmentsUrl = `${(process.env.CH_API_BASE || 'https://api.company-information.service.gov.uk').replace(/\/$/, '')}${apiPath}`;
            const items = Array.isArray(officerRaw?.items) ? officerRaw.items : [];

            // Score each appointment's address against the target address (fully typed)
            type AppointmentScore = { it: any; addr: any; detail: AddressMatchDetail };

            const scoredAppointments: AppointmentScore[] = items.map((it: any): AppointmentScore => {
              const addr = it?.address;
              const detail: AddressMatchDetail = scoreAddressMatch(address, addr);
              return { it, addr, detail };
            });

            // Prefer deterministic address matches; otherwise take the highest confidence
            const deterministic: AppointmentScore[] = scoredAppointments.filter((s: AppointmentScore) =>
              isDeterministicAddressMatch(s.detail)
            );

            const best: AppointmentScore | undefined = (deterministic.length ? deterministic : scoredAppointments)
              .sort((a: AppointmentScore, b: AppointmentScore) => b.detail.confidence - a.detail.confidence)[0];

            const chosen = best?.it || items[0];
            if (chosen?.name_elements) {
              // Use the actual CH-provided name elements (ground truth)
              const ne = chosen.name_elements;
              firstName = ne.forename || undefined;
              middleName = ne.other_forenames || undefined;
              lastName = ne.surname || undefined;
            } else if (chosen) {
              // Fallback to generic extractor if name_elements absent
              const parts = extractOfficerNameParts(chosen);
              firstName = parts.firstName;
              middleName = parts.middleName;
              lastName = parts.lastName;
            }

            // Collect addresses from all appointments (deduped)
            const addrCandidates: any[] = [];
            for (const it of items) {
              if (it?.address) addrCandidates.push(it.address);
            }
            const seen = new Set<string>();
            addresses = addrCandidates.filter((a) => {
              try { const sig = JSON.stringify(a || {}); if (seen.has(sig)) return false; seen.add(sig); return true; } catch { return true; }
            });
          }
        } catch (e) {
          logger.debug({ officerId: p.officerId, err: String(e) }, 'Officer appointments fetch failed; continuing with search payload');
        }
        const { __detail, ...rest } = p as any;
        return {
          ...rest,
          firstName,
          middleName,
          lastName,
          addresses,
          officerRaw,
          officerApiPath,
          officerAppointmentsUrl,
        } as OfficerAddressHit;
      })
    );

    // Combine enriched strong matches with any other (non-strong) matches if you still want them
    // Here we return only strong, enriched matches to keep semantics tight
    const scored = enrichedStrong
      .filter((hit: OfficerAddressHit): hit is OfficerAddressHit => hit.matched && hit.matchConfidence >= 0.6)
      .sort((a: OfficerAddressHit, b: OfficerAddressHit) => b.matchConfidence - a.matchConfidence);

    return scored;
  } catch (err) {
    logger.warn({ q, err: String(err) }, 'Companies House officer search by address failed');
    return [];
  }
}

export async function fetchCompany(companyNumber: string): Promise<any> {
  if (!HAS_CH_API_KEY) throw new Error('Missing CH_API_KEY');
  return chGetJson<any>(`/company/${companyNumber}`, { retries: 2 });
}

export async function fetchOfficerAppointments(officerId: string): Promise<any> {
  if (!HAS_CH_API_KEY) throw new Error('Missing CH_API_KEY');
  return chGetJson<any>(`/officers/${officerId}/appointments`, { retries: 2 });
}

export function summarizeOfficer(hit: OfficerAddressHit) {
  return {
    name: hit.name,
    firstName: hit.firstName,
    middleName: hit.middleName,
    lastName: hit.lastName,
    officerId: hit.officerId,
    matched: hit.matched,
    matchConfidence: hit.matchConfidence,
    matchReasons: hit.matchReasons,
    address: hit.address,
    addresses: Array.isArray(hit.addresses) ? hit.addresses.slice(0, 5) : undefined,
    linksSelf: hit.linksSelf,
    officerAppointmentsUrl: hit.officerAppointmentsUrl,
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
  address?: string;
  appointedOn?: string;
  resignedOn?: string;
  officerRole?: string;
  officerId?: string;
  raw?: any;
};

export type CompanyPscRecord = {
  name: string;
  address?: string;
  ceasedOn?: string;
  natureOfControl?: string[];
  raw?: any;
};

export async function listCompanyDirectors(companyNumber: string): Promise<CompanyOfficerRecord[]> {
  if (!HAS_CH_API_KEY) {
    logger.debug({ companyNumber }, 'Skipping CH directors list; no API key');
    return [];
  }
  try {
    const json = await chGetJson<any>(`/company/${companyNumber}/officers?items_per_page=100`, {
      retries: 2,
    });
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
  if (!HAS_CH_API_KEY) {
    logger.debug({ companyNumber }, 'Skipping CH PSC list; no API key');
    return [];
  }
  try {
    const json = await chGetJson<any>(
      `/company/${companyNumber}/persons-with-significant-control`,
      { retries: 2 }
    );
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
