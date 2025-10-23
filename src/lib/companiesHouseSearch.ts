import { logger } from './logger.js';
import { AddressInput, normalizePostcode } from './address.js';
import { chGetJson } from './companiesHouseClient.js';
import { logEvent } from './progress.js';
import { normalizeAddressFragments, NormalizedAddress } from './normalizedAddress.js';

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

function normalizedFromTarget(target: AddressInput): NormalizedAddress {
  // Exclude unit from lines (do not treat SAON as part of street lines)
  const lines = [target.buildingName, target.line1, target.line2, target.city].filter(Boolean) as string[];
  return normalizeAddressFragments({
    unit: target.unit,
    saon: target.unit,
    buildingName: target.buildingName,
    lines,
    town: target.city,
    postcode: target.postcode,
    countryCode: target.country || 'GB',
    fullAddress: lines.concat(target.postcode || '').filter(Boolean).join(', '),
    source: 'target',
  });
}

// Helper to split SAON/PAON from address_line_1 or premises
function splitSaonPaonLine(line1?: string, premises?: string): { unit?: string; line1?: string } {
  const s1 = String(line1 || '').trim();
  const pre = String(premises || '').trim();
  // If premises already looks like an SAON, prefer that
  const SAON = /(flat|apartment|apt|suite|unit|room|block|floor|fl|lvl|level)\s*(\d+[a-z]?)/i;
  const numToken = /\b\d+[a-z]?\b/i;
  if (pre && SAON.test(pre)) {
    return { unit: pre, line1: s1 || undefined };
  }
  if (!s1) return { unit: pre || undefined, line1: undefined };
  // Common CH pattern: "Flat 2, 9 Waterfront Mews" (optional comma after SAON)
  const m = s1.match(SAON);
  if (m) {
    let after = s1.slice(m.index! + m[0].length).trim();
    // Handle optional comma right after SAON, e.g., "Flat 2, 9 Waterfront Mews"
    if (after.startsWith(',')) {
      after = after.slice(1).trim();
    }
    const n = after.match(numToken);
    if (n) {
      const paonAndRest = after.slice(n.index!).trim(); // e.g., "9 Waterfront Mews"
      return { unit: m[0], line1: paonAndRest };
    }
  }
  // Fallback: if s1 starts with a number already, treat it as line1
  if (numToken.test(s1)) return { unit: pre || undefined, line1: s1 };
  return { unit: pre || undefined, line1: s1 };
}

function normalizedFromCandidate(candidate: any): NormalizedAddress {
  if (!candidate || typeof candidate !== 'object') {
    return normalizeAddressFragments({
      lines: [],
      countryCode: 'GB',
      source: 'candidate',
    });
  }

  // Use helper to split SAON/PAON from address_line_1 and premises
  const split = splitSaonPaonLine(candidate.address_line_1 || candidate.premises, candidate.premises);
  const unit = split.unit || candidate.saon || candidate.sub_building_name || candidate.po_box || candidate.care_of || '';
  const line1Norm = split.line1 || candidate.address_line_1 || candidate.premises || '';
  // Exclude unit from lines (do not treat SAON as part of street lines)
  const lines = [
    candidate.building_name || candidate.organisation_name || '',
    line1Norm,
    candidate.address_line_2 || candidate.address_line_3 || candidate.street_address || candidate.address_snippet || '',
    candidate.locality || candidate.town || candidate.post_town || '',
    candidate.region || candidate.county || '',
  ]
    .map((s: string) => (typeof s === 'string' ? s : ''))
    .filter(Boolean);

  const postcode = candidate.postal_code || candidate.postcode || candidate.post_code;
  const country =
    candidate.country ||
    candidate.country_of_residence ||
    (candidate.address && typeof candidate.address === 'object' ? candidate.address.country : undefined) ||
    'GB';
  const fullAddress = candidate.address_snippet || candidate.snippet || lines.join(', ');

  return normalizeAddressFragments({
    unit,
    saon: unit,
    buildingName: candidate.building_name || candidate.organisation_name,
    lines,
    town: candidate.locality || candidate.town || candidate.post_town || candidate.region,
    postcode,
    countryCode: country,
    fullAddress,
    source: 'companies_house',
  });
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
  canonical: {
    target: string;
    candidate: string;
    matched: boolean;
  };
  normalized: {
    target: NormalizedAddress;
    candidate: NormalizedAddress;
  };
};

function scoreAddressMatch(target: AddressInput, candidate: any): AddressMatchDetail {
  const targetNormalized = normalizedFromTarget(target);

  if (!candidate) {
    const candidateNormalized = normalizedFromCandidate(undefined);
    return {
      confidence: 0,
      matched: false,
      reasons: [],
      candidateAddress: {},
      targetTokens: [],
      candidateTokens: [],
      canonical: {
        target: targetNormalized.canonical_key,
        candidate: candidateNormalized.canonical_key,
        matched: false,
      },
      normalized: {
        target: targetNormalized,
        candidate: candidateNormalized,
      },
    };
  }

  const candidateNormalized = normalizedFromCandidate(candidate);

  const candidateAddress = {
    unit: candidateNormalized.saon || candidateNormalized.saon_identifier || '',
    buildingName: candidateNormalized.building_name || '',
    line1: candidate.address_line_1 || candidate.premises || candidate.care_of || '',
    line2: candidate.address_line_2 || candidate.address_line_3 || candidate.street_address || candidate.address_snippet || '',
    locality: candidate.locality || candidate.town || candidate.post_town || '',
    region: candidate.region || candidate.county || '',
    postcode: candidateNormalized.postcode || candidate.postal_code || candidate.postcode || candidate.post_code || '',
  };

  const targetTokenSet = buildTokenSet(target.unit, target.buildingName, target.line1, target.line2, target.city);
  targetNormalized.variants.forEach((variant) => tokenize(variant).forEach((token) => targetTokenSet.add(token)));
  if (targetNormalized.street_address) tokenize(targetNormalized.street_address).forEach((token) => targetTokenSet.add(token));
  if (targetNormalized.street_name) tokenize(targetNormalized.street_name).forEach((token) => targetTokenSet.add(token));

  const candidateTokenSet = buildTokenSet(
    candidateNormalized.saon,
    candidateNormalized.building_name,
    candidateNormalized.street_address,
    candidateNormalized.street_name,
    candidateNormalized.town,
    candidateNormalized.postcode,
    candidate.address_snippet,
    candidate.snippet,
    candidate.premises
  );
  candidateNormalized.variants.forEach((variant) => tokenize(variant).forEach((token) => candidateTokenSet.add(token)));

  const targetTokens = Array.from(targetTokenSet);
  const candidateTokens = Array.from(candidateTokenSet);

  const reasons: string[] = [];
  let confidence = 0;

  const targetPostcode = normalizePostcode(targetNormalized.postcode || target.postcode);
  const candidatePostcode = normalizePostcode(candidateNormalized.postcode || candidateAddress.postcode);

  const canonicalMatch =
    Boolean(targetNormalized.canonical_key) &&
    Boolean(candidateNormalized.canonical_key) &&
    targetNormalized.canonical_key === candidateNormalized.canonical_key;

  if (canonicalMatch) {
    confidence = Math.max(confidence, 0.95);
    reasons.push('canonical_match');
  }

  if (targetPostcode) {
    if (!candidatePostcode || candidatePostcode !== targetPostcode) {
      return {
        confidence: 0,
        matched: false,
        reasons: canonicalMatch ? ['canonical_match', 'postcode_mismatch'] : [],
        candidateAddress,
        targetTokens,
        candidateTokens,
        canonical: {
          target: targetNormalized.canonical_key,
          candidate: candidateNormalized.canonical_key,
          matched: canonicalMatch,
        },
        normalized: {
          target: targetNormalized,
          candidate: candidateNormalized,
        },
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
  } else if (targetTokens.length && !canonicalMatch) {
    return {
      confidence,
      matched: false,
      reasons,
      candidateAddress,
      targetTokens,
      candidateTokens,
      canonical: {
        target: targetNormalized.canonical_key,
        candidate: candidateNormalized.canonical_key,
        matched: canonicalMatch,
      },
      normalized: {
        target: targetNormalized,
        candidate: candidateNormalized,
      },
    };
  }

  const numericTokens = targetTokens.filter((token) => /\d/.test(token));
  if (numericTokens.length) {
    const allNumericPresent = numericTokens.every((token) => candidateTokens.includes(token));
    if (!allNumericPresent && !canonicalMatch) {
      return {
        confidence,
        matched: false,
        reasons,
        candidateAddress,
        targetTokens,
        candidateTokens,
        canonical: {
          target: targetNormalized.canonical_key,
          candidate: candidateNormalized.canonical_key,
          matched: canonicalMatch,
        },
        normalized: {
          target: targetNormalized,
          candidate: candidateNormalized,
        },
      };
    }
    if (allNumericPresent) {
      confidence += 0.1;
      reasons.push('number_match');
    }
  }

  if (candidateTokens.length) {
    const excessRatio = candidateTokens.filter((token) => !targetTokens.includes(token)).length / candidateTokens.length;
    if (excessRatio <= 0.4) {
      confidence += 0.1;
      reasons.push('candidate_tokens_similar');
    }
  }

  const targetTown = normalizeTown(targetNormalized.town || target.city);
  const candidateTown = normalizeTown(candidateNormalized.town || candidateAddress.locality || candidateAddress.region);
  if (targetTown && candidateTown && targetTown === candidateTown) {
    confidence += 0.1;
    reasons.push('town_match');
  }

  confidence = Math.min(confidence, 1);
  const matched =
    canonicalMatch ||
    (confidence >= 0.6 && tokenIntersection.length >= 2 && (!numericTokens.length || reasons.includes('number_match')));

  return {
    confidence,
    matched,
    reasons,
    candidateAddress,
    targetTokens,
    candidateTokens,
    canonical: {
      target: targetNormalized.canonical_key,
      candidate: candidateNormalized.canonical_key,
      matched: canonicalMatch,
    },
    normalized: {
      target: targetNormalized,
      candidate: candidateNormalized,
    },
  };
}

export type CompanyAddressHit = {
  companyId?: number | null;
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
  canonical?: {
    target: string;
    candidate: string;
    matched: boolean;
  };
  normalized?: {
    target: NormalizedAddress;
    candidate: NormalizedAddress;
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
  canonical?: {
    target: string;
    candidate: string;
    matched: boolean;
  };
  normalized?: {
    target: NormalizedAddress;
    candidate: NormalizedAddress;
  };
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
          canonical: score.canonical,
          normalized: score.normalized,
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
  if (detail.canonical?.matched) return true;
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
        canonical: score.canonical,
        normalized: score.normalized,
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
    companyId: hit.companyId ?? null,
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
        address: item.address || undefined,
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
