import { normalizePostcode } from './address.js';

export type AddressFragments = {
  fullAddress?: string;
  lines?: string[];
  unit?: string;
  saon?: string;
  saonIdentifier?: string;
  buildingName?: string;
  paon?: string;
  streetName?: string;
  streetAddress?: string;
  lad?: string;
  town?: string;
  locality?: string;
  city?: string;
  postcode?: string;
  countryCode?: string;
  country?: string;
  additionalDetail?: string;
  source?: string;
};

export type NormalizedAddress = {
  full_address: string;
  saon?: string;
  saon_identifier?: string;
  building_name?: string;
  paon?: string;
  street_name?: string;
  street_address?: string;
  lad?: string;
  town?: string;
  postcode?: string;
  country_code?: string;
  additional_detail?: string;
  canonical_key: string;
  variants: string[];
  source?: string;
};

const UK_POSTCODE_RE =
  /([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})/i;

const NUMBER_PREFIX_RE = /^(\d+[A-Z]?)(?:\s+(.+))?$/i;
const NUMBER_SUFFIX_RE = /^(.+?)\s+(\d+[A-Z]?)$/i;

const SAON_HINTS = [
  'flat',
  'apartment',
  'apt',
  'unit',
  'suite',
  'room',
  'studio',
  'floor',
  'level',
  'part',
];

function clean(value?: string | null): string {
  return (value || '').replace(/\s+/g, ' ').trim();
}

function explode(value?: string | null): string[] {
  const trimmed = clean(value);
  if (!trimmed) return [];
  return trimmed.split(/\s*,\s*/).map((part) => part.trim()).filter(Boolean);
}

function isLikelySaon(line: string): boolean {
  const lower = line.toLowerCase();
  return SAON_HINTS.some((hint) => lower.startsWith(`${hint} `) || lower.includes(` ${hint} `));
}

function pickSaonIdentifier(saon?: string): string | undefined {
  if (!saon) return undefined;
  const match = saon.match(/([A-Z]|\d+)(?!.*[A-Z0-9])/i);
  return match ? clean(match[1]).toUpperCase() : undefined;
}

function parsePaonAndStreet(line?: string): { paon?: string; street?: string } {
  const trimmed = clean(line);
  if (!trimmed) return {};
  const prefix = trimmed.match(NUMBER_PREFIX_RE);
  if (prefix) {
    return {
      paon: prefix[1].toUpperCase(),
      street: clean(prefix[2]),
    };
  }
  const suffix = trimmed.match(NUMBER_SUFFIX_RE);
  if (suffix) {
    return {
      paon: suffix[2].toUpperCase(),
      street: clean(suffix[1]),
    };
  }
  return { street: trimmed };
}

function extractPostcode(lines: string[], fallback?: string): string | undefined {
  for (const line of lines) {
    const match = line.match(UK_POSTCODE_RE);
    if (match) return clean(match[1].toUpperCase());
  }
  if (fallback) {
    const match = fallback.match(UK_POSTCODE_RE);
    if (match) return clean(match[1].toUpperCase());
  }
  return undefined;
}

function stripPostcode(lines: string[], postcode?: string): string[] {
  if (!postcode) return lines;
  return lines.map((line) => clean(line.replace(postcode, '')).replace(/\(\s*\)/g, '').trim()).filter(Boolean);
}

function removeBracketedPostcode(value: string): string {
  return value.replace(/\([^)]+\)/g, '').trim();
}

export function normalizeAddressFragments(fragments: AddressFragments): NormalizedAddress {
  const variants = new Set<string>();
  const lines: string[] = [
    ...((fragments.lines || []).map(clean)),
    clean(fragments.unit || fragments.saon),
    clean(fragments.buildingName),
    clean(fragments.streetAddress),
    clean(fragments.streetName),
    clean(fragments.town || fragments.city || fragments.locality),
  ]
    .filter(Boolean)
    .map((line) => removeBracketedPostcode(line))
    .filter(Boolean);

  const fullAddressParts: string[] = [];
  if (fragments.fullAddress) {
    fullAddressParts.push(removeBracketedPostcode(fragments.fullAddress));
  }

  const exploded = fragments.fullAddress ? explode(fragments.fullAddress) : [];
  for (const line of exploded) lines.push(removeBracketedPostcode(line));

  const postcode = extractPostcode([...exploded, ...lines], fragments.postcode);
  const countryCode = fragments.countryCode || fragments.country || 'GB';

  const cleanLines = stripPostcode(Array.from(new Set(lines)), postcode);

  let saon = clean(fragments.saon || fragments.unit) || undefined;
  let saonIdentifier = fragments.saonIdentifier;
  let buildingName = clean(fragments.buildingName) || undefined;
  let paon = clean(fragments.paon) || undefined;
  let streetName = clean(fragments.streetName) || undefined;
  let streetAddress = clean(fragments.streetAddress) || undefined;
  let town = clean(fragments.town || fragments.city || fragments.locality) || undefined;
  let lad = clean(fragments.lad) || undefined;
  let additionalDetail = clean(fragments.additionalDetail) || undefined;

  const baseLines = cleanLines.filter(Boolean);
  const remaining: string[] = [];

  for (const line of baseLines) {
    if (!saon && isLikelySaon(line)) {
      saon = line;
      continue;
    }
    if (!buildingName && !/\d/.test(line) && line.split(' ').length <= 4) {
      buildingName = line;
      continue;
    }
    remaining.push(line);
  }

  if (!streetAddress && remaining.length) {
    streetAddress = remaining[0];
  }

  if (!streetName && streetAddress) {
    const parsed = parsePaonAndStreet(streetAddress);
    paon = paon || parsed.paon || paon;
    streetName = parsed.street || streetName;
  }

  if (!paon && remaining.length) {
    for (const line of remaining) {
      const parsed = parsePaonAndStreet(line);
      if (parsed.paon) {
        paon = parsed.paon;
        if (!streetName) streetName = parsed.street;
        break;
      }
    }
  }

  if (!town) {
    const last = remaining[remaining.length - 1];
    if (last && last !== streetAddress) {
      town = last;
    }
  }

  saonIdentifier = saonIdentifier || pickSaonIdentifier(saon);

  const canonical = [
    clean(saon ? saon.toLowerCase() : ''),
    clean(paon ? paon.toLowerCase() : ''),
    clean(streetName ? streetName.toLowerCase() : ''),
    clean(town ? town.toLowerCase() : ''),
    normalizePostcode(postcode)?.toLowerCase() || '',
  ]
    .filter(Boolean)
    .join('|');

  const fullAddressCandidates = [
    fragments.fullAddress,
    [saon, buildingName, streetAddress].filter(Boolean).join(', '),
    [buildingName, streetAddress].filter(Boolean).join(', '),
    [streetAddress, town, postcode].filter(Boolean).join(', '),
  ].map(clean);

  for (const candidate of fullAddressCandidates) {
    if (candidate) variants.add(candidate);
  }
  if (postcode) variants.add(postcode);
  if (streetAddress) variants.add(streetAddress);

  const full_address = Array.from(variants).find(Boolean) || '';

  return {
    full_address,
    saon: saon || undefined,
    saon_identifier: saonIdentifier,
    building_name: buildingName || undefined,
    paon: paon || undefined,
    street_name: streetName || undefined,
    street_address: streetAddress || undefined,
    lad: lad || undefined,
    town: town || undefined,
    postcode: postcode || undefined,
    country_code: countryCode ? countryCode.toUpperCase() : undefined,
    additional_detail: additionalDetail || undefined,
    canonical_key: canonical,
    variants: Array.from(variants),
    source: fragments.source,
  };
}

export function normalizeFreeformAddress(value: string, opts: { source?: string; defaultCountry?: string } = {}): NormalizedAddress {
  const cleaned = clean(value);
  const lines = explode(cleaned);
  const postcode = extractPostcode(lines, cleaned);
  const fragments: AddressFragments = {
    fullAddress: cleaned,
    lines,
    postcode: postcode,
    countryCode: opts.defaultCountry || 'GB',
    source: opts.source,
  };
  // Additional detail detection – phrases starting with LAND etc
  if (/^land\b/i.test(cleaned)) {
    fragments.additionalDetail = cleaned;
  }
  return normalizeAddressFragments(fragments);
}
