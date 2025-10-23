import fs from 'fs';
import path from 'path';
import { fetch } from 'undici';
import { logger } from './logger.js';
import { AddressInput, prettyAddress, normalizePostcode, addressVariants } from './address.js';

export type OccupantCompanyRelation = {
  role: 'director' | 'psc';
  companyNumber?: string;
  companyName?: string;
  officerId?: string | null;
  appointedOn?: string | null;
  ceasedOn?: string | null;
  source?: string;
};

export type OccupantRecord = {
  firstName: string;
  lastName: string;
  fullName: string;
  ageBand?: string;
  birthYear?: number;
  firstSeenYear?: number;
  lastSeenYear?: number;
  dataSources: string[];
  indicators?: string[];
  companyRelations?: OccupantCompanyRelation[];
};

export type OpenRegisterResult = {
  occupants: OccupantRecord[];
  raw?: any;
  source: string;
};

function dedupe<T>(items: T[], keyFn: (item: T) => string): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const item of items) {
    const key = keyFn(item);
    if (!key) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function mergeName(first: string, last: string): string {
  return `${first || ''} ${last || ''}`.trim();
}

function normalizeFragment(value: any): string {
  if (value == null) return '';
  return String(value)
    .toLowerCase()
    .replace(/[\u2013\u2014\-\/]/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function collectTokens(values: any[]): Set<string> {
  const tokens = new Set<string>();
  for (const value of values) {
    const normalized = normalizeFragment(value);
    if (!normalized) continue;
    normalized.split(' ').forEach((token) => {
      if (token) tokens.add(token);
    });
  }
  return tokens;
}

type AddressVariantDetail = {
  raw: string;
  tokens: Set<string>;
  numericTokens: string[];
};

function buildVariantDetails(address: AddressInput): AddressVariantDetail[] {
  return addressVariants(address)
    .map((raw) => {
      const tokenList = Array.from(collectTokens([raw]));
      if (!tokenList.length) return null;
      const tokens = new Set(tokenList);
      const numericTokens = tokenList.filter((token) => /\d/.test(token));
      return { raw, tokens, numericTokens };
    })
    .filter((detail): detail is AddressVariantDetail => !!detail);
}

type PremisesMatch = {
  item: any;
  score: number;
  sharedTokens: number;
  targetVariant: string;
  candidateTokens: string[];
  candidatePostcode?: string | null;
};

function choosePremisesMatch(
  variants: AddressVariantDetail[],
  premisesList: any[],
  targetPostcode: string | null
): PremisesMatch | null {
  if (!premisesList.length || !variants.length) return null;
  let best: PremisesMatch | null = null;
  for (const item of premisesList) {
    if (!item || typeof item !== 'object') continue;
    const candidateStrings: any[] = [
      item.full_address,
      item.address,
      item.premises,
      item.premise,
      item.premises_name,
      item.building_name,
      item.sub_building_name,
      item.secondary_addressable_object_name,
      item.secondary_addressable_object,
      item.primary_addressable_object_name,
      item.primary_addressable_object,
      item.saon,
      item.paon,
      item.street,
      item.thoroughfare,
      item.locality,
      item.town,
      item.post_town,
    ];
    const candidateTokens = collectTokens(candidateStrings);
    if (!candidateTokens.size) continue;
    const candidatePostcode = normalizePostcode(item.postcode || item.post_code || item.post_code_in || item.postcode_in);
    const postcodeMatches = Boolean(
      targetPostcode &&
        candidatePostcode &&
        candidatePostcode === targetPostcode
    );
    const postcodeMismatch = Boolean(
      targetPostcode &&
        candidatePostcode &&
        candidatePostcode !== targetPostcode
    );
    for (const variant of variants) {
      if (variant.numericTokens.length) {
        const numericOk = variant.numericTokens.every((token) => candidateTokens.has(token));
        if (!numericOk) continue;
      }
      let shared = 0;
      variant.tokens.forEach((token) => {
        if (candidateTokens.has(token)) shared += 1;
      });
      if (!shared) continue;
      const missing = variant.tokens.size - shared;
      const extra = candidateTokens.size - shared;
      let score = shared * 10 - missing * 5 - extra;
      if (postcodeMatches) score += 8;
      if (postcodeMismatch) score -= 10;
      if (!best || score > best.score) {
        best = {
          item,
          score,
          sharedTokens: shared,
          targetVariant: variant.raw,
          candidateTokens: Array.from(candidateTokens),
          candidatePostcode,
        };
      }
    }
  }
  return best;
}

function matchesAddressTokens(
  fields: any[],
  candidatePostcode: string | null,
  targetTokens: Set<string>,
  targetNumericTokens: string[],
  targetPostcode: string | null
): boolean {
  if (!targetTokens.size) return true;
  const candidateTokens = collectTokens(fields);
  if (!candidateTokens.size) return true;
  if (targetPostcode && candidatePostcode && targetPostcode !== candidatePostcode) return false;
  if (targetNumericTokens.length) {
    for (const token of targetNumericTokens) {
      if (!candidateTokens.has(token)) return false;
    }
  }
  let shared = 0;
  targetTokens.forEach((token) => {
    if (candidateTokens.has(token)) shared += 1;
  });
  if (shared >= Math.min(targetTokens.size, 3)) return true;
  const coverage = shared / targetTokens.size;
  return shared >= 2 && coverage >= 0.5;
}

export async function lookupOpenRegister(address: AddressInput): Promise<OpenRegisterResult | null> {
  const pretty = prettyAddress(address);
  const variantDetails = buildVariantDetails(address);
  const targetTokens = new Set<string>();
  const targetNumericTokens = new Set<string>();
  for (const detail of variantDetails) {
    detail.tokens.forEach((token) => targetTokens.add(token));
    detail.numericTokens.forEach((token) => targetNumericTokens.add(token));
  }
  const targetNumericTokenList = Array.from(targetNumericTokens);
  const targetPostcode = normalizePostcode(address.postcode);

  const apiKey = process.env.T2A_API_KEY;
  const apiBase = process.env.T2A_BASE_URL || 'https://api.t2a.io/rest/rest.aspx';
  if (apiKey) {
    try {
      const premisesParts = [address.unit, address.buildingName, address.line1].map(s => (s || '').trim()).filter(Boolean);
      const premises = premisesParts.join(' ');
      const street = (address.line2 || '').trim();
      const url = new URL(apiBase);
      url.searchParams.set('method', 'address_person');
      url.searchParams.set('api_key', apiKey);
      if (premises) {
        url.searchParams.set('premises', premises);
      } else if (street) {
        url.searchParams.set('premises', street);
      }
      if (street) url.searchParams.set('street', street);
      if (address.postcode) url.searchParams.set('postcode', address.postcode.trim());
      if (address.city) url.searchParams.set('town', address.city.trim());
      url.searchParams.set('output', 'json');

      const res = await fetch(url.toString(), {
        method: 'GET',
        headers: { Accept: 'application/json' },
      });
      if (!res.ok) {
        logger.warn({ status: res.status, address: pretty }, 'Open register API request failed');
      } else {
        const json: any = await res.json();
        if (json?.status && typeof json.status === 'string' && json.status.toLowerCase() === 'error') {
          logger.warn({ address: pretty, code: json.error_code || json.error, message: json.message || json.error_message || null }, 'Open register API returned error status');
        } else {
          const personList = Array.isArray(json?.person_list) ? json.person_list : [];
          const premisesList = Array.isArray(json?.premises_list) ? json.premises_list : [];
          const matchedPremises = choosePremisesMatch(variantDetails, premisesList, targetPostcode);
          if (matchedPremises) {
            logger.debug(
              {
                address: pretty,
                matchedPremises: {
                  score: matchedPremises.score,
                  sharedTokens: matchedPremises.sharedTokens,
                  targetVariant: matchedPremises.targetVariant,
                  candidatePostcode: matchedPremises.candidatePostcode || null,
                },
              },
              'Open register premises match selected'
            );
          }
          const records: OccupantRecord[] = [];

          const toYear = (value: any): number | undefined => {
            const n = Number(value);
            return Number.isFinite(n) ? n : undefined;
          };

          const dedupeStrings = (values: any[]): string[] => {
            const seen = new Set<string>();
            const out: string[] = [];
            for (const val of values || []) {
              if (val == null) continue;
              const str = String(val).trim();
              if (!str) continue;
              if (seen.has(str.toLowerCase())) continue;
              seen.add(str.toLowerCase());
              out.push(str);
            }
            return out;
          };

          const addFromPerson = (item: any) => {
            if (!item || typeof item !== 'object') return;
            const candidatePostcode = normalizePostcode(item.postcode || item.post_code || item.property_postcode || item.postcode_in);
            const addressFields: any[] = [
              item.property_address,
              item.address,
              item.full_address,
              item.premises,
              item.premise,
              item.street,
              item.thoroughfare,
              item.saon,
              item.paon,
            ];
            if (Array.isArray(item.address_lines)) {
              addressFields.push(...item.address_lines);
            }
            if (
              !matchesAddressTokens(
                addressFields,
                candidatePostcode,
                targetTokens,
                targetNumericTokenList,
                targetPostcode
              )
            ) {
              return;
            }
            const firstName = String(
              item.first_name ||
                item.firstname ||
                item.forename ||
                item.forename1 ||
                item.given_name ||
                ''
            ).trim();
            const lastName = String(
              item.last_name ||
                item.lastname ||
                item.surname ||
                item.family_name ||
                ''
            ).trim();
            const fullName = String(
              item.full_name ||
                item.fullname ||
                item.name_single_line ||
                mergeName(firstName, lastName)
            ).trim();
            const years = Array.isArray(item.years_list)
              ? (item.years_list as any[])
                  .map(toYear)
                  .filter((value: number | undefined): value is number => typeof value === 'number' && Number.isFinite(value))
              : [];
            const sources = dedupeStrings([
              ...(Array.isArray(item.data_sources) ? item.data_sources : []),
              ...(Array.isArray(item.sources) ? item.sources : []),
              item.source,
            ]);
            const indicators = dedupeStrings([
              ...(Array.isArray(item.flags) ? item.flags : []),
              ...(Array.isArray(item.indicators) ? item.indicators : []),
            ]);
            records.push({
              firstName,
              lastName,
              fullName: fullName || mergeName(firstName, lastName),
              ageBand: String(item.age_band || item.ageBand || '').trim() || undefined,
              birthYear: toYear(item.year_of_birth || item.birth_year || item.dob_year),
              firstSeenYear: years.length ? Math.min(...years) : undefined,
              lastSeenYear: years.length ? Math.max(...years) : undefined,
              dataSources: sources,
              indicators,
            });
          };

          const addFromPremises = (item: any) => {
            if (!item || typeof item !== 'object') return;
            const candidatePostcode = normalizePostcode(item.postcode || item.post_code || item.postcode_in);
            const addressFields: any[] = [
              item.full_address,
              item.address,
              item.premises,
              item.premise,
              item.premises_name,
              item.secondary_addressable_object_name,
              item.secondary_addressable_object,
              item.primary_addressable_object_name,
              item.primary_addressable_object,
              item.saon,
              item.paon,
              item.street,
              item.thoroughfare,
            ];
            if (
              !matchesAddressTokens(
                addressFields,
                candidatePostcode,
                targetTokens,
                targetNumericTokenList,
                targetPostcode
              )
            ) {
              return;
            }
            const name = String(item.occupant_name || item.name || item.full_name || '').trim();
            if (!name) return;
            const parts = name.split(/\s+/).filter(Boolean);
            const firstName = parts[0] || '';
            const lastName = parts.length > 1 ? parts.slice(1).join(' ') : '';
            const years = Array.isArray(item.years_list)
              ? (item.years_list as any[])
                  .map(toYear)
                  .filter((value: number | undefined): value is number => typeof value === 'number' && Number.isFinite(value))
              : [];
            const sources = dedupeStrings([
              ...(Array.isArray(item.data_sources) ? item.data_sources : []),
              ...(Array.isArray(item.sources) ? item.sources : []),
              item.source,
            ]);
            const indicators = dedupeStrings([
              ...(Array.isArray(item.flags) ? item.flags : []),
              ...(Array.isArray(item.indicators) ? item.indicators : []),
            ]);
            records.push({
              firstName,
              lastName,
              fullName: name,
              ageBand: undefined,
              birthYear: undefined,
              firstSeenYear: years.length ? Math.min(...years) : undefined,
              lastSeenYear: years.length ? Math.max(...years) : undefined,
              dataSources: sources,
              indicators,
            });
          };

          personList.forEach(addFromPerson);
          premisesList.forEach(addFromPremises);

          const cleaned = dedupe<OccupantRecord>(
            records,
            (occ) => occ.fullName.toLowerCase() || `${(occ.firstName || '').toLowerCase()} ${(occ.lastName || '').toLowerCase()}`.trim()
          );
          if (cleaned.length) {
            try {
              logger.info(
                {
                  address: pretty,
                  source: 't2a_address_person',
                  occupantCount: cleaned.length,
                  sample: cleaned.slice(0, 5).map((occ) => ({
                    fullName: occ.fullName,
                    firstSeenYear: occ.firstSeenYear,
                    lastSeenYear: occ.lastSeenYear,
                    dataSources: occ.dataSources,
                    indicators: occ.indicators,
                  })),
                },
                'Open register occupants found'
              );
            } catch {}
            return { occupants: cleaned, raw: json, source: 't2a_address_person' };
          }
          logger.info({ address: pretty, source: 't2a_address_person', occupantCount: 0 }, 'Open register returned no occupants');
        }
      }
    } catch (err) {
      logger.warn({ address: pretty, err: String(err) }, 'Open register API lookup failed');
    }
  }

  return null;
}
