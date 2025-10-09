import fs from 'fs';
import path from 'path';
import { fetch } from 'undici';
import { logger } from './logger.js';
import { AddressInput, addressKey, prettyAddress, normalizePostcode } from './address.js';

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

export async function lookupOpenRegister(address: AddressInput): Promise<OpenRegisterResult | null> {
  const pretty = prettyAddress(address);

  const apiKey = process.env.T2A_API_KEY;
  const apiBase = process.env.T2A_BASE_URL || 'https://api.t2a.io/rest/rest.aspx';
  if (apiKey) {
    try {
      const premisesParts = [address.line1, address.line2].map(s => (s || '').trim()).filter(Boolean);
      const premises = premisesParts.join(' ');
      const url = new URL(apiBase);
      url.searchParams.set('method', 'address_person');
      url.searchParams.set('api_key', apiKey);
      if (premises) url.searchParams.set('premises', premises);
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
