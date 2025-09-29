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
  const cacheDir = process.env.OWNER_OPEN_REGISTER_CACHE;
  if (cacheDir) {
    try {
      const key = addressKey(address) || normalizePostcode(address.postcode);
      if (key) {
        const filePath = path.isAbsolute(cacheDir) ? path.join(cacheDir, `${key}.json`) : path.join(process.cwd(), cacheDir, `${key}.json`);
        if (fs.existsSync(filePath)) {
          const txt = fs.readFileSync(filePath, 'utf-8');
          const raw = JSON.parse(txt);
          const occupants: OccupantRecord[] = Array.isArray(raw?.occupants)
            ? raw.occupants.map((rec: any) => ({
                firstName: rec.firstName || rec.first_name || '',
                lastName: rec.lastName || rec.last_name || '',
                fullName: rec.fullName || rec.full_name || mergeName(rec.firstName || rec.first_name, rec.lastName || rec.last_name),
                ageBand: rec.ageBand || rec.age_band,
                birthYear: rec.birthYear || rec.birth_year,
                firstSeenYear: rec.firstSeenYear || rec.first_seen_year,
                lastSeenYear: rec.lastSeenYear || rec.last_seen_year,
                dataSources: Array.isArray(rec.dataSources) ? rec.dataSources : Array.isArray(rec.data_sources) ? rec.data_sources : [],
                indicators: Array.isArray(rec.indicators) ? rec.indicators : [],
              }))
            : [];
          return {
            occupants,
            raw,
            source: raw?.source || 'open_register_cache',
          };
        }
      }
    } catch (err) {
      logger.warn({ address: pretty, err: String(err) }, 'Failed to read open register cache');
    }
  }

  const apiKey = process.env.T2A_API_KEY;
  const apiBase = process.env.T2A_BASE_URL || 'https://api.t2a.io/v1';
  if (apiKey) {
    try {
      const url = new URL('/person/address', apiBase);
      url.searchParams.set('address', pretty);
      const res = await fetch(url.toString(), {
        headers: {
          'X-API-KEY': apiKey,
          Accept: 'application/json',
        },
      });
      if (res.ok) {
        const json: any = await res.json();
        const data: any[] = Array.isArray(json?.items) ? json.items : [];
        const mapped: OccupantRecord[] = data.map((item: any) => {
          const sources: string[] = [];
          if (Array.isArray(item.sources)) {
            for (const src of item.sources) {
              if (src?.code) sources.push(String(src.code));
            }
          }
          return {
            firstName: item.firstName || item.first_name || '',
            lastName: item.lastName || item.last_name || '',
            fullName: mergeName(item.firstName || item.first_name, item.lastName || item.last_name),
            ageBand: item.ageBand || item.age_band,
            birthYear: item.yearOfBirth || item.birthYear || item.birth_year,
            firstSeenYear: item.firstSeen || item.first_seen,
            lastSeenYear: item.lastSeen || item.last_seen,
            dataSources: sources,
            indicators: Array.isArray(item.indicators) ? item.indicators : [],
          } as OccupantRecord;
        });
        const cleaned = dedupe<OccupantRecord>(mapped, (occ) => occ.fullName.toLowerCase());
        return { occupants: cleaned, raw: json, source: 't2a_open_register' };
      }
      logger.warn({ status: res.status, address: pretty }, 'Open register API request failed');
    } catch (err) {
      logger.warn({ address: pretty, err: String(err) }, 'Open register API lookup failed');
    }
  }

  return null;
}
