import fs from 'fs';
import path from 'path';
import { fetch } from 'undici';
import { logger } from './logger.js';
import { AddressInput, addressKey, prettyAddress, normalizePostcode } from './address.js';
import { pool, query } from './db.js';
import { logEvent } from '../lib/progress.js';

export type CorporateOwnerRecord = {
  ownerName: string;
  companyNumber?: string;
  addressKey: string;
  raw?: any;
  datasetLabel?: string;
};

export type CorporateOwnerMatch = {
  matchType: 'exact' | 'postcode';
  ownerName: string;
  companyNumber?: string;
  source: string;
  raw?: any;
};

export type PricePaidTransaction = {
  paon?: string;
  saon?: string;
  street?: string;
  town?: string;
  county?: string;
  postcode?: string;
  amount?: number;
  date?: string;
  category?: string;
  raw?: any;
};

type DatasetConfig = {
  label: string;
  fileSource?: string;
  datasetId?: string;
  refreshMs: number;
};

const DEFAULT_REFRESH_MONTHS = Math.max(1, Number(process.env.LAND_REGISTRY_CORPORATE_DATA_REFRESH_MONTHS || '1'));
const DEFAULT_REFRESH_MS = DEFAULT_REFRESH_MONTHS * 30 * 24 * 60 * 60 * 1000;
const API_KEY = (process.env.LAND_REGISTRY_API_KEY || '').trim();
const API_BASE = (process.env.LAND_REGISTRY_API_BASE || 'https://use-land-property-data.service.gov.uk/api').replace(/\/$/, '');
const PRICE_PAID_ENDPOINT = (process.env.LAND_REGISTRY_PRICE_PAID_ENDPOINT || 'https://landregistry.data.gov.uk/landregistry/query').replace(/\/$/, '');

const DATASET_CONFIGS: DatasetConfig[] = [
  {
    label: 'uk',
    fileSource: (process.env.LAND_REGISTRY_UK_CORPORATE_DATA || '').trim() || undefined,
    datasetId: (process.env.LAND_REGISTRY_UK_CORPORATE_DATASET || '').trim() || undefined,
    refreshMs: DEFAULT_REFRESH_MS,
  },
  {
    label: 'intl',
    fileSource: (process.env.LAND_REGISTRY_INTL_CORPORATE_DATA || '').trim() || undefined,
    datasetId: (process.env.LAND_REGISTRY_INTL_CORPORATE_DATASET || '').trim() || undefined,
    refreshMs: DEFAULT_REFRESH_MS,
  },
].filter(cfg => cfg.fileSource || cfg.datasetId);

const API_SEARCH_BASES = [
  (process.env.LAND_REGISTRY_UK_CORPORATE_API || '').trim(),
  (process.env.LAND_REGISTRY_INTL_CORPORATE_API || '').trim(),
].filter(Boolean);

let datasetCache: Map<string, CorporateOwnerRecord> | null = null;
let datasetCacheLoaded = false;

function formatPostcodeForQuery(postcode: string): string | null {
  const normalized = normalizePostcode(postcode);
  if (!normalized) return null;
  if (normalized.length <= 3) return normalized;
  const outward = normalized.slice(0, normalized.length - 3);
  const inward = normalized.slice(-3);
  return `${outward} ${inward}`;
}

export async function fetchPricePaidTransactionsByPostcode(
  postcode: string
): Promise<PricePaidTransaction[]> {
  const formattedPostcode = formatPostcodeForQuery(postcode);
  if (!formattedPostcode) return [];

  const prefixes = [
    'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>',
    'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>',
    'PREFIX owl: <http://www.w3.org/2002/07/owl#>',
    'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>',
    'PREFIX sr: <http://data.ordnancesurvey.co.uk/ontology/spatialrelations/>',
    'PREFIX ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>',
    'PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>',
    'PREFIX skos: <http://www.w3.org/2004/02/skos/core#>',
    'PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>',
  ].join('\n');

  const sparqlQuery = `${prefixes}

SELECT ?paon ?saon ?street ?town ?county ?postcode ?amount ?date ?category
WHERE
{
  VALUES ?postcode {"${formattedPostcode}"^^xsd:string}

  ?addr lrcommon:postcode ?postcode.

  ?transx lrppi:propertyAddress ?addr ;
          lrppi:pricePaid ?amount ;
          lrppi:transactionDate ?date ;
          lrppi:transactionCategory/skos:prefLabel ?category.

  OPTIONAL {?addr lrcommon:county ?county}
  OPTIONAL {?addr lrcommon:paon ?paon}
  OPTIONAL {?addr lrcommon:saon ?saon}
  OPTIONAL {?addr lrcommon:street ?street}
  OPTIONAL {?addr lrcommon:town ?town}
}
ORDER BY DESC(?date)
`;

  try {
    const params = new URLSearchParams();
    params.set('query', sparqlQuery);

    const res = await fetch(PRICE_PAID_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/sparql-results+json',
      },
      body: params.toString(),
    });

    if (!res.ok) {
      logger.warn(
        { status: res.status, postcode: formattedPostcode },
        'Land Registry price paid data request failed'
      );
      return [];
    }

    const json: any = await res.json();
    const bindings: any[] = Array.isArray(json?.results?.bindings) ? json.results.bindings : [];
    const literal = (binding: any, key: string): string | undefined => {
      if (!binding || typeof binding !== 'object') return undefined;
      const entry = binding[key];
      if (!entry || typeof entry !== 'object') return undefined;
      const value = entry.value;
      return typeof value === 'string' ? value.trim() : undefined;
    };
    const toNumber = (value: string | undefined): number | undefined => {
      if (value == null) return undefined;
      const num = Number(value);
      return Number.isFinite(num) ? num : undefined;
    };

    return bindings.map((binding: any) => {
      const amount = toNumber(literal(binding, 'amount'));
      const transactionDate = literal(binding, 'date');
      return {
        paon: literal(binding, 'paon'),
        saon: literal(binding, 'saon'),
        street: literal(binding, 'street'),
        town: literal(binding, 'town'),
        county: literal(binding, 'county'),
        postcode: literal(binding, 'postcode') || formattedPostcode,
        amount,
        date: transactionDate,
        category: literal(binding, 'category'),
        raw: binding,
      };
    });
  } catch (err) {
    logger.warn(
      { postcode: formattedPostcode, error: err instanceof Error ? err.message : String(err) },
      'Land Registry price paid data request threw'
    );
    return [];
  }
}

function tokenizeAddress(value: string | undefined | null): string[] {
  if (!value) return [];
  return value
    .toLowerCase()
    .replace(/[\u2013\u2014\-\/]/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

function addressTokensFromRecord(record: CorporateOwnerRecord): Set<string> {
  const tokens = new Set<string>();
  const addTokens = (val: string | undefined | null) => {
    for (const token of tokenizeAddress(val)) tokens.add(token);
  };

  const keyParts = (record.addressKey || '').split('|');
  keyParts.forEach((part) => addTokens(part));

  const raw = record.raw || {};
  addTokens(raw?.property_address || raw?.Property_Address);
  addTokens(raw?.address || raw?.Address);
  addTokens(raw?.premises);

  const rawAddressLines = Array.isArray(raw?.address_lines) ? raw.address_lines : [];
  rawAddressLines.forEach((line: any) => addTokens(typeof line === 'string' ? line : ''));

  const proprietorLines = Array.isArray(raw?.proprietor?.address_lines) ? raw.proprietor.address_lines : [];
  proprietorLines.forEach((line: any) => addTokens(typeof line === 'string' ? line : ''));

  return tokens;
}

function addressMatchesInput(record: CorporateOwnerRecord, address: AddressInput): boolean {
  const candidateTokens = addressTokensFromRecord(record);
  if (!candidateTokens.size) return false;

  const primaryTokens = tokenizeAddress(address.line1);
  if (!primaryTokens.length) return false;

  const secondaryTokens = tokenizeAddress(address.line2 || '');
  const requiredTokens = secondaryTokens.length
    ? Array.from(new Set([...primaryTokens, ...secondaryTokens]))
    : primaryTokens;

  const numericTokens = requiredTokens.filter((token) => /\d/.test(token));
  if (numericTokens.length && !numericTokens.every((token) => candidateTokens.has(token))) {
    return false;
  }

  if (!requiredTokens.every((token) => candidateTokens.has(token))) {
    return false;
  }

  return true;
}

export async function searchCorporateOwnersByPostcode(postcode: string): Promise<CorporateOwnerRecord[]> {
  const normalized = normalizePostcode(postcode);
  if (!normalized) return [];
  const suffix = normalized.toLowerCase();
  const { rows } = await query<{
    address_key: string;
    owner_name: string;
    company_number: string | null;
    dataset_label: string | null;
    raw: any;
  }>(
    `SELECT address_key, owner_name, company_number, dataset_label, raw
       FROM land_registry_corporate
       WHERE address_key LIKE $1
       ORDER BY owner_name`,
    [`%|${suffix}`]
  );
  return rows.map((row) => ({
    ownerName: row.owner_name,
    companyNumber: row.company_number || undefined,
    addressKey: row.address_key,
    raw: row.raw,
    datasetLabel: row.dataset_label || undefined,
  }));
}

function isUrl(value: string): boolean {
  return /^https?:\/\//i.test(value);
}

function readField(entry: any, candidates: string[]): any {
  if (!entry || typeof entry !== 'object') return undefined;
  const keys = Object.keys(entry);
  for (const candidate of candidates) {
    const lower = candidate.toLowerCase();
    for (const actual of keys) {
      if (actual === candidate || actual.toLowerCase() === lower) {
        return entry[actual];
      }
    }
  }
  return undefined;
}

function ensureString(value: any): string {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  return String(value).trim();
}

function entryToRecord(entry: any, defaultLabel: string): CorporateOwnerRecord | null {
  if (!entry || typeof entry !== 'object') return null;
  let line1 = ensureString(readField(entry, ['address1', 'address_line_1', 'address', 'property_address_1', 'property_address1', 'premises']));
  let line2 = ensureString(readField(entry, ['address2', 'address_line_2', 'property_address_2', 'property_address2', 'street']));
  let city = ensureString(readField(entry, ['town', 'city', 'locality', 'address_town', 'post_town']));
  let postcode = ensureString(readField(entry, ['postcode', 'pc', 'post_code', 'postal_code']));

  if (!line1) {
    const combined = ensureString(readField(entry, ['property_address', 'full_address', 'address']));
    if (combined) {
      const parts = combined.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
      if (parts.length) {
        line1 = parts[0];
        if (!line2 && parts.length > 1) line2 = parts[1];
        if (!city && parts.length > 2) city = parts[parts.length - 2];
        if (!postcode) postcode = parts[parts.length - 1];
      }
    }
  }

  if (!line1 || !postcode) return null;

  const ownerName = ensureString(readField(entry, ['companyName', 'ownerName', 'name', 'proprietor_name', 'proprietors_name', 'company']));
  if (!ownerName) return null;

  const key = addressKey({
    line1,
    line2: line2 || undefined,
    city: city || undefined,
    postcode,
    country: ensureString(readField(entry, ['country'])) || 'GB',
  });
  if (!key) return null;

  return {
    ownerName,
    companyNumber: ensureString(readField(entry, ['companyNumber', 'company_number', 'crn', 'companyregistrationno', 'company_registration_number'])) || undefined,
    addressKey: key,
    raw: entry,
    datasetLabel: ensureString(readField(entry, ['datasetLabel', 'dataset', 'source'])) || defaultLabel,
  };
}

function parseDatasetJson(json: any): any[] {
  if (!json) return [];
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.records)) return json.records;
  if (Array.isArray(json?.items)) return json.items;
  return [];
}

function splitCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuote && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuote = !inQuote;
      }
    } else if (ch === ',' && !inQuote) {
      result.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
}

function parseCsv(text: string): any[] {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (!lines.length) return [];
  const headers = splitCsvLine(lines[0]);
  const records: any[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    if (!cols.length) continue;
    const obj: Record<string, any> = {};
    headers.forEach((header, idx) => {
      obj[header] = cols[idx] ?? '';
    });
    records.push(obj);
  }
  return records;
}

function extractApiRecords(payload: any): any[] {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) {
    return payload.data.map((item: any) => {
      if (item && typeof item === 'object') {
        return item.attributes || item;
      }
      return item;
    });
  }
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.records)) return payload.records;
  if (Array.isArray(payload?.items)) return payload.items;
  return [];
}

async function parseDataset(config: DatasetConfig, payload: string, contentType?: string | null): Promise<CorporateOwnerRecord[]> {
  let entries: any[] = [];
  const trimmed = payload.trim();
  if (contentType && /json/i.test(contentType)) {
    try {
      entries = parseDatasetJson(JSON.parse(trimmed));
    } catch (err) {
      logger.warn({ dataset: config.label, err: String(err) }, 'Failed to parse dataset JSON, attempting CSV fallback');
    }
  }
  if (!entries.length) {
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        entries = parseDatasetJson(JSON.parse(trimmed));
      } catch {
        entries = parseCsv(trimmed);
      }
    } else {
      entries = parseCsv(trimmed);
    }
  }
  const records: CorporateOwnerRecord[] = [];
  for (const entry of entries) {
    const record = entryToRecord(entry, config.label);
    if (record) {
      record.datasetLabel = record.datasetLabel || config.label;
      records.push(record);
    }
  }
  return records;
}

async function loadDatasetFromFile(config: DatasetConfig): Promise<Map<string, CorporateOwnerRecord>> {
  const map = new Map<string, CorporateOwnerRecord>();
  if (!config.fileSource || !config.fileSource.length || isUrl(config.fileSource)) return map;
  try {
    const resolved = path.isAbsolute(config.fileSource) ? config.fileSource : path.join(process.cwd(), config.fileSource);
    const data = await fs.promises.readFile(resolved, 'utf-8');
    const records = await parseDataset(config, data, 'application/json');
    for (const record of records) {
      if (!map.has(record.addressKey)) map.set(record.addressKey, record);
    }
    if (records.length) {
      logger.info({ dataset: config.label, entries: records.length, source: resolved }, 'Loaded Land Registry dataset from file');
    }
  } catch (err) {
    logger.warn({ dataset: config.label, err: String(err) }, 'Failed to load Land Registry dataset from file');
  }
  return map;
}

async function loadDatasetFromDb(): Promise<Map<string, CorporateOwnerRecord>> {
  const { rows } = await query<{
    address_key: string;
    owner_name: string;
    company_number: string | null;
    dataset_label: string | null;
    raw: any;
  }>(
    `SELECT address_key, owner_name, company_number, dataset_label, raw
       FROM land_registry_corporate
       ORDER BY CASE dataset_label WHEN 'uk' THEN 0 WHEN 'intl' THEN 1 ELSE 2 END, address_key`
  );
  const map = new Map<string, CorporateOwnerRecord>();
  for (const row of rows) {
    if (!map.has(row.address_key)) {
      map.set(row.address_key, {
        ownerName: row.owner_name,
        companyNumber: row.company_number || undefined,
        addressKey: row.address_key,
        datasetLabel: row.dataset_label || 'uk',
        raw: row.raw,
      });
    }
  }
  if (map.size) {
    logger.debug({ entries: map.size }, 'Loaded Land Registry dataset from database');
  }
  return map;
}

async function storeRecordsInDb(config: DatasetConfig, records: CorporateOwnerRecord[]): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM land_registry_corporate WHERE dataset_label = $1', [config.label]);

    const batchSize = 400;
    for (let i = 0; i < records.length; i += batchSize) {
      const slice = records.slice(i, i + batchSize);
      const values: string[] = [];
      const params: any[] = [];
      slice.forEach((record, idx) => {
        const base = idx * 5;
        values.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
        params.push(
          record.addressKey,
          record.ownerName,
          record.companyNumber || null,
          record.datasetLabel || config.label,
          record.raw ? JSON.stringify(record.raw) : JSON.stringify({ ownerName: record.ownerName })
        );
      });
      await client.query(
        `INSERT INTO land_registry_corporate (address_key, owner_name, company_number, dataset_label, raw)
         VALUES ${values.join(',')}`,
        params
      );
    }

    await client.query(
      `INSERT INTO land_registry_corporate_meta (dataset_label, last_refreshed_at, source_url, row_count)
         VALUES ($1, now(), $2, $3)
         ON CONFLICT (dataset_label) DO UPDATE
           SET last_refreshed_at = excluded.last_refreshed_at,
               source_url = excluded.source_url,
               row_count = excluded.row_count`,
      [config.label, config.datasetId || config.fileSource || '', records.length]
    );

    await client.query('COMMIT');
    datasetCacheLoaded = false;
    datasetCache = null;
    logger.info({ dataset: config.label, records: records.length }, 'Land Registry dataset refreshed');
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch {}
    throw err;
  } finally {
    client.release();
  }
}

function buildApiHeaders(): Record<string, string> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (API_KEY) {
    headers['Authorization'] = `Bearer ${API_KEY}`;
    headers['X-API-KEY'] = API_KEY;
  }
  return headers;
}

async function downloadDatasetViaApi(config: DatasetConfig): Promise<CorporateOwnerRecord[]> {
  if (!config.datasetId) return [];
  const pageSize = 1000;
  let page = 1;
  const records: CorporateOwnerRecord[] = [];
  let nextUrl: string | null = `${API_BASE}/datasets/${encodeURIComponent(config.datasetId)}/records?page[number]=${page}&page[size]=${pageSize}`;
  const seen = new Set<string>();
  const headers = buildApiHeaders();

  while (nextUrl) {
    if (seen.has(nextUrl)) {
      logger.warn({ dataset: config.datasetId }, 'Detected repeated Land Registry API URL, stopping to avoid loop');
      break;
    }
    seen.add(nextUrl);
    const res = await fetch(nextUrl, { headers });
    if (!res.ok) {
      throw new Error(`Failed to fetch dataset ${config.datasetId}: ${res.status} ${res.statusText}`);
    }
    const payload: any = await res.json();
    const chunk = extractApiRecords(payload);
    if (!Array.isArray(chunk) || !chunk.length) {
      break;
    }
    for (const entry of chunk) {
      const record = entryToRecord(entry, config.label);
      if (record) {
        record.datasetLabel = record.datasetLabel || config.label;
        records.push(record);
      }
    }
    const linkNext = payload?.links?.next;
    if (typeof linkNext === 'string' && linkNext.trim()) {
      try {
        const resolved = new URL(linkNext, API_BASE);
        nextUrl = resolved.toString();
        continue;
      } catch {
        nextUrl = linkNext;
        continue;
      }
    }
    if (chunk.length < pageSize) {
      break;
    }
    page += 1;
    nextUrl = `${API_BASE}/datasets/${encodeURIComponent(config.datasetId)}/records?page[number]=${page}&page[size]=${pageSize}`;
  }

  return records;
}

async function refreshDatasetIfNeeded(config: DatasetConfig): Promise<void> {
  const hasRemoteSource = config.fileSource && isUrl(config.fileSource);
  const usesApi = !!config.datasetId;
  if (!hasRemoteSource && !usesApi) return;

  let needsRefresh = true;
  try {
    const { rows } = await query<{ last_refreshed_at: Date | null }>(
      `SELECT last_refreshed_at FROM land_registry_corporate_meta WHERE dataset_label = $1 LIMIT 1`,
      [config.label]
    );
    if (rows.length && rows[0].last_refreshed_at) {
      const last = new Date(rows[0].last_refreshed_at).getTime();
      if (!Number.isNaN(last)) {
        const ageMs = Date.now() - last;
        const threshold = config.refreshMs;
        if (ageMs < threshold) needsRefresh = false;
      }
    }
  } catch (err) {
    logger.warn({ dataset: config.label, err: String(err) }, 'Failed to read Land Registry dataset metadata; forcing refresh');
  }
  if (!needsRefresh) return;

  if (usesApi) {
    const records = await downloadDatasetViaApi(config);
    if (!records.length) {
      throw new Error(`No records parsed from Land Registry API dataset (${config.datasetId})`);
    }
    await storeRecordsInDb(config, records);
    return;
  }

  if (hasRemoteSource && config.fileSource) {
    const res = await fetch(config.fileSource);
    if (!res.ok) {
      throw new Error(`Failed to download Land Registry dataset (${config.label}): ${res.status} ${res.statusText}`);
    }
    const payload = await res.text();
    const records = await parseDataset(config, payload, res.headers.get('content-type'));
    if (!records.length) {
      throw new Error(`No records parsed from Land Registry dataset (${config.label})`);
    }
    await storeRecordsInDb(config, records);
  }
}

async function ensureDatasetLoaded(): Promise<Map<string, CorporateOwnerRecord>> {
  if (datasetCacheLoaded && datasetCache) return datasetCache;

  for (const config of DATASET_CONFIGS) {
    try {
      await refreshDatasetIfNeeded(config);
    } catch (err) {
      logger.error({ dataset: config.label, err: String(err) }, 'Failed to refresh Land Registry dataset');
    }
  }

  const combined = await loadDatasetFromDb();
  for (const config of DATASET_CONFIGS) {
    if (!config.fileSource || isUrl(config.fileSource)) continue;
    const fileMap = await loadDatasetFromFile(config);
    for (const [key, record] of fileMap.entries()) {
      if (!combined.has(key)) combined.set(key, record);
    }
  }

  datasetCache = combined;
  datasetCacheLoaded = true;
  return combined;
}

async function lookupSearchApi(queryString: string): Promise<CorporateOwnerMatch | null> {
  if (!API_SEARCH_BASES.length) return null;
  const headers = buildApiHeaders();
  for (const base of API_SEARCH_BASES) {
    try {
      const url = new URL(base);
      url.searchParams.set('q', queryString);
      const res = await fetch(url.toString(), { headers });
      if (!res.ok) continue;
      const data: any = await res.json();
      const match = Array.isArray(data?.results) ? data.results[0] : data;
      if (match?.ownerName || match?.name) {
        return {
          matchType: 'exact',
          ownerName: match.ownerName || match.name,
          companyNumber: match.companyNumber || match.company_number,
          source: new URL(base).hostname,
          raw: match,
        };
      }
    } catch (err) {
      logger.warn({ base, err: String(err) }, 'Land Registry search API lookup failed');
    }
  }
  return null;
}

export async function findCorporateOwner(address: AddressInput): Promise<CorporateOwnerMatch | null> {
  const pretty = prettyAddress(address);
  const dataset = await ensureDatasetLoaded();
  if (dataset.size) {
    const key = addressKey(address);
    const direct = key ? dataset.get(key) : undefined;
    if (direct && direct.ownerName && addressMatchesInput(direct, address)) {
      return {
        matchType: 'exact',
        ownerName: direct.ownerName,
        companyNumber: direct.companyNumber,
        source: direct.datasetLabel || 'land_registry_dataset',
        raw: direct.raw,
      };
    }
    const postcode = normalizePostcode(address.postcode);
    if (postcode) {
      const hits = Array.from(dataset.values()).filter((rec) => {
        if (!rec.raw) return false;
        const pc = normalizePostcode(
          rec.raw?.postcode || rec.raw?.pc || rec.raw?.postal_code || rec.raw?.post_code
        );
        if (pc !== postcode) return false;
        return addressMatchesInput(rec, address);
      });
      if (hits.length === 1 && hits[0].ownerName) {
        return {
          matchType: 'postcode',
          ownerName: hits[0].ownerName,
          companyNumber: hits[0].companyNumber,
          source: hits[0].datasetLabel || 'land_registry_dataset',
          raw: hits[0].raw,
        };
      }
    }
  }

  const fallback = await lookupSearchApi(pretty);
  if (!fallback) return null;

  if (fallback.raw) {
    const synthetic: CorporateOwnerRecord = {
      ownerName: fallback.ownerName,
      companyNumber: fallback.companyNumber,
      addressKey: '',
      raw: fallback.raw,
    };
    if (!addressMatchesInput(synthetic, address)) {
      logger.debug({ address: pretty }, 'Discarded Land Registry API result due to address mismatch');
      return null;
    }
  }

  return fallback;
}
