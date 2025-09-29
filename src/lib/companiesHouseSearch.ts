import { Buffer } from 'node:buffer';
import { logger } from './logger.js';
import { httpGetJson } from './http.js';
import { AddressInput, prettyAddress, normalizePostcode } from './address.js';

const CH_BASE = process.env.CH_API_BASE || 'https://api.company-information.service.gov.uk';
const CH_KEY = process.env.CH_API_KEY || '';

function chHeaders() {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  const auth = Buffer.from(`${CH_KEY}:`).toString('base64');
  return { Authorization: `Basic ${auth}` };
}

function normalizeLine(line?: string | null) {
  return (line || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function addressMatches(target: AddressInput, candidate: any): boolean {
  if (!candidate) return false;
  const cPostcode = normalizePostcode(candidate.postal_code || candidate.postcode || candidate.post_code);
  const tPostcode = normalizePostcode(target.postcode);
  if (tPostcode && cPostcode && cPostcode !== tPostcode) return false;
  const candLines = [candidate.address_line_1 || candidate.address_line_2 || candidate.street_address]
    .flat()
    .filter(Boolean)
    .map((line: any) => normalizeLine(String(line)));
  const targetLine = normalizeLine(target.line1);
  if (targetLine && candLines.length) {
    return candLines.some((line) => line.includes(targetLine) || targetLine.includes(line));
  }
  return true;
}

export type CompanyAddressHit = {
  companyNumber: string;
  companyName: string;
  status?: string;
  raw: any;
};

export type OfficerAddressHit = {
  name: string;
  officerId?: string;
  address?: any;
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
    const filtered = items.filter((item: any) => addressMatches(address, item?.address || item?.registered_office_address));
    return filtered.map((item: any) => ({
      companyNumber: item.company_number,
      companyName: item.title || item.company_name,
      status: item.company_status,
      raw: item,
    }));
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
    const filtered = items.filter((item: any) => addressMatches(address, item?.address));
    return filtered.map((item: any) => ({
      name: item.title || item.name,
      officerId: item.links?.officer?.self?.split('/').pop(),
      address: item.address,
      raw: item,
    }));
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
  };
}

export function summarizeCompany(hit: CompanyAddressHit) {
  return {
    companyNumber: hit.companyNumber,
    companyName: hit.companyName,
    status: hit.status,
  };
}
