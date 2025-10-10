import { Buffer } from 'node:buffer';
import { httpGetJson, HttpOpts } from './http.js';
import { scheduleChRequest } from './chRateLimiter.js';

const CH_BASE = process.env.CH_API_BASE || 'https://api.company-information.service.gov.uk';
const CH_KEY = process.env.CH_API_KEY || '';

function ensureKey() {
  if (!CH_KEY) throw new Error('Missing CH_API_KEY');
  return CH_KEY;
}

function buildUrl(endpoint: string): string {
  if (/^https?:\/\//i.test(endpoint)) return endpoint;
  const path = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  return `${CH_BASE.replace(/\/$/, '')}${path}`;
}

function withAuthHeaders(headers: Record<string, string> = {}): Record<string, string> {
  const key = ensureKey();
  const auth = Buffer.from(`${key}:`).toString('base64');
  return { Authorization: `Basic ${auth}`, ...headers };
}

export async function chGetJson<T = any>(endpoint: string, opts: HttpOpts = {}): Promise<T> {
  const headers = withAuthHeaders(opts.headers || {});
  const url = buildUrl(endpoint);
  return scheduleChRequest(() => httpGetJson<T>(url, { ...opts, headers }));
}
