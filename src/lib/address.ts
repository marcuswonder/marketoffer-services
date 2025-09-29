export type AddressInput = {
  line1: string;
  line2?: string;
  city?: string;
  postcode: string;
  country?: string;
};

export function normalizePostcode(postcode: string | undefined): string {
  return (postcode || '')
    .replace(/[^a-z0-9]/gi, '')
    .toUpperCase();
}

export function canonicalizeAddress(addr: AddressInput): { key: string; lines: string[] } {
  const lineA = (addr.line1 || '').trim().toLowerCase();
  const lineB = (addr.line2 || '').trim().toLowerCase();
  const city = (addr.city || '').trim().toLowerCase();
  const postcode = normalizePostcode(addr.postcode);
  const parts = [lineA, lineB, city].filter(Boolean);
  const key = [lineA, lineB, city, postcode.toLowerCase()].filter(Boolean).join('|');
  return { key, lines: parts };
}

export function addressKey(addr: AddressInput): string {
  const { key } = canonicalizeAddress(addr);
  return key;
}

export function prettyAddress(addr: AddressInput): string {
  const lines = [addr.line1, addr.line2, addr.city, addr.postcode].filter(Boolean);
  const country = (addr.country || '').trim();
  if (country) lines.push(country);
  return lines.join(', ');
}

export function mergeAddress(base: AddressInput, overrides: Partial<AddressInput>): AddressInput {
  return {
    line1: overrides.line1 ?? base.line1,
    line2: overrides.line2 ?? base.line2,
    city: overrides.city ?? base.city,
    postcode: overrides.postcode ?? base.postcode,
    country: overrides.country ?? base.country
  };
}
