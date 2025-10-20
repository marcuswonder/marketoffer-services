export type AddressInput = {
  unit?: string;
  buildingName?: string;
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
  const unit = (addr.unit || '').trim().toLowerCase();
  const building = (addr.buildingName || '').trim().toLowerCase();
  const lineA = (addr.line1 || '').trim().toLowerCase();
  const lineB = (addr.line2 || '').trim().toLowerCase();
  const city = (addr.city || '').trim().toLowerCase();
  const postcode = normalizePostcode(addr.postcode);
  const parts = [unit, building, lineA, lineB, city].filter(Boolean);
  const key = [unit, building, lineA, lineB, city, postcode.toLowerCase()].filter(Boolean).join('|');
  return { key, lines: parts };
}

export function addressKey(addr: AddressInput): string {
  const { key } = canonicalizeAddress(addr);
  return key;
}

export function prettyAddress(addr: AddressInput): string {
  const lines = [addr.unit, addr.buildingName, addr.line1, addr.line2, addr.city, addr.postcode].filter(Boolean);
  const country = (addr.country || '').trim();
  if (country) lines.push(country);
  return lines.join(', ');
}

export function mergeAddress(base: AddressInput, overrides: Partial<AddressInput>): AddressInput {
  return {
    unit: overrides.unit ?? base.unit,
    buildingName: overrides.buildingName ?? base.buildingName,
    line1: overrides.line1 ?? base.line1,
    line2: overrides.line2 ?? base.line2,
    city: overrides.city ?? base.city,
    postcode: overrides.postcode ?? base.postcode,
    country: overrides.country ?? base.country
  };
}

export function addressVariants(addr: AddressInput): string[] {
  const variants = new Set<string>();
  const add = (val?: string | null) => {
    if (!val) return;
    const trimmed = val.trim();
    if (!trimmed) return;
    variants.add(trimmed);
  };
  const segments = [addr.unit, addr.buildingName, addr.line1, addr.line2].filter(Boolean) as string[];
  if (segments.length) add(segments.join(' '));
  if (segments.length > 1) add([...segments].reverse().join(' '));
  const lines = [addr.line1, addr.line2].filter(Boolean) as string[];
  if (lines.length) add(lines.join(' '));
  if (lines.length > 1) add([...lines].reverse().join(' '));
  add(addr.line1);
  add(addr.line2);
  add(addr.unit);
  add(addr.buildingName);
  if (addr.unit && addr.line1) add(`${addr.unit} ${addr.line1}`);
  if (addr.unit && addr.line2) add(`${addr.unit} ${addr.line2}`);
  if (addr.buildingName && addr.line1) add(`${addr.buildingName} ${addr.line1}`);
  if (addr.buildingName && addr.line2) add(`${addr.buildingName} ${addr.line2}`);
  if (addr.city) {
    add(`${addr.line1} ${addr.city}`);
    if (lines.length) add(`${lines.join(' ')} ${addr.city}`);
    if (addr.unit) add(`${addr.unit} ${addr.city}`);
    if (addr.buildingName) add(`${addr.buildingName} ${addr.city}`);
  }
  return Array.from(variants);
}
