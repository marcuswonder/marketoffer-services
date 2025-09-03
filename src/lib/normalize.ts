export function cleanCompanyName(name: string): string {
  return name.replace(/\b(limited|ltd|plc)\b\.?/gi, "").replace(/\s{2,}/g, " ").trim();
}

export function baseHost(url: string): string {
  try {
    const u = new URL(url);
    let h = u.hostname.toLowerCase();
    if (h.startsWith("www.")) h = h.slice(4);
    return h;
  } catch {
    return url.toLowerCase();
  }
}

export function parseOfficerName(companiesHouseName: string) {
  // CH names often "SURNAME, First Middle"
  const [last, rest] = companiesHouseName.split(",").map(s => s.trim());
  if (!rest) return { first: last, middle: "", last: "" };
  const parts = rest.split(/\s+/);
  const first = parts.shift() || "";
  const middle = parts.join(" ");
  return { first, middle, last };
}

export function officerIdFromUrl(url: string) {
  const m = url.match(/\/officers\/([^/]+)\//i);
  return m ? m[1] : null;
}

export function monthStr(m: number) {
  return ["January","February","March","April","May","June","July","August","September","October","November","December"][m-1] || "";
}


export function normalizeWord(s: string) {
  return (s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

export function nameMatches(
  target: { first?: string; last?: string },
  candidate: { first: string; last: string }
) {
  const tf = normalizeWord(target.first || "");
  const tl = normalizeWord(target.last || "");
  const cf = normalizeWord(candidate.first);
  const cl = normalizeWord(candidate.last);
  // Last name must match if provided
  if (tl && cl !== tl) return false;
  // First name: accept exact or initial match if provided
  if (tf && cf && cf !== tf && cf[0] !== tf[0]) return false;
  return true;
}

