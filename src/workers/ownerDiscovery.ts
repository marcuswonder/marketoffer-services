import 'dotenv/config';
import { Worker } from 'bullmq';
import { connection, chQ, companyQ, personQ } from '../queues/index.js';
import { initDb, startJob, logEvent, completeJob, failJob } from '../lib/progress.js';
import { query } from '../lib/db.js';
import { AddressInput, prettyAddress, addressKey } from '../lib/address.js';
import { findCorporateOwner, searchCorporateOwnersByPostcode, fetchPricePaidTransactionsByPostcode, normalizeCorporateRecordAddress } from '../lib/landRegistry.js';
import type { CorporateOwnerRecord, CorporateOwnerMatch, PricePaidTransaction } from '../lib/landRegistry.js';
import { lookupOpenRegister } from '../lib/openRegister.js';
import type { OccupantRecord, OccupantCompanyRelation } from '../lib/openRegister.js';
import { scoreOccupants } from '../lib/homeownerRubric.js';
import {
  searchCompaniesByAddress,
  searchOfficersByAddress,
  summarizeCompany,
  summarizeOfficer,
  listCompanyDirectors,
  listCompanyPscs,
  fetchOfficerAppointments,
  type CompanyAddressHit,
  type CompanyOfficerRecord,
  type CompanyPscRecord,
} from '../lib/companiesHouseSearch.js';
import { parseOfficerName } from '../lib/normalize.js';
import { normalizeAddressFragments, NormalizedAddress } from '../lib/normalizedAddress.js';

await initDb();

const ACCEPT_THRESHOLD = Number(process.env.OWNER_ACCEPT_THRESHOLD || 0.55);
const REVIEW_THRESHOLD = Number(process.env.OWNER_REVIEW_THRESHOLD || 0.35);
const AUTO_QUEUE_CH = (process.env.OWNER_QUEUE_CH_DIRECTORS || 'true').toLowerCase() === 'true';
const AUTO_QUEUE_COMPANY_DISCOVERY =
  (process.env.OWNER_QUEUE_COMPANY_DISCOVERY || 'true').toLowerCase() === 'true';

export type OwnerDiscoveryJob = {
  address: AddressInput;
  rootJobId?: string;
  parentJobId?: string;
  requestSource?: string;
  metadata?: Record<string, any>;
  allowCorporateQueue?: boolean;
};

function normalizeName(name: string): string {
  return (name || '')
    .toLowerCase()
    .replace(/^(mr|mrs|ms|miss|dr|prof|sir|dame|lady|lord)\b\.?\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function slugifyName(name: string): string {
  const normalized = normalizeName(name);
  const slug = normalized.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return slug || 'unknown';
}

function deriveNameParts(occ: OccupantRecord): { first: string; middle: string; last: string } {
  const parsed = parseOfficerName(occ.fullName ?? '') || { first: '', middle: '', last: '' };
  const first = (occ.firstName || parsed.first || '').trim();
  const middle = (parsed.middle || '').trim();
  const last = (occ.lastName || parsed.last || '').trim();
  return { first, middle, last };
}

async function logToJobAndRoot(
  jobId: string,
  rootJobId: string | undefined,
  level: 'info' | 'warn' | 'error' | 'debug',
  message: string,
  data: Record<string, any>,
  rootExtra?: Record<string, any>
): Promise<void> {
  await logEvent(jobId, level, message, data);
  if (rootJobId) {
    const rootPayload = { childJobId: jobId, ...data, ...(rootExtra || {}) };
    await logEvent(rootJobId, level, message, rootPayload);
  }
}

async function upsertProperty(jobId: string, payload: OwnerDiscoveryJob): Promise<{ id: number }> {
  const addr = payload.address;
  const { rows } = await query<{ id: number }>(
    `INSERT INTO owner_properties (job_id, root_job_id, address_line1, address_line2, city, postcode, country, status)
     VALUES ($1,$2,$3,$4,$5,$6,$7,'running')
     ON CONFLICT (job_id) DO UPDATE
       SET root_job_id = EXCLUDED.root_job_id,
           address_line1 = EXCLUDED.address_line1,
           address_line2 = EXCLUDED.address_line2,
           city = EXCLUDED.city,
           postcode = EXCLUDED.postcode,
           country = EXCLUDED.country,
           updated_at = now(),
           status = 'running'
     RETURNING id`,
    [
      jobId,
      payload.rootJobId || null,
      addr.line1,
      addr.line2 || null,
      addr.city || null,
      addr.postcode,
      addr.country || 'GB',
    ]
  );
  return rows[0];
}

async function clearCandidates(propertyId: number) {
  await query(`DELETE FROM owner_signals WHERE candidate_id IN (SELECT id FROM owner_candidates WHERE property_id = $1)`, [propertyId]);
  await query(`DELETE FROM owner_candidates WHERE property_id = $1`, [propertyId]);
}

function normalizeForMatch(input: string): string {
  return (input || '')
    .toLowerCase()
    .replace(/\(.*?\)/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokens(value: string): string[] {
  return normalizeForMatch(value).split(' ').filter(Boolean);
}

// --- Address parsing helpers for corporate/Neon matching ---
const SAON_LABELS = /\b(flat|apartment|apt|suite|unit|room|block|floor|fl|lvl|level)\b/i;

function extractPaonFromFreeform(s?: string | null): string | null {
  const str = String(s || '').toLowerCase();
  if (!str) return null;
  // collect all number tokens (e.g., "2", "2a", "12b")
  const nums = str.match(/\b\d+[a-z]?\b/g);
  if (!nums || !nums.length) return null;
  // detect an SAON like "flat 2", "apt 3" so we can ignore it for PAON
  const saon = extractSaonFromFreeform(str);
  const paon = nums.find(n => n !== saon) || null;
  return paon || null;
}

function extractSaonFromFreeform(s?: string | null): string | null {
  const str = String(s || '').toLowerCase();
  if (!str) return null;
  const m = str.match(/\b(flat|apartment|apt|suite|unit|room|fl|floor|lvl|level)\b\s*(\d+[a-z]?)/i);
  return m ? m[2].toLowerCase() : null;
}

function extractPaonFromAddressInput(addr: AddressInput): string | null {
  // Prefer a leading number token from line1; if line1 is SAON (e.g., "Flat 2, 9 Waterfront Mews"), pick the next number from line1+line2
  const l1 = String(addr.line1 || '').toLowerCase();
  const l2 = String(addr.line2 || '').toLowerCase();
  // if line1 begins with SAON label, ignore that number as PAON
  const saon = extractSaonFromFreeform(`${addr.unit || ''} ${l1}`);
  const all = `${l1} ${l2}`.trim();
  const nums = all.match(/\b\d+[a-z]?\b/g) || [];
  if (nums.length) {
    const candidate = nums.find(n => n !== saon);
    if (candidate) return candidate;
  }
  // fallback: first number in line1 or line2
  const m1 = l1.match(/\b\d+[a-z]?\b/);
  if (m1) return m1[0];
  const m2 = l2.match(/\b\d+[a-z]?\b/);
  return m2 ? m2[0] : null;
}

function extractSaonFromAddressInput(addr: AddressInput): string | null {
  // Prefer explicit unit; otherwise parse SAON label from line1
  const unit = String(addr.unit || '').toLowerCase();
  if (unit && SAON_LABELS.test(unit)) {
    const m = unit.match(/\b\d+[a-z]?\b/);
    if (m) return m[0];
  }
  const l1 = String(addr.line1 || '').toLowerCase();
  const m2 = l1.match(/\b(flat|apartment|apt|suite|unit|room|fl|floor|lvl|level)\b\s*(\d+[a-z]?)/i);
  return m2 ? m2[2].toLowerCase() : null;
}

function pricePaidCandidateVariants(tx: PricePaidTransaction): Array<{ raw: string; normalized: string; tokens: Set<string> }> {
  const variants = new Set<string>();
  const paon = (tx.paon || '').toString();
  const saon = (tx.saon || '').toString();
  const street = (tx.street || '').toString();
  const town = (tx.town || '').toString();

  const add = (val: string) => {
    const trimmed = val.trim();
    if (!trimmed) return;
    variants.add(trimmed);
  };

  if (paon && street) add(`${paon} ${street}`);
  if (saon && paon && street) add(`${saon} ${paon} ${street}`);
  if (saon && street) add(`${saon} ${street}`);
  if (street) add(street);
  if (paon) add(paon);
  if (saon) add(saon);
  if (street && town) add(`${street} ${town}`);

  return Array.from(variants)
    .map((val) => {
      const normalized = normalizeForMatch(val);
      return {
        raw: val,
        normalized,
        tokens: new Set(tokens(normalized)),
      };
    })
    .filter((entry) => entry.tokens.size > 0);
}

function choosePricePaidTransaction(
  normalized: NormalizedAddress,
  transactions: PricePaidTransaction[]
): {
  transaction: PricePaidTransaction;
  score: number;
  sharedTokens: number;
  targetVariant: string;
  candidateVariant: string;
  unitExact?: boolean;
  paonExact?: boolean;
  reason?: string;
} | null {
  if (!transactions.length) return null;

  const targetPaon = (normalized.paon || '').toString().toLowerCase();
  const targetSaon = (normalized.saon_identifier || normalized.saon || '').toString().toLowerCase();

  const targetVariants = buildTargetVariants(normalized).map((value) => ({
    raw: value,
    tokens: new Set(tokens(value)),
    numericTokens: tokens(value).filter((token) => /\d/.test(token)),
  }));
  if (!targetVariants.length) return null;

  type MatchResult = {
    transaction: PricePaidTransaction;
    score: number;
    sharedTokens: number;
    targetVariant: string;
    candidateVariant: string;
    timestamp: number;
    unitExact?: boolean;
    paonExact?: boolean;
    reason?: string;
  };

  const parseTimestamp = (value?: string): number => {
    if (!value) return Number.NEGATIVE_INFINITY;
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
    const match = value.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) {
      const year = Number(match[1]);
      const month = Number(match[2]) - 1;
      const day = Number(match[3]);
      return Date.UTC(year, month, day);
    }
    return Number.NEGATIVE_INFINITY;
  };

  let best: MatchResult | null = null;

  // Helper to evaluate a single candidate variant against a target variant with strict PAON/SAON logic
  const consider = (
    tx: PricePaidTransaction,
    candidateRaw: string,
    candidateTokens: Set<string>,
    t: { raw: string; tokens: Set<string>; numericTokens: string[] },
    timestamp: number
  ) => {
    // Hard requirements when input has a unit (SAON)
    if (targetSaon) {
      const candSaon = (tx.saon || '').toString().toLowerCase();
      if (!candSaon || candSaon !== targetSaon) {
        return; // reject if target has SAON but candidate does not match exactly
      }
    }

    // Hard requirements for PAON when present on target
    if (targetPaon) {
      const candPaon = (tx.paon || '').toString().toLowerCase();
      if (!candPaon || candPaon !== targetPaon) {
        return; // reject if target has PAON but candidate does not match exactly
      }
    }

    // Token subset scoring as a tie-breaker only (after strict checks above)
    let shared = 0;
    for (const token of t.tokens) {
      if (candidateTokens.has(token)) shared += 1;
    }
    if (!shared) return;

    // Preserve the old numeric token discipline for cases with no PAON/SAON in the prompt
    if (!targetPaon && !targetSaon && t.numericTokens.length) {
      let numericMatch = true;
      for (const token of t.numericTokens) {
        if (!candidateTokens.has(token)) {
          numericMatch = false;
          break;
        }
      }
      if (!numericMatch) return;
    }

    const missing = t.tokens.size - shared;
    const extra = candidateTokens.size - shared;
    const score = shared * 10 - missing * 5 - extra;

    const res: MatchResult = {
      transaction: tx,
      score,
      sharedTokens: shared,
      targetVariant: t.raw,
      candidateVariant: candidateRaw,
      timestamp,
      unitExact: Boolean(targetSaon),
      paonExact: Boolean(targetPaon),
      reason: targetSaon || targetPaon ? 'strict_unit_paon_match' : 'token_subset',
    };

    if (
      !best ||
      res.score > best.score ||
      (res.score === best.score && timestamp > best.timestamp)
    ) {
      best = res;
    }
  };

  for (const tx of transactions) {
    const variants = pricePaidCandidateVariants(tx);
    if (!variants.length) continue;
    const timestamp = parseTimestamp(tx.date);

    for (const cand of variants) {
      if (!cand.tokens.size) continue;
      for (const t of targetVariants) {
        if (!t.tokens.size) continue;
        consider(tx, cand.raw, cand.tokens, t, timestamp);
      }
    }
  }

  if (!best) {
    return null;
  }
  const finalized = best as MatchResult;
  return {
    transaction: finalized.transaction,
    score: finalized.score,
    sharedTokens: finalized.sharedTokens,
    targetVariant: finalized.targetVariant,
    candidateVariant: finalized.candidateVariant,
    unitExact: finalized.unitExact,
    paonExact: finalized.paonExact,
    reason: finalized.reason,
  };
}

function extractSaleYear(dateStr?: string): number | undefined {
  if (!dateStr) return undefined;
  const match = dateStr.match(/^(\d{4})/);
  if (!match) return undefined;
  const year = Number(match[1]);
  return Number.isFinite(year) ? year : undefined;
}

function buildTargetVariants(target: NormalizedAddress): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  const add = (fragment?: string) => {
    const normalized = normalizeForMatch(fragment || '');
    if (normalized.length > 2 && !seen.has(normalized)) {
      seen.add(normalized);
      out.push(normalized);
    }
  };

  (target.variants || []).forEach(add);
  add(target.full_address);
  add(target.street_address);
  add(target.street_name);
  add([target.paon, target.street_name].filter(Boolean).join(' '));
  add([target.saon, target.paon, target.street_name].filter(Boolean).join(' '));
  add([target.building_name, target.street_name].filter(Boolean).join(' '));
  add([target.street_name, target.town].filter(Boolean).join(' '));
  return out;
}

function extractCandidateVariants(record: CorporateOwnerRecord): Array<{ raw: string; normalized: string }> {
  const variants = new Set<string>();
  const keyPrefix = record.addressKey?.split('|')[0] || '';
  if (keyPrefix) variants.add(keyPrefix);

  const raw = record.raw || {};
  const propertyAddress = raw?.property_address || raw?.propertyAddress || raw?.address || raw?.Property_Address;
  if (typeof propertyAddress === 'string' && propertyAddress) {
    variants.add(propertyAddress);
    propertyAddress.split(',').forEach((part: string) => variants.add(part));
  }

  const addressLines = Array.isArray(raw?.proprietor?.address_lines) ? raw.proprietor.address_lines : [];
  addressLines.forEach((line: any) => {
    if (typeof line === 'string' && line.trim()) variants.add(line);
  });

  return Array.from(variants)
    .map((value) => ({
      raw: value,
      normalized: normalizeForMatch(value),
    }))
    .filter((entry) => entry.normalized.length > 2);
}

function chooseCorporateMatch(
  candidates: CorporateOwnerRecord[],
  address: AddressInput,
  normalized: NormalizedAddress
) {
  const targetVariants = buildTargetVariants(normalized).map((value) => ({
    raw: value,
    tokens: new Set(tokens(value)),
  }));
  console.log('Target variants for address in ownerDiscovery:', targetVariants.map((v) => v.raw));
  if (!targetVariants.length) return null;

  // Extract target PAON/SAON (house vs unit)
  const targetPaon = (normalized.paon || extractPaonFromAddressInput(address) || '').toLowerCase();
  const targetSaon = (normalized.saon_identifier || normalized.saon || extractSaonFromAddressInput(address) || '').toLowerCase();

  type MatchResult = {
    record: CorporateOwnerRecord;
    extraTokens: number;
    targetSize: number;
    targetVariant: string;
    candidateVariant: string;
    unitExact?: boolean;
    paonExact?: boolean;
    canonicalMatched?: boolean;
    targetCanonical?: string;
    candidateCanonical?: string;
    // diagnostics
    resolvedCandidateSaon?: string | null;
    resolvedCandidatePaon?: string | null;
    candidateSaonSet?: string[];
    reason?: string;
  };

  let best: MatchResult | null = null;

  for (const record of candidates) {
    const normalizedCandidate = normalizeCorporateRecordAddress(record);

    // Collect candidate variants (deduped)
    const variantMap = new Map<string, { raw: string; tokens: Set<string> }>();
    for (const entry of extractCandidateVariants(record)) {
      const normalizedValue = normalizeForMatch(entry.raw);
      if (!variantMap.has(normalizedValue)) {
        variantMap.set(normalizedValue, {
          raw: entry.raw,
          tokens: new Set(tokens(normalizedValue)),
        });
      }
    }
    if (normalizedCandidate.variants) {
      for (const variant of normalizedCandidate.variants) {
        const normalizedValue = normalizeForMatch(variant);
        if (!variantMap.has(normalizedValue)) {
          variantMap.set(normalizedValue, {
            raw: variant,
            tokens: new Set(tokens(normalizedValue)),
          });
        }
      }
    }
    const candidateVariants = Array.from(variantMap.values());

    // Resolve SAON from any source on the candidate (normalized fields or any variant string)
    const candidateSaonField = (normalizedCandidate.saon_identifier || normalizedCandidate.saon || '').toLowerCase();
    const candidateSaonSet = new Set<string>();
    if (candidateSaonField) candidateSaonSet.add(candidateSaonField);
    for (const v of candidateVariants) {
      const vSaon = extractSaonFromFreeform(v.raw);
      if (vSaon) candidateSaonSet.add(vSaon.toLowerCase());
    }

    if (!candidateVariants.length) continue;

    // Canonical-key perfect match remains a hard accept
    const candidateCanonical = normalizedCandidate.canonical_key || undefined;
    if (candidateCanonical && normalized.canonical_key && candidateCanonical === normalized.canonical_key) {
      return {
        record,
        extraTokens: 0,
        targetSize: targetVariants[0]?.tokens.size || 0,
        targetVariant: normalized.canonical_key!,
        candidateVariant: candidateCanonical,
        unitExact: true,
        paonExact: true,
        canonicalMatched: true,
        targetCanonical: normalized.canonical_key!,
        candidateCanonical,
        resolvedCandidateSaon: targetSaon || null,
        resolvedCandidatePaon: targetPaon || null,
        candidateSaonSet: Array.from(candidateSaonSet),
        reason: 'canonical_key_equal',
      };
    }

    // If the target has an SAON, be strict:
    //  - If ANY SAON can be resolved for the candidate and it != target, skip this record entirely.
    if (targetSaon && candidateSaonSet.size > 0 && !candidateSaonSet.has(targetSaon)) {
      continue;
    }

    for (const candidate of candidateVariants) {
      if (!candidate.tokens.size) continue;

      for (const target of targetVariants) {
        if (!target.tokens.size) continue;

        // Require target tokens subset of candidate tokens
        let missing = 0;
        for (const token of target.tokens) {
          if (!candidate.tokens.has(token)) { missing = 1; break; }
        }
        if (missing) continue;

        // Resolve PAON/SAON from the current candidate variant
        const candPaon = extractPaonFromFreeform(candidate.raw);
        const candSaon = (extractSaonFromFreeform(candidate.raw) || candidateSaonField || '').toLowerCase();

        // PAON must agree if both sides have one
        if (targetPaon && candPaon && targetPaon !== candPaon) continue;

        // If the target has an SAON, then REQUIRE unitExact (the variant must carry the same SAON)
        const unitExact = Boolean(targetSaon && candSaon && targetSaon === candSaon);
        if (targetSaon && !unitExact) {
          // do not allow a match via a candidate variant that omits SAON when target has SAON
          continue;
        }

        const paonExact = Boolean(targetPaon && candPaon && targetPaon === candPaon);
        const extraTokens = candidate.tokens.size - target.tokens.size;

        if (
          !best ||
          (unitExact && !best.unitExact) ||
          (paonExact && !best.paonExact) ||
          extraTokens < best.extraTokens ||
          (extraTokens === best.extraTokens && target.tokens.size > best.targetSize)
        ) {
          best = {
            record,
            extraTokens,
            targetSize: target.tokens.size,
            targetVariant: target.raw,
            candidateVariant: candidate.raw,
            unitExact,
            paonExact,
            canonicalMatched: false,
            targetCanonical: normalized.canonical_key || undefined,
            candidateCanonical: candidateCanonical,
            resolvedCandidateSaon: candSaon || null,
            resolvedCandidatePaon: candPaon || null,
            candidateSaonSet: Array.from(candidateSaonSet),
            reason: unitExact ? 'unit_exact' : (paonExact ? 'paon_exact' : 'token_subset'),
          };
        }
      }
    }
  }

  return best;
}

function occupantRecordKey(rec: OccupantRecord): string {
  const name = rec.fullName || `${rec.firstName || ''} ${rec.lastName || ''}`;
  return normalizeName(name);
}

function mergeCompanyRelations(
  existing?: OccupantCompanyRelation[] | null,
  incoming?: OccupantCompanyRelation[] | null
): OccupantCompanyRelation[] | undefined {
  const combined = [
    ...(Array.isArray(existing) ? existing : []),
    ...(Array.isArray(incoming) ? incoming : []),
  ];
  if (!combined.length) return existing || incoming || undefined;
  const deduped: OccupantCompanyRelation[] = [];
  const keyFor = (rel: OccupantCompanyRelation) => {
    const role = rel.role || 'unknown';
    const company = (rel.companyNumber || '').toLowerCase();
    const officer = (rel.officerId || '').toLowerCase();
    return `${role}:${company}:${officer}`;
  };
  const seen = new Set<string>();
  for (const rel of combined) {
    if (!rel) continue;
    const key = keyFor(rel);
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push({
      role: rel.role,
      companyNumber: rel.companyNumber,
      companyName: rel.companyName,
      officerId: rel.officerId,
      appointedOn: rel.appointedOn ?? null,
      source: rel.source,
    });
  }
  return deduped;
}

function addOrMergeOccupant(
  map: Map<string, OccupantRecord>,
  incoming: OccupantRecord
): { added: boolean; merged: boolean; key: string } {
  const key = occupantRecordKey(incoming);
  const existing = map.get(key);
  if (!existing) {
    map.set(key, incoming);
    return { added: true, merged: false, key };
  }

  const firstSeenA = existing.firstSeenYear ?? Number.POSITIVE_INFINITY;
  const firstSeenB = incoming.firstSeenYear ?? Number.POSITIVE_INFINITY;
  const lastSeenA = existing.lastSeenYear ?? Number.NEGATIVE_INFINITY;
  const lastSeenB = incoming.lastSeenYear ?? Number.NEGATIVE_INFINITY;

  const merged: OccupantRecord = {
    firstName: existing.firstName || incoming.firstName,
    lastName: existing.lastName || incoming.lastName,
    fullName: (existing.fullName?.length || 0) >= (incoming.fullName?.length || 0)
      ? existing.fullName
      : incoming.fullName,
    ageBand: existing.ageBand ?? incoming.ageBand,
    birthYear: existing.birthYear ?? incoming.birthYear,
    firstSeenYear: isFinite(Math.min(firstSeenA, firstSeenB))
      ? Math.min(firstSeenA, firstSeenB)
      : (existing.firstSeenYear ?? incoming.firstSeenYear),
    lastSeenYear: isFinite(Math.max(lastSeenA, lastSeenB))
      ? Math.max(lastSeenA, lastSeenB)
      : (existing.lastSeenYear ?? incoming.lastSeenYear),
    dataSources: Array.from(new Set([...(existing.dataSources || []), ...(incoming.dataSources || [])])),
    indicators: Array.from(new Set([...(existing.indicators || []), ...(incoming.indicators || [])])),
    companyRelations: mergeCompanyRelations(existing.companyRelations, incoming.companyRelations),
  };

  map.set(key, merged);
  return { added: false, merged: true, key };
}

async function enqueuePersonLinkedInJob(opts: {
  ownerJobId: string;
  rootJobId?: string;
  occupant: OccupantRecord;
  propertyAddress: string;
  address: AddressInput;
  registerSource?: string | null;
  score: number;
  rank: number;
  latestSaleYear?: number | null;
  reason: string;
}): Promise<void> {
  const { ownerJobId, rootJobId, occupant, propertyAddress, address, registerSource, score, rank, latestSaleYear, reason } = opts;
  const { first, middle, last } = deriveNameParts(occupant);
  const fullName = occupant.fullName || [first, middle, last].filter(Boolean).join(' ');
  const slug = slugifyName(fullName);
  const personJobId = `owner-person:${ownerJobId}:${slug}`;
  const dob = typeof occupant.birthYear === 'number' && Number.isFinite(occupant.birthYear) ? String(occupant.birthYear) : '';
  const companyNumbers = Array.from(
    new Set((occupant.companyRelations || [])
      .map((rel) => (rel?.companyNumber || '').trim())
      .filter(Boolean))
  );
  const companyNames = Array.from(
    new Set((occupant.companyRelations || [])
      .map((rel) => (rel?.companyName || '').trim())
      .filter(Boolean))
  );
  const payload = {
    person: {
      firstName: first,
      middleNames: middle,
      lastName: last,
      dob,
    },
    context: {
      propertyAddress,
      postcode: address.postcode,
      unit: address.unit || null,
      city: address.city,
      openRegister: {
        source: registerSource || null,
        firstSeenYear: occupant.firstSeenYear ?? null,
        lastSeenYear: occupant.lastSeenYear ?? null,
      },
      ownerDiscovery: {
        jobId: ownerJobId,
        score,
        rank,
        dataSources: occupant.dataSources || [],
        indicators: occupant.indicators || [],
        companyNumbers,
        companyNames,
        latestSaleYear: latestSaleYear ?? null,
        reason,
      },
    },
    rootJobId: rootJobId || ownerJobId,
    parentJobId: ownerJobId,
    requestSource: 'owner-discovery',
  };

  try {
    await query(
      `INSERT INTO job_progress(job_id, queue, name, status, data, root_job_id, parent_job_id, request_source)
         VALUES ($1,'person-linkedin','discover','pending',$2,$3,$4,$5)
         ON CONFLICT (job_id)
         DO UPDATE SET status='pending',
                       data=$2,
                       root_job_id=$3,
                       parent_job_id=$4,
                       request_source=$5,
                       updated_at=now()`,
      [personJobId, JSON.stringify(payload), rootJobId || ownerJobId, ownerJobId, 'owner-discovery']
    );
  } catch (err) {
    await logToJobAndRoot(ownerJobId, rootJobId, 'warn', 'Failed to upsert LinkedIn job progress', {
      personJobId,
      error: String(err),
    });
  }

  try {
    await personQ.add('discover', payload, {
      jobId: personJobId,
      attempts: 5,
      backoff: { type: 'exponential', delay: 2000 },
    });
    await logToJobAndRoot(ownerJobId, rootJobId, 'info', 'Enqueued LinkedIn enrichment for owner candidate', {
      personJobId,
      occupantName: fullName,
      score,
      rank,
      companyNumbers,
      reason,
    });
  } catch (err) {
    await logToJobAndRoot(ownerJobId, rootJobId, 'error', 'Failed to enqueue LinkedIn enrichment for owner candidate', {
      personJobId,
      occupantName: fullName,
      error: String(err),
    });
  }
}

async function enqueueChAppointmentsForOwner(opts: {
  ownerJobId: string;
  rootJobId?: string;
  occupant: OccupantRecord;
  propertyAddress: string;
  relations?: OccupantCompanyRelation[];
  reason: string;
  allowQueue: boolean;
}): Promise<void> {
  const { ownerJobId, rootJobId, occupant, propertyAddress, relations, reason, allowQueue } = opts;
  if (!allowQueue) {
    await logToJobAndRoot(ownerJobId, rootJobId, 'info', 'Skipping CH appointment enqueue for owner-director (flag disabled)', {
      occupantName: occupant.fullName,
      reason,
    });
    return;
  }

  const targets = (relations || []).filter((rel) => rel?.role === 'director' && (rel.companyNumber || '').trim());
  if (!targets.length) {
    await logToJobAndRoot(ownerJobId, rootJobId, 'info', 'No director relations available for CH enqueue', {
      occupantName: occupant.fullName,
      reason,
    });
    return;
  }

  const seen = new Set<string>();
  const scheduled: Array<{ companyNumber: string; chJobId: string; officerId: string | null }> = [];
  const { first, middle, last } = deriveNameParts(occupant);
  for (const rel of targets) {
    const companyNumber = (rel.companyNumber || '').trim();
    if (!companyNumber) continue;
    const dedupeKey = `${companyNumber}:${(rel.officerId || '').toLowerCase()}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    const slug = slugifyName(occupant.fullName || `${first} ${last}`);
    const chJobId = `owner-ch:${ownerJobId}:${companyNumber}:${slug}`;
    const payload: Record<string, any> = {
      companyNumber,
      firstName: first,
      lastName: last,
      rootJobId: rootJobId || ownerJobId,
      source: 'owner-discovery-owner-linked',
      ownerJobId,
      occupantName: occupant.fullName,
      relation: rel.role,
      officerId: rel.officerId || null,
      propertyAddress,
      parentJobId: ownerJobId,
      requestSource: 'owner-discovery',
    };
    if (middle) payload.middleName = middle;
    try {
      await chQ.add('fetch', payload, {
        jobId: chJobId,
        attempts: 5,
        backoff: { type: 'exponential', delay: 1000 },
      });
      scheduled.push({ companyNumber, chJobId, officerId: rel.officerId || null });
    } catch (err) {
      await logToJobAndRoot(ownerJobId, rootJobId, 'error', 'Failed to enqueue CH appointments for owner-director', {
        chJobId,
        companyNumber,
        error: String(err),
      });
    }
  }

  if (scheduled.length) {
    await logToJobAndRoot(ownerJobId, rootJobId, 'info', 'Enqueued Companies House appointment lookups for owner-director', {
      occupantName: occupant.fullName,
      scheduled,
      reason,
    });
  }
}

// Helper to split SAON/PAON from address_line_1 or premises (local copy)
function splitSaonPaonLine(line1?: string, premises?: string): { unit?: string; line1?: string } {
  const s1 = String(line1 || '').trim();
  const pre = String(premises || '').trim();
  const SAON = /(flat|apartment|apt|suite|unit|room|block|floor|fl|lvl|level)\s*(\d+[a-z]?)/i;
  const numToken = /\b\d+[a-z]?\b/i;
  if (pre && SAON.test(pre)) {
    return { unit: pre, line1: s1 || undefined };
  }
  if (!s1) return { unit: pre || undefined, line1: undefined };
  const m = s1.match(SAON);
  if (m) {
    let after = s1.slice(m.index! + m[0].length).trim();
    // Handle optional comma right after SAON, e.g., "Flat 2, 9 Waterfront Mews"
    if (after.startsWith(',')) {
      after = after.slice(1).trim();
    }
    const n = after.match(numToken);
    if (n) {
      const paonAndRest = after.slice(n.index!).trim();
      return { unit: m[0], line1: paonAndRest };
    }
  }
  if (numToken.test(s1)) return { unit: pre || undefined, line1: s1 };
  return { unit: pre || undefined, line1: s1 };
}

function addressInputFromChAddress(addr: any): AddressInput | null {
  if (!addr) return null;
  const pick = (...candidates: Array<any>): string | undefined => {
    for (const c of candidates) {
      if (typeof c === 'string') {
        const trimmed = c.trim();
        if (trimmed) return trimmed;
      }
    }
    return undefined;
  };
  const split = splitSaonPaonLine(addr.address_line_1 || addr.premises, addr.premises);
  const unit = pick(addr.saon, addr.sub_building_name, addr.sub_building, addr.po_box, addr.care_of, split.unit);
  const buildingName = pick(addr.building_name, addr.organisation_name, addr.property_name, addr.care_of);
  const line1Parts = [split.line1 || addr.premises, split.line1 ? undefined : addr.address_line_1].filter(Boolean);
  let line1 = line1Parts.length
    ? line1Parts.map((part: any) => String(part).trim()).filter(Boolean).join(' ')
    : pick(split.line1, addr.address_line_1, addr.premises, addr.street_address);
  let line2 = pick(addr.address_line_2, addr.street_address, addr.address_line_3, addr.thoroughfare);
  const city = pick(addr.locality, addr.town, addr.post_town, addr.region);
  const postcode = pick(addr.postal_code, addr.postcode, addr.post_code);
  if (!line1 && line2) {
    line1 = line2;
    line2 = undefined;
  }
  if (!line1 && !postcode) return null;
  return {
    unit,
    buildingName,
    line1: line1 || '',
    line2: line2,
    city: city,
    postcode: postcode || '',
    country: pick(addr.country, addr.country_of_residence) || 'GB',
  } as AddressInput;
}

function chPersonalAddressMatchesPrompt(chAddr: any, prompt: AddressInput): boolean {
  const ai = addressInputFromChAddress(chAddr);
  if (!ai) return false;
  try {
    return addressKey(ai) === addressKey(prompt);
  } catch {
    return false;
  }
}

function occupantFromCompanyPerson(
  name: string,
  opts: {
    source: 'director' | 'psc';
    company: CompanyAddressHit;
    officer?: CompanyOfficerRecord;
    psc?: CompanyPscRecord;
  }
): OccupantRecord | null {
  if (!name?.trim()) return null;
  const parsed = parseOfficerName(name);
  const fullName = [parsed.first, parsed.middle, parsed.last].filter(Boolean).join(' ').trim() || name.trim();
  const appointedYear = opts.officer?.appointedOn?.slice(0, 4);
  const firstSeenYear = appointedYear && /^\d{4}$/.test(appointedYear) ? Number(appointedYear) : undefined;
  const dataSource = opts.source === 'director' ? 'companies_house_director' : 'companies_house_psc';
  const indicators = ['companies_house'];
  const sources = [dataSource];
  if (opts.company.companyNumber) {
    sources.push(`ch_company_${opts.company.companyNumber}`);
  }
  const relations: OccupantCompanyRelation[] = [];
  const baseRelation: OccupantCompanyRelation = {
    role: opts.source,
    companyNumber: opts.company.companyNumber || undefined,
    companyName: opts.company.companyName || undefined,
    officerId: opts.officer?.officerId || null,
    appointedOn: opts.officer?.appointedOn || null,
    ceasedOn: opts.psc?.ceasedOn || null,
    source: opts.company.companyNumber ? `ch_company_${opts.company.companyNumber}` : undefined,
  };
  relations.push(baseRelation);
  return {
    firstName: parsed.first || '',
    lastName: parsed.last || '',
    fullName,
    ageBand: undefined,
    birthYear: undefined,
    firstSeenYear,
    lastSeenYear: undefined,
    dataSources: Array.from(new Set(sources)),
    indicators,
    companyRelations: relations,
  };
}

export default new Worker<OwnerDiscoveryJob>(
  'owner-discovery',
  async (job) => {
    const rawPayload = job.data;
    const jobId = job.id as string;
    const inheritedRoot =
      typeof rawPayload.rootJobId === 'string' && rawPayload.rootJobId.trim()
        ? rawPayload.rootJobId.trim()
        : null;
    const rootJobId = inheritedRoot || jobId;
    const baseAddr = rawPayload.address;
    if (!baseAddr?.line1 || !baseAddr?.city || !baseAddr?.postcode) {
      throw new Error('Address requires line1, city, and postcode');
    }
    const line2 = typeof baseAddr.line2 === 'string' ? baseAddr.line2.trim() : '';
    const address: AddressInput = {
      unit: baseAddr.unit?.trim() || undefined,
      buildingName: baseAddr.buildingName?.trim() || undefined,
      line1: baseAddr.line1.trim(),
      line2: line2 ? line2 : undefined,
      city: baseAddr.city.trim(),
      postcode: baseAddr.postcode.trim(),
      country: baseAddr.country?.trim() || baseAddr.country || 'GB',
    };
    const targetNormalized = normalizeAddressFragments({
      unit: address.unit,
      saon: address.unit,
      buildingName: address.buildingName,
      lines: [address.line1, address.line2, address.city].filter((value): value is string => typeof value === 'string' && value.trim().length > 0),
      town: address.city,
      postcode: address.postcode,
      countryCode: address.country,
      fullAddress: prettyAddress(address),
      source: 'owner_input',
    });
    const requestSource =
      typeof rawPayload.requestSource === 'string' && rawPayload.requestSource.trim()
        ? rawPayload.requestSource.trim()
        : 'owner-discovery';
    const payload = {
      ...rawPayload,
      rootJobId,
      parentJobId: rawPayload.parentJobId || undefined,
      requestSource,
      address,
      normalizedAddress: targetNormalized,
    };
    await startJob({
      jobId,
      queue: 'owner-discovery',
      name: job.name,
      payload,
      rootJobId,
      parentJobId: rawPayload.parentJobId || undefined,
      requestSource,
    });
    const pretty = prettyAddress(address);

    try {
      const property = await upsertProperty(jobId, payload);
      const propertyId = property.id;
      await logEvent(jobId, 'info', 'Owner property job started', {
        prettyAddress: pretty,
        unit: baseAddr.unit?.trim() || undefined,
        buildingName: baseAddr.buildingName?.trim() || undefined,
        line1: baseAddr.line1.trim(),
        line2: line2 ? line2 : undefined,
        city: baseAddr.city.trim(),
        postcode: baseAddr.postcode.trim(),
        country: baseAddr.country?.trim() || baseAddr.country || 'GB',
        addressKey: addressKey(address),
        normalized: {
          canonical: targetNormalized.canonical_key,
          saon: targetNormalized.saon || null,
          paon: targetNormalized.paon || null,
          street: targetNormalized.street_name || targetNormalized.street_address || null,
          town: targetNormalized.town || null,
          postcode: targetNormalized.postcode || null,
        },
        propertyId,
      });
      if (rootJobId) {
        await logEvent(rootJobId, 'info', 'Owner pipeline started', { childJobId: jobId, address: pretty });
      }

      // Step 1: Land Registry corporate ownership check
      let corporate: CorporateOwnerMatch | null = null;
      const postcodeHits = await searchCorporateOwnersByPostcode(address.postcode);
      console.log('Corporate postcode hits in ownerDiscovery:', postcodeHits.length, postcodeHits.slice(0, 5).map((h) => h.ownerName));

      if (postcodeHits.length) {
        const best = chooseCorporateMatch(postcodeHits, address, targetNormalized);
        await logEvent(jobId, 'info', 'Corporate postcode search', {
          postcode: address.postcode,
          hitCount: postcodeHits.length,
          hits: postcodeHits.slice(0, 8).map((hit) => ({
            ownerName: hit.ownerName,
            companyNumber: hit.companyNumber || null,
            addressKey: hit.addressKey,
            propertyAddress:
              typeof hit.raw?.property_address === 'string'
                ? hit.raw.property_address
                : hit.raw?.propertyAddress || null,
          })),
          matched: best
            ? {
                ownerName: best.record.ownerName,
                companyNumber: best.record.companyNumber || null,
                addressKey: best.record.addressKey,
                datasetLabel: best.record.datasetLabel || null,
                targetVariant: best.targetVariant,
                candidateVariant: best.candidateVariant,
                extraTokens: best.extraTokens,
                targetCanonical: targetNormalized.canonical_key,
                targetPaon: targetNormalized.paon || null,
                targetSaon: targetNormalized.saon || null,
                candidateCanonical: best.candidateCanonical || null,
                canonicalMatched: best.canonicalMatched ?? false,
                candidateResolvedSaon: best.resolvedCandidateSaon || null,
                candidateResolvedPaon: best.resolvedCandidatePaon || null,
                candidateSaonSet: best.candidateSaonSet || null,
                reason: best.reason || null,
              }
            : null,
        });
        if (best) {
          corporate = {
            matchType: (best.unitExact || !targetNormalized.saon) && best.extraTokens === 0 ? 'exact' : 'postcode',
            ownerName: best.record.ownerName,
            companyNumber: best.record.companyNumber,
            source: best.record.datasetLabel || 'land_registry_dataset',
            raw: best.record.raw,
          };
        } else {
          await logEvent(jobId, 'info', 'Corporate postcode match not found', {
            postcode: address.postcode,
            hitCount: postcodeHits.length,
          });
        }
      } else {
        await logEvent(jobId, 'info', 'Corporate postcode search', {
          postcode: address.postcode,
          hitCount: 0,
        });
      }

      if (!corporate && postcodeHits.length === 0) {
        corporate = await findCorporateOwner(address);
      }

      if (corporate) {
        await query(
          `UPDATE owner_properties SET owner_type = 'company', status = 'corporate', corporate_owner = $2, updated_at = now()
           WHERE id = $1`,
          [propertyId, JSON.stringify(corporate)]
        );
        await logEvent(jobId, 'info', 'Corporate owner identified', corporate);
        if (rootJobId) {
          await logEvent(rootJobId, 'info', 'Corporate owner identified', { childJobId: jobId, corporate });
        }
        if (AUTO_QUEUE_CH && corporate.companyNumber) {
          const chJobId = `ch-owner:${corporate.companyNumber}`;
          await chQ.add(
            'fetch',
            {
              companyNumber: corporate.companyNumber,
              // Propagate the original root so all downstream jobs/logs live under the same request
              rootJobId: rootJobId || jobId,
              // Helpful context for downstream logging/debugging
              source: 'owner-discovery',
              propertyAddress: pretty,
              parentJobId: jobId,
              requestSource: 'owner-discovery',
            },
            { jobId: chJobId, attempts: 3, backoff: { type: 'exponential', delay: 1000 } }
          );
          await logEvent(jobId, 'info', 'Enqueued CH director/PSC lookup with root propagation', {
            companyNumber: corporate.companyNumber,
            chJobId,
            rootJobId: rootJobId || jobId,
          });
        }
        await completeJob(jobId, {
          propertyId,
          ownerType: 'company',
          corporate,
        });
        return;
      }

      // Step 2: Open register lookup
      const register = await lookupOpenRegister(address);
      await logEvent(jobId, 'info', 'lookupOpenRegister data', {
        register: register,
      });

      const openRegisterOccupants = register?.occupants || [];
      await logEvent(jobId, 'info', 'Open register results', {
        count: openRegisterOccupants.length,
        source: register?.source || null,
        openRegisterOccupants: openRegisterOccupants || null,
      });

      let pricePaidTransactions: PricePaidTransaction[] = [];
      let pricePaidMatch: ReturnType<typeof choosePricePaidTransaction> | null = null;
      let latestSaleYear: number | undefined;
      let latestSaleDate: string | undefined;
      if (address.postcode) {
        pricePaidTransactions = await fetchPricePaidTransactionsByPostcode(address.postcode);
        pricePaidMatch = choosePricePaidTransaction(targetNormalized, pricePaidTransactions);
        if (pricePaidMatch?.transaction?.date) {
          latestSaleDate = pricePaidMatch.transaction.date;
          latestSaleYear = extractSaleYear(pricePaidMatch.transaction.date);
        }
        await logEvent(jobId, 'info', 'Land Registry price paid search', {
          postcode: address.postcode,
          targetCanonical: targetNormalized.canonical_key,
          transactionCount: pricePaidTransactions.length,
          latestSaleDate: latestSaleDate || null,
          latestSaleYear: latestSaleYear ?? null,
          matchedTransaction: pricePaidMatch
            ? {
                amount: pricePaidMatch.transaction.amount ?? null,
                date: pricePaidMatch.transaction.date ?? null,
                paon: pricePaidMatch.transaction.paon ?? null,
                saon: pricePaidMatch.transaction.saon ?? null,
                street: pricePaidMatch.transaction.street ?? null,
                score: pricePaidMatch.score,
                targetVariant: pricePaidMatch.targetVariant,
                candidateVariant: pricePaidMatch.candidateVariant,
                canonical: pricePaidMatch.transaction.normalized?.canonical_key || null,
                unitExact: pricePaidMatch.unitExact ?? null,
                paonExact: pricePaidMatch.paonExact ?? null,
                reason: pricePaidMatch.reason || null,
              }
            : null,
          sampleTransactions: pricePaidTransactions.slice(0, 5).map((tx) => ({
            amount: tx.amount ?? null,
            date: tx.date ?? null,
            paon: tx.paon ?? null,
            saon: tx.saon ?? null,
            street: tx.street ?? null,
            canonical: tx.normalized?.canonical_key || null,
          })),
        });
      } else {
        await logEvent(jobId, 'warn', 'Land Registry price paid search skipped (no postcode)', {
          address: prettyAddress(address),
        });
      }
      
      if (rootJobId && openRegisterOccupants.length) {
        await logEvent(rootJobId, 'info', 'Occupants located', {
          childJobId: jobId,
          count: openRegisterOccupants.length,
          openRegisterOccupants: openRegisterOccupants || null,
        });
      }

      // Track which occupants came from Open Register so we can enforce provenance later
      const openRegisterKeys = new Set<string>();

      const occupantMap = new Map<string, OccupantRecord>();
      for (const occ of openRegisterOccupants) {
        const enriched: OccupantRecord = {
          ...occ,
          dataSources: Array.from(new Set([...(occ.dataSources || []), 'open_register', register?.source ? `or_${register.source}` : undefined].filter(Boolean) as string[])),
          indicators: Array.from(new Set([...(occ.indicators || []), 'open_register'])),
        };
        const key = occupantRecordKey(enriched);
        addOrMergeOccupant(occupantMap, enriched);
        openRegisterKeys.add(key);
      }

      // Step 4: Companies House search (done before scoring to feed rubric)
      const companyHits = await searchCompaniesByAddress(jobId, address);
      const officerHits = await searchOfficersByAddress(jobId, address);

      await logEvent(jobId, 'info', 'Companies House address search', {
        companyMatches: companyHits,
        officerMatches: officerHits,
      });

      await logEvent(jobId, 'info', 'Summarized Companies House address search', {
        companyMatches: companyHits.map(summarizeCompany),
        officerMatches: officerHits.map(summarizeOfficer),
      });

      const matchedCompanies = companyHits
        .filter((hit) => hit.matched && hit.matchConfidence >= 0.7 && !!hit.companyNumber)
        .sort((a, b) => b.matchConfidence - a.matchConfidence)

      await logEvent(jobId, 'info', 'CH Companies Matched by address search', {
        count: matchedCompanies.length,
        matchedCompanies
      });

      const chAffiliatedNames = new Set<string>();
      const enqueuedCompanies: string[] = [];
      if (matchedCompanies.length) {
        const companyPersonsForLog: Array<{
          companyNumber: string;
          companyName: string;
          directors: number;
          pscs: number;
          savedDirectorsAtAddress: string[];
          savedPscsAtAddress: string[];
          directorsHeldNotAtAddress: Array<{ name: string; appointedOn?: string | null; address?: any }>;
          pscsHeldNotAtAddress: Array<{ name: string; ceasedOn?: string | null; address?: any }>;
        }> = [];
        for (const company of matchedCompanies) {
          if (!company.companyNumber) continue;
          const [directors, pscs] = await Promise.all([
            listCompanyDirectors(company.companyNumber).catch(async (err) => {
              await logEvent(jobId, 'warn', 'Failed to fetch company directors', {
                companyNumber: company.companyNumber,
                companyName: company.companyName,
                error: String(err),
              });
              return [] as CompanyOfficerRecord[];
            }),
            listCompanyPscs(company.companyNumber).catch(async (err) => {
              await logEvent(jobId, 'warn', 'Failed to fetch company PSCs', {
                companyNumber: company.companyNumber,
                companyName: company.companyName,
                error: String(err),
              });
              return [] as CompanyPscRecord[];
            }),
          ]);

          // Separate CH people whose PERSONAL address matches the prompt from those who don't
          const savedDirectorsAtAddress: string[] = [];
          const savedPscsAtAddress: string[] = [];
          const directorsHeldNotAtAddress: Array<{ name: string; appointedOn?: string | null; address?: any }> = [];
          const pscsHeldNotAtAddress: Array<{ name: string; ceasedOn?: string | null; address?: any }> = [];

          for (const officer of directors) {
            const personalMatches = chPersonalAddressMatchesPrompt(officer.address, address);
            let matchesAtAddress = personalMatches;
            let matchedAppointmentAddress: any = undefined;
            if (!matchesAtAddress && officer.officerId) {
              try {
                const appts = await fetchOfficerAppointments(officer.officerId);
                const items = Array.isArray(appts?.items) ? appts.items : [];
                for (const it of items) {
                  if (it?.address && chPersonalAddressMatchesPrompt(it.address, address)) {
                    matchesAtAddress = true;
                    matchedAppointmentAddress = it.address;
                    break;
                  }
                }
              } catch (e) {
                await logEvent(jobId, 'debug', 'Officer appointment lookup failed during address confirmation', {
                  officerId: officer.officerId,
                  error: String(e),
                });
              }
            }
            if (matchesAtAddress) {
              const occ = occupantFromCompanyPerson(officer.name, {
                source: 'director',
                company,
                officer,
              });
              if (occ) {
                addOrMergeOccupant(occupantMap, occ);
                chAffiliatedNames.add(normalizeName(occ.fullName));
                savedDirectorsAtAddress.push(occ.fullName);
              }
            } else {
              directorsHeldNotAtAddress.push({
                name: officer.name,
                appointedOn: officer.appointedOn || null,
                address: officer.address || matchedAppointmentAddress || null,
              });
            }
          }

          for (const psc of pscs) {
            const personalMatches = chPersonalAddressMatchesPrompt(psc.address, address);
            if (personalMatches) {
              const occ = occupantFromCompanyPerson(psc.name, {
                source: 'psc',
                company,
                psc,
              });
              if (occ) {
                addOrMergeOccupant(occupantMap, occ);
                chAffiliatedNames.add(normalizeName(occ.fullName));
                savedPscsAtAddress.push(occ.fullName);
              }
            } else {
              pscsHeldNotAtAddress.push({
                name: psc.name,
                ceasedOn: psc.ceasedOn || null,
                address: psc.address || null,
              });
            }
          }

          // Keep rich per-company log + “hold” non-at-address people under the company
          companyPersonsForLog.push({
            companyNumber: company.companyNumber,
            companyName: company.companyName,
            directors: directors.length,
            pscs: pscs.length,
            savedDirectorsAtAddress,
            savedPscsAtAddress,
            directorsHeldNotAtAddress,
            pscsHeldNotAtAddress,
          });
        }
        if (companyPersonsForLog.length) {
          const occupantsForLog = Array.from(occupantMap.entries()).map(([key, occ]) => ({
            key,
            fullName: occ.fullName,
            firstName: occ.firstName,
            lastName: occ.lastName,
            firstSeenYear: occ.firstSeenYear,
            lastSeenYear: occ.lastSeenYear,
            dataSources: occ.dataSources,
            indicators: occ.indicators,
          }));
          await logEvent(jobId, 'info', 'Enriched occupants with Companies House data', {
            companies: companyPersonsForLog,
            totalOccupants: occupantMap.size,
            occupants: occupantsForLog,
          });
        }

        if (AUTO_QUEUE_COMPANY_DISCOVERY) {
          for (const company of matchedCompanies) {
            if (!company.companyNumber || enqueuedCompanies.includes(company.companyNumber)) continue;
            try {
              const coJobId = `owner-company:${company.companyNumber}`;
              await companyQ.add(
                'discover',
                {
                  companyNumber: company.companyNumber,
                  companyName: company.companyName,
                  address: pretty,
                  postcode: address.postcode,
                  rootJobId: rootJobId || undefined,
                  ownerJobId: jobId,
                  requestSource: 'owner-discovery',
                  parentJobId: jobId,
                },
                { jobId: coJobId, attempts: 5, backoff: { type: 'exponential', delay: 1500 } }
              );
              enqueuedCompanies.push(company.companyNumber);
            } catch (err) {
              await logEvent(jobId, 'warn', 'Failed to enqueue company discovery from owner match', {
                companyNumber: company.companyNumber,
                companyName: company.companyName,
                error: String(err),
              });
            }
          }
          if (enqueuedCompanies.length) {
            await logEvent(jobId, 'info', 'Enqueued company discovery jobs from owner search', {
              companies: enqueuedCompanies,
            });
          }
        }
      }

      for (const key of openRegisterKeys) {
        const occ = occupantMap.get(key);
        if (!occ) continue;
        const ds = new Set(occ.dataSources || []);
        ds.add('open_register');
        if (register?.source) ds.add(`or_${register.source}`);
        const ind = new Set(occ.indicators || []);
        ind.add('open_register');
        occupantMap.set(key, {
          ...occ,
          dataSources: Array.from(ds),
          indicators: Array.from(ind),
        });
      }

      const officerNames = new Set<string>();
      // Track size and added keys before adding officer hits
      const beforeOfficerAddSize = occupantMap.size;
      const addedKeysFromOfficers: string[] = [];

      for (const hit of officerHits) {
        if (hit?.name) {
          const parsed = parseOfficerName(hit.name);
          const normalized = normalizeName(`${parsed.first} ${parsed.last}`);
          if (normalized) officerNames.add(normalized);
          officerNames.add(normalizeName(hit.name));
        }
      }
      // Promote Companies House officer hits into occupant candidates (so they can be scored like Open Register occupants)
      for (const hit of officerHits) {
        // Build a full name from structured parts when available, else fall back to the display name
        const first = (hit.firstName || '').trim();
        const middle = (hit.middleName || '').trim();
        const last = (hit.lastName || '').trim();
        const parts = [first, middle, last].filter(Boolean);
        const fullName = (parts.length ? parts.join(' ') : (hit.name || '')).trim();
        if (!fullName) continue;

        const occ: OccupantRecord = {
          firstName: first || '',
          lastName: last || '',
          fullName,
          ageBand: undefined,
          birthYear: undefined,
          firstSeenYear: undefined,
          lastSeenYear: undefined,
          dataSources: Array.from(new Set([
            'companies_house_officer',
            hit.officerId ? `ch_officer_${hit.officerId}` : undefined,
          ].filter(Boolean) as string[])),
          indicators: ['companies_house'],
        };

        const { added, key: occKey } = addOrMergeOccupant(occupantMap, occ);
        if (added) addedKeysFromOfficers.push(occKey);
      }

      const occupants = Array.from(occupantMap.values());
      if (occupants.length !== openRegisterOccupants.length) {
        const occupantsForLog = occupants.map((occ) => ({
          fullName: occ.fullName,
          firstName: occ.firstName,
          lastName: occ.lastName,
          firstSeenYear: occ.firstSeenYear,
          lastSeenYear: occ.lastSeenYear,
          dataSources: occ.dataSources,
          indicators: occ.indicators,
        }));
        await logEvent(jobId, 'info', 'Occupant list expanded with Companies House data', {
          openRegisterCount: openRegisterOccupants.length,
          total: occupants.length,
          added: occupants.length - openRegisterOccupants.length,
          occupants: occupantsForLog,
        });
      }

      const afterOfficerAddSize = occupantMap.size;
      const addedFromOfficers = afterOfficerAddSize - beforeOfficerAddSize;
      const addedOccupantsForLog = addedKeysFromOfficers.map((key) => {
        const occ = occupantMap.get(key)!;
        return {
          key,
          fullName: occ.fullName,
          firstName: occ.firstName,
          lastName: occ.lastName,
          firstSeenYear: occ.firstSeenYear,
          lastSeenYear: occ.lastSeenYear,
          dataSources: occ.dataSources,
          indicators: occ.indicators,
        };
      });

      await logEvent(jobId, 'info', 'Added officer hits as occupant candidates', {
        officerHits: officerHits.length,
        addedFromOfficers,
        totalOccupantsNow: afterOfficerAddSize,
        added: addedOccupantsForLog,
      });

      const allOccupantsForLog = Array.from(occupantMap.entries()).map(([key, occ]) => ({
        key,
        fullName: occ.fullName,
        firstName: occ.firstName,
        lastName: occ.lastName,
        firstSeenYear: occ.firstSeenYear,
        lastSeenYear: occ.lastSeenYear,
        dataSources: occ.dataSources,
        indicators: occ.indicators,
      }));

      await logEvent(jobId, 'info', 'All occupant candidates before scoring', {
        total: occupantMap.size,
        openRegisterCount: openRegisterOccupants.length,
        addedFromCompaniesHouse:
          occupantMap.size - openRegisterOccupants.length,
        occupants: allOccupantsForLog,
      });

      const confirmedMatches = new Set<string>(chAffiliatedNames);
      for (const occ of occupants) {
        const norm = normalizeName(occ.fullName);
        if (norm && officerNames.has(norm)) {
          confirmedMatches.add(norm);
        }
      }

      await logEvent(jobId, 'debug', 'Scoring inputs', {
        occupantCount: occupants.length,
        confirmedMatches: Array.from(confirmedMatches),
        latestSaleYear: latestSaleYear ?? null,
      });

      // Step 3: Apply rubric scoring (using confirmed matches to boost scores)
      const candidates = scoreOccupants(occupants, { confirmedMatches, latestSaleYear });
      await logEvent(jobId, 'info', 'Scoring results', {
        totalCandidates: candidates.length,
        topFive: candidates.slice(0, 5).map(c => ({
          name: c.fullName,
          score: c.score,
          rank: c.rank,
        })),
      });
      await clearCandidates(propertyId);
      await logEvent(jobId, 'debug', 'Cleared previous candidates for property', { propertyId });
      for (const cand of candidates) {
        const normalizedFullName = normalizeName(cand.fullName);
        const occupantMatch =
          occupants.find((o) => normalizeName(o.fullName) === normalizedFullName) || null;
        const relationCompanyNumbers = occupantMatch?.companyRelations
          ? Array.from(
              new Set(
                occupantMatch.companyRelations
                  .map((rel) => (rel?.companyNumber || '').trim())
                  .filter(Boolean)
              )
            )
          : [];
        const relationOfficerIds = occupantMatch?.companyRelations
          ? Array.from(
              new Set(
                occupantMatch.companyRelations
                  .map((rel) => (rel?.officerId || '').trim())
                  .filter(Boolean)
              )
            )
          : [];
        const { rows } = await query<{ id: number }>(
          `INSERT INTO owner_candidates (property_id, full_name, first_name, last_name, score, rank, sources, evidence, normalized_full_name, company_numbers, officer_ids)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           RETURNING id`,
          [
            propertyId,
            cand.fullName,
            cand.firstName || null,
            cand.lastName || null,
            cand.score,
            cand.rank,
            cand.sources,
            JSON.stringify({ signals: cand.signals, occupant: occupantMatch }),
            normalizedFullName || null,
            relationCompanyNumbers.length ? relationCompanyNumbers : null,
            relationOfficerIds.length ? relationOfficerIds : null,
          ]
        );
        const candidateId = rows[0].id;
        await logEvent(jobId, 'debug', 'Persisted candidate', {
          candidateId,
          name: cand.fullName,
          score: cand.score,
          rank: cand.rank,
          signalCount: cand.signals.length,
        });
        for (const sig of cand.signals) {
          await query(
            `INSERT INTO owner_signals (candidate_id, signal_id, label, weight, value, score, reason)
             VALUES ($1,$2,$3,$4,$5,$6,$7)`,
            [candidateId, sig.id, sig.label, sig.weight, sig.value, sig.score, sig.reason]
          );
        }
        await logEvent(jobId, 'debug', 'Persisted candidate signals', {
          candidateId,
          signals: cand.signals.map(s => ({
            id: s.id, label: s.label, weight: s.weight, value: s.value, score: s.score
          })),
        });
      }

      const totalCandidates = candidates.length;
      const best = candidates[0];
      const candidateByKey = new Map<string, (typeof candidates)[number]>();
      for (const cand of candidates) {
        candidateByKey.set(normalizeName(cand.fullName), cand);
      }
      const bestKey = best ? normalizeName(best.fullName) : null;
      const bestOccupant = bestKey ? occupantMap.get(bestKey) : undefined;
      const bestSources = new Set<string>([
        ...((best?.sources || []) as string[]),
        ...((bestOccupant?.dataSources || []) as string[]),
      ]);
      const bestIsOpenRegister = Boolean(bestKey && openRegisterKeys.has(bestKey));
      const bestIsDirector = bestSources.has('companies_house_director');
      const bestIsOfficer = bestSources.has('companies_house_officer');
      const bestIsPsc = bestSources.has('companies_house_psc');
      const openRegisterDetails = Array.from(openRegisterKeys).map((key) => {
        const occupant = occupantMap.get(key);
        const candidate = candidateByKey.get(key) || null;
        const sources = new Set<string>(occupant?.dataSources || []);
        const relations = occupant?.companyRelations || [];
        return {
          key,
          name: occupant?.fullName || candidate?.fullName || '',
          score: candidate?.score ?? null,
          rank: candidate?.rank ?? null,
          isDirector: sources.has('companies_house_director'),
          isOfficer: sources.has('companies_house_officer'),
          isPsc: sources.has('companies_house_psc'),
          dataSources: occupant?.dataSources || [],
          indicators: occupant?.indicators || [],
          firstSeenYear: occupant?.firstSeenYear ?? null,
          lastSeenYear: occupant?.lastSeenYear ?? null,
          companyNumbers: Array.from(new Set((relations || []).map((rel) => rel?.companyNumber).filter(Boolean))) as string[],
        };
      });
      const openRegisterSummary = openRegisterDetails.map((detail) => ({
        name: detail.name,
        score: detail.score,
        rank: detail.rank,
        isDirector: detail.isDirector,
        isOfficer: detail.isOfficer,
        isPsc: detail.isPsc,
        companyNumbers: detail.companyNumbers,
        firstSeenYear: detail.firstSeenYear,
        lastSeenYear: detail.lastSeenYear,
      }));
      const ownerDetermined = Boolean(best && best.score >= ACCEPT_THRESHOLD && bestIsOpenRegister);
      const ownerReviewCandidate = Boolean(best && best.score >= REVIEW_THRESHOLD && bestIsOpenRegister && best.score < ACCEPT_THRESHOLD);
      let decisionScenario: string | null = null;
      let scenarioResolution: Record<string, any> | null = null;

      if (ownerDetermined && best && bestOccupant) {
        decisionScenario = bestIsDirector || bestIsOfficer || bestIsPsc
          ? 'open_register_owner_director'
          : 'open_register_owner_individual';
        scenarioResolution = {
          scenario: decisionScenario,
          ownerSources: Array.from(bestSources),
          companyNumbers: Array.from(new Set((bestOccupant.companyRelations || []).map((rel) => rel?.companyNumber).filter(Boolean))),
        };
        await enqueuePersonLinkedInJob({
          ownerJobId: jobId,
          rootJobId,
          occupant: bestOccupant,
          propertyAddress: pretty,
          address,
          registerSource: register?.source || null,
          score: best.score,
          rank: best.rank,
          latestSaleYear,
          reason: decisionScenario,
        });
        if (bestIsDirector || bestIsOfficer) {
          await enqueueChAppointmentsForOwner({
            ownerJobId: jobId,
            rootJobId,
            occupant: bestOccupant,
            propertyAddress: pretty,
            relations: bestOccupant.companyRelations,
            reason: decisionScenario,
            allowQueue: AUTO_QUEUE_CH,
          });
        }
      } else if (openRegisterKeys.size) {
        const hasDirector = openRegisterDetails.some((detail) => detail.isDirector || detail.isOfficer);
        decisionScenario = hasDirector
          ? 'open_register_director_not_confirmed'
          : 'open_register_not_confirmed';
        scenarioResolution = {
          scenario: decisionScenario,
          openRegisterSummary,
          thresholds: { accept: ACCEPT_THRESHOLD, review: REVIEW_THRESHOLD },
        };
        await logToJobAndRoot(jobId, rootJobId, hasDirector ? 'info' : 'info', 'Open register occupant present but no confirmed owner', {
          scenario: decisionScenario,
          openRegisterSummary,
          bestCandidate: best ? { name: best.fullName, score: best.score, rank: best.rank } : null,
          ownerReviewCandidate,
        });
      }
      let status = 'needs_title_register';
      let ownerType: 'individual' | null = null;
      let resolution: any = null;

      if (best && best.score >= ACCEPT_THRESHOLD) {
        status = 'resolved';
        ownerType = 'individual';
        resolution = {
          ownerName: best.fullName,
          score: best.score,
          rank: best.rank,
          reason: 'score_above_accept_threshold',
          scenario: decisionScenario,
          sources: Array.from(bestSources),
          openRegister: bestIsOpenRegister ? { source: register?.source || null } : null,
        };
      } else if (best && best.score >= REVIEW_THRESHOLD) {
        status = 'needs_confirmation';
        ownerType = 'individual';
        resolution = {
          ownerName: best.fullName,
          score: best.score,
          rank: best.rank,
          reason: 'score_between_review_bounds',
          scenario: decisionScenario,
          sources: Array.from(bestSources),
          openRegister: bestIsOpenRegister ? { source: register?.source || null } : null,
        };
      } else if (!occupants.length && !officerHits.length && !companyHits.length) {
        status = 'no_public_data';
        resolution = { reason: 'no_open_data_hits' };
      }

      if (!resolution && scenarioResolution) {
        resolution = scenarioResolution;
      } else if (resolution && scenarioResolution) {
        resolution = { ...resolution, ...scenarioResolution };
      }

      await query(
        `UPDATE owner_properties
           SET status = $2,
               owner_type = $3,
               resolution = $4,
               candidate_summary = $5,
               corporate_owner = NULL,
               updated_at = now()
         WHERE id = $1`,
        [
          propertyId,
          status,
          ownerType,
          resolution ? JSON.stringify(resolution) : null,
          JSON.stringify({
            totalCandidates,
            bestScore: best?.score ?? null,
            occupantCount: occupants.length,
            openRegisterSource: register?.source || null,
            officerMatches: officerHits.length,
            companyMatches: companyHits.length,
          }),
        ]
      );

      await logEvent(jobId, 'info', 'Owner discovery decision', {
        status,
        thresholds: { ACCEPT_THRESHOLD, REVIEW_THRESHOLD },
        best: best ? { name: best.fullName, score: best.score, rank: best.rank } : null,
        occupantCount: occupants.length,
        officerHits: officerHits.length,
        companyMatches: companyHits.length,
        scenario: decisionScenario,
      });

      await logEvent(jobId, 'info', 'Owner discovery complete', {
        status,
        ownerType,
        bestCandidate: best ? { name: best.fullName, score: best.score } : null,
        totalCandidates,
        scenario: decisionScenario,
      });
      if (rootJobId) {
        await logEvent(rootJobId, 'info', 'Owner discovery summary', {
          childJobId: jobId,
          status,
          ownerType,
          bestCandidate: best ? { name: best.fullName, score: Number(best.score?.toFixed?.(3) ?? best.score) } : null,
          scenario: decisionScenario,
        });
      }

      await completeJob(jobId, {
        propertyId,
        status,
        ownerType,
        bestCandidate: best ? { name: best.fullName, score: best.score } : null,
      });
    } catch (err) {
      await failJob(jobId, err);
      if (rootJobId) {
        await logEvent(rootJobId, 'error', 'Owner discovery failed', { childJobId: jobId, error: String(err) });
      }
      throw err;
    }
  },
  { connection, concurrency: Number(process.env.OWNER_WORKER_CONCURRENCY || 2) }
);

// const temp = {
//   "active_count":1,
//   "date_of_birth": {
//     "month":5,
//     "year":1981
//   },"etag":"5405e488d80e6636422aba24183ea44f33d79d5f",
//   "inactive_count":2,
//   "is_corporate_officer":false,
//   "items":[
//     {
//       "address":{
//         "address_line_1": "132 Ben Jonson Road",
//         "country":"United Kingdom",
//         "locality":"London",
//         "postal_code":"E1 4GJ",
//         "premises":"Apartment 305"
//       },
//       "appointed_on":"2015-08-12",
//       "appointed_to":{
//         "company_name":"ROCK BASE PROPERTIES LIMITED",
//         "company_number":"09728495",
//         "company_status":"dissolved"
//       },"name":"Timothy AGBETILE",
//       "country_of_residence":"United Kingdom",
//       "is_pre_1992_appointment":false,
//       "links":{
//         "company":"/company/09728495"
//       },
//       "name_elements":{
//         "forename":"Timothy",
//         "title":"Mr",
//         "surname":"AGBETILE"
//       },
//       "nationality":"British",
//       "occupation":"Property",
//       "officer_role":"director"
//     },
//     {
//       "address":{
//         "address_line_1":"Greenhough Road",
//         "country":"United Kingdom",
//         "locality":"Lichfield",
//         "postal_code":"WS13 7FE",
//         "premises":"4 Parkside Court",
//         "region":"Staffordshire"
//       },"appointed_on":"2015-04-18",
//       "appointed_to":{
//         "company_name":"VITAX SOLUTIONS LIMITED",
//         "company_number":"09549152",
//         "company_status":"active"
//       },
//       "name":"Timothy AGBETILE",
//       "country_of_residence":"England",
//       "is_pre_1992_appointment":false,
//       "links":{
//         "company":"/company/09549152"
//       },
//       "name_elements":{
//         "forename":"Timothy",
//         "title":"Mr",
//         "surname":"AGBETILE"
//       },"nationality":"British",
//       "occupation":"Consultant",
//       "officer_role":"director"
//     },{
//       "address":{
//         "address_line_1":"Calico Business Park",
//         "locality":"Sandy Way",
//         "postal_code":"B77 4BF",
//         "premises":"3 Hamel House",
//         "region":"Tamworth"
//       },
//       "appointed_on":"2013-10-08",
//       "appointed_to":{
//         "company_name":"TIMTRIX LIMITED",
//         "company_number":"08722589",
//         "company_status":"dissolved"
//       },"name":"Timothy AGBETILE",
//       "country_of_residence":"United Kingdom",
//       "is_pre_1992_appointment":false,
//       "links":{
//         "company":"/company/08722589"
//       },
//       "name_elements":{
//         "forename":"Timothy",
//         "title":"Mr",
//         "surname":"AGBETILE"
//       },"nationality":"British",
//       "occupation":"Retail/Software",
//       "officer_role":"director"
//     }
//   ],"items_per_page":35,
//   "kind":"personal-appointment",
//   "links":{
//     "self":"/officers/SwGj0VQh0zdkZsfee4UOPaI2CZI/appointments"
//   },
//   "name":"Timothy AGBETILE",
//   "resigned_count":0,
//   "start_index":0,
//   "total_results":3
// }
