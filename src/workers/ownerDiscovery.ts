import 'dotenv/config';
import { Worker } from 'bullmq';
import { connection, chQ } from '../queues/index.js';
import { initDb, startJob, logEvent, completeJob, failJob } from '../lib/progress.js';
import { query } from '../lib/db.js';
import { AddressInput, prettyAddress, addressKey } from '../lib/address.js';
import { findCorporateOwner, searchCorporateOwnersByPostcode } from '../lib/landRegistry.js';
import type { CorporateOwnerRecord, CorporateOwnerMatch } from '../lib/landRegistry.js';
import { lookupOpenRegister } from '../lib/openRegister.js';
import { scoreOccupants } from '../lib/homeownerRubric.js';
import { searchCompaniesByAddress, searchOfficersByAddress, summarizeCompany, summarizeOfficer } from '../lib/companiesHouseSearch.js';
import { parseOfficerName } from '../lib/normalize.js';

await initDb();

const ACCEPT_THRESHOLD = Number(process.env.OWNER_ACCEPT_THRESHOLD || 0.55);
const REVIEW_THRESHOLD = Number(process.env.OWNER_REVIEW_THRESHOLD || 0.35);
const AUTO_QUEUE_CH = (process.env.OWNER_QUEUE_CH_DIRECTORS || 'true').toLowerCase() === 'true';

export type OwnerDiscoveryJob = {
  address: AddressInput;
  rootJobId?: string;
  metadata?: Record<string, any>;
  allowCorporateQueue?: boolean;
};

function normalizeName(name: string): string {
  return (name || '').toLowerCase().replace(/\s+/g, ' ').trim();
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

function buildTargetVariants(address: AddressInput): string[] {
  const variants = new Set<string>();
  const lines = [address.line1, address.line2].filter(Boolean) as string[];
  if (lines.length) variants.add(lines.join(' '));
  if (lines.length > 1) variants.add([...lines].reverse().join(' '));
  variants.add(address.line1);
  if (address.line2) variants.add(address.line2);
  if (address.city) {
    variants.add(`${address.line1} ${address.city}`);
    if (lines.length) variants.add(`${lines.join(' ')} ${address.city}`);
  }
  return Array.from(variants)
    .map(normalizeForMatch)
    .filter((value) => value.length > 2);
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

function chooseCorporateMatch(candidates: CorporateOwnerRecord[], address: AddressInput) {
  const targetVariants = buildTargetVariants(address).map((value) => ({
    raw: value,
    tokens: new Set(tokens(value)),
  }));
  if (!targetVariants.length) return null;

  type MatchResult = {
    record: CorporateOwnerRecord;
    extraTokens: number;
    targetSize: number;
    targetVariant: string;
    candidateVariant: string;
  };

  let best: MatchResult | null = null;

  for (const record of candidates) {
    const candidateVariants = extractCandidateVariants(record).map((entry) => ({
      raw: entry.raw,
      normalized: entry.normalized,
      tokens: new Set(tokens(entry.normalized)),
    }));
    if (!candidateVariants.length) continue;

    for (const candidate of candidateVariants) {
      if (!candidate.tokens.size) continue;
      for (const target of targetVariants) {
        if (!target.tokens.size) continue;
        let missing = 0;
        for (const token of target.tokens) {
          if (!candidate.tokens.has(token)) {
            missing = 1;
            break;
          }
        }
        if (missing) continue;
        const extraTokens = candidate.tokens.size - target.tokens.size;
        if (
          !best ||
          extraTokens < best.extraTokens ||
          (extraTokens === best.extraTokens && target.tokens.size > best.targetSize)
        ) {
          best = {
            record,
            extraTokens,
            targetSize: target.tokens.size,
            targetVariant: target.raw,
            candidateVariant: candidate.raw,
          };
        }
      }
    }
  }

  return best;
}

export default new Worker<OwnerDiscoveryJob>(
  'owner-discovery',
  async (job) => {
    const payload = job.data;
    const jobId = job.id as string;
    const rootJobId = payload.rootJobId;
    const address = payload.address;
    if (!address?.line1 || !address?.postcode) {
      throw new Error('Address with line1 and postcode is required');
    }
    await startJob({ jobId, queue: 'owner-discovery', name: job.name, payload });
    const pretty = prettyAddress(address);

    try {
      const property = await upsertProperty(jobId, payload);
      const propertyId = property.id;
      await logEvent(jobId, 'info', 'Owner property job started', {
        address: pretty,
        addressKey: addressKey(address),
        propertyId,
      });
      if (rootJobId) {
        await logEvent(rootJobId, 'info', 'Owner pipeline started', { childJobId: jobId, address: pretty });
      }

      // Step 1: Land Registry corporate ownership check
      let corporate: CorporateOwnerMatch | null = null;
      const postcodeHits = await searchCorporateOwnersByPostcode(address.postcode);
      if (postcodeHits.length) {
        const best = chooseCorporateMatch(postcodeHits, address);
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
              }
            : null,
        });
        if (best) {
          corporate = {
            matchType: best.extraTokens === 0 ? 'exact' : 'postcode',
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
            { companyNumber: corporate.companyNumber },
            { jobId: chJobId, attempts: 3, backoff: { type: 'exponential', delay: 1000 } }
          );
          await logEvent(jobId, 'info', 'Enqueued CH director lookup', { companyNumber: corporate.companyNumber, chJobId });
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
      const occupants = register?.occupants || [];
      await logEvent(jobId, 'info', 'Open register results', {
        count: occupants.length,
        source: register?.source || null,
      });
      if (rootJobId && occupants.length) {
        await logEvent(rootJobId, 'info', 'Occupants located', {
          childJobId: jobId,
          count: occupants.length,
        });
      }

      // Step 4: Companies House search (done before scoring to feed rubric)
      const companyHits = await searchCompaniesByAddress(address);
      const officerHits = await searchOfficersByAddress(address);
      await logEvent(jobId, 'info', 'Companies House address search', {
        companyMatches: companyHits.map(summarizeCompany),
        officerMatches: officerHits.map(summarizeOfficer),
      });

      const officerNames = new Set<string>();
      for (const hit of officerHits) {
        if (hit?.name) {
          const parsed = parseOfficerName(hit.name);
          const normalized = normalizeName(`${parsed.first} ${parsed.last}`);
          if (normalized) officerNames.add(normalized);
          officerNames.add(normalizeName(hit.name));
        }
      }

      const confirmedMatches = new Set<string>();
      for (const occ of occupants) {
        const norm = normalizeName(occ.fullName);
        if (norm && officerNames.has(norm)) {
          confirmedMatches.add(norm);
        }
      }

      // Step 3: Apply rubric scoring (using confirmed matches to boost scores)
      const candidates = scoreOccupants(occupants, { confirmedMatches });
      await clearCandidates(propertyId);
      for (const cand of candidates) {
        const { rows } = await query<{ id: number }>(
          `INSERT INTO owner_candidates (property_id, full_name, first_name, last_name, score, rank, sources, evidence)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           RETURNING id`,
          [
            propertyId,
            cand.fullName,
            cand.firstName || null,
            cand.lastName || null,
            cand.score,
            cand.rank,
            cand.sources,
            JSON.stringify({ signals: cand.signals, occupant: occupants.find((o) => normalizeName(o.fullName) === normalizeName(cand.fullName)) || null }),
          ]
        );
        const candidateId = rows[0].id;
        for (const sig of cand.signals) {
          await query(
            `INSERT INTO owner_signals (candidate_id, signal_id, label, weight, value, score, reason)
             VALUES ($1,$2,$3,$4,$5,$6,$7)`,
            [candidateId, sig.id, sig.label, sig.weight, sig.value, sig.score, sig.reason]
          );
        }
      }

      const totalCandidates = candidates.length;
      const best = candidates[0];
      let status = 'needs_title_register';
      let ownerType: 'individual' | null = null;
      let resolution: any = null;

      if (best && best.score >= ACCEPT_THRESHOLD) {
        status = 'resolved';
        ownerType = 'individual';
        resolution = { ownerName: best.fullName, score: best.score, rank: best.rank, reason: 'score_above_accept_threshold' };
      } else if (best && best.score >= REVIEW_THRESHOLD) {
        status = 'needs_confirmation';
        ownerType = 'individual';
        resolution = { ownerName: best.fullName, score: best.score, rank: best.rank, reason: 'score_between_review_bounds' };
      } else if (!occupants.length && !officerHits.length && !companyHits.length) {
        status = 'no_public_data';
        resolution = { reason: 'no_open_data_hits' };
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

      await logEvent(jobId, 'info', 'Owner discovery complete', {
        status,
        ownerType,
        bestCandidate: best ? { name: best.fullName, score: best.score } : null,
        totalCandidates,
      });
      if (rootJobId) {
        await logEvent(rootJobId, 'info', 'Owner discovery summary', {
          childJobId: jobId,
          status,
          ownerType,
          bestCandidate: best ? { name: best.fullName, score: Number(best.score?.toFixed?.(3) ?? best.score) } : null,
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
