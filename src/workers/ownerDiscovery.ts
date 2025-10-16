import 'dotenv/config';
import { Worker } from 'bullmq';
import { connection, chQ, companyQ } from '../queues/index.js';
import { initDb, startJob, logEvent, completeJob, failJob } from '../lib/progress.js';
import { query } from '../lib/db.js';
import { AddressInput, prettyAddress, addressKey } from '../lib/address.js';
import { findCorporateOwner, searchCorporateOwnersByPostcode } from '../lib/landRegistry.js';
import type { CorporateOwnerRecord, CorporateOwnerMatch } from '../lib/landRegistry.js';
import { lookupOpenRegister } from '../lib/openRegister.js';
import type { OccupantRecord } from '../lib/openRegister.js';
import { scoreOccupants } from '../lib/homeownerRubric.js';
import {
  searchCompaniesByAddress,
  searchOfficersByAddress,
  summarizeCompany,
  summarizeOfficer,
  listCompanyDirectors,
  listCompanyPscs,
  type CompanyAddressHit,
  type CompanyOfficerRecord,
  type CompanyPscRecord,
} from '../lib/companiesHouseSearch.js';
import { parseOfficerName } from '../lib/normalize.js';

await initDb();

const ACCEPT_THRESHOLD = Number(process.env.OWNER_ACCEPT_THRESHOLD || 0.55);
const REVIEW_THRESHOLD = Number(process.env.OWNER_REVIEW_THRESHOLD || 0.35);
const AUTO_QUEUE_CH = (process.env.OWNER_QUEUE_CH_DIRECTORS || 'true').toLowerCase() === 'true';
const AUTO_QUEUE_COMPANY_DISCOVERY =
  (process.env.OWNER_QUEUE_COMPANY_DISCOVERY || 'true').toLowerCase() === 'true';

export type OwnerDiscoveryJob = {
  address: AddressInput;
  rootJobId?: string;
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
  console.log('Target variants for address in ownerDiscovery:', targetVariants.map((v) => v.raw));
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

function occupantRecordKey(rec: OccupantRecord): string {
  const name = rec.fullName || `${rec.firstName || ''} ${rec.lastName || ''}`;
  return normalizeName(name);
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
  };

  map.set(key, merged);
  return { added: false, merged: true, key };
}

function addressInputFromChAddress(addr: any): AddressInput | null {
  if (!addr) return null;
  const line1Parts = [addr.premises, addr.address_line_1].filter(Boolean);
  const line1 = line1Parts.length ? line1Parts.join(' ') : (addr.address_line_1 || addr.premises || '').trim();
  if (!line1 && !addr.postal_code) return null;
  return {
    line1: (line1 || '').trim(),
    line2: (addr.address_line_2 || '').trim() || null,
    city: (addr.locality || addr.region || '').trim() || null,
    postcode: (addr.postal_code || '').trim(),
    country: (addr.country || 'GB').trim(),
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
  };
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
      console.log('Corporate postcode hits in ownerDiscovery:', postcodeHits.length, postcodeHits.slice(0, 5).map((h) => h.ownerName));

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
      await logEvent(jobId, 'info', 'lookupOpenRegister data', {
        register: register,
      });

      const openRegisterOccupants = register?.occupants || [];
      await logEvent(jobId, 'info', 'Open register results', {
        count: openRegisterOccupants.length,
        source: register?.source || null,
        openRegisterOccupants: openRegisterOccupants || null,
      });
      
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
            if (personalMatches) {
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
                address: officer.address || null,
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

      const confirmedMatches = new Set<string>(chAffiliatedNames);
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
