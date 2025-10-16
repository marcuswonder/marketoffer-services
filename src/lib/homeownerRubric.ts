import { OccupantRecord } from './openRegister.js';
import { logger } from './logger.js';

export type RubricSignal = {
  id: string;
  label: string;
  weight: number;
  value: number;
  score: number;
  reason: string;
};

export type RubricCandidate = {
  fullName: string;
  firstName: string;
  lastName: string;
  score: number;
  rank: number;
  signals: RubricSignal[];
  sources: string[];
};

export type RubricOptions = {
  confirmedMatches?: Set<string>;
  corporateOccupancy?: boolean;
};

const NOW_YEAR = new Date().getFullYear();

function calcAgeFromBand(ageBand?: string, birthYear?: number): number | null {
  if (typeof birthYear === 'number' && birthYear > 1900 && birthYear <= NOW_YEAR) {
    return NOW_YEAR - birthYear;
  }
  if (!ageBand) return null;
  const trimmed = ageBand.trim();
  const match = trimmed.match(/(\d{2})[-+]?/);
  if (match) return Number(match[1]);
  return null;
}

function computeTenure(firstSeen?: number, lastSeen?: number): number | null {
  if (!firstSeen) return null;
  const end = lastSeen || NOW_YEAR;
  return end - firstSeen + 1;
}

function surname(fullName: string): string {
  return (fullName.split(/\s+/).pop() || '').toLowerCase();
}

export function scoreOccupants(records: OccupantRecord[], opts: RubricOptions = {}): RubricCandidate[] {
  if (!records.length) return [];
  const lastNameCounts = new Map<string, number>();
  for (const rec of records) {
    const key = surname(rec.fullName);
    if (!key) continue;
    lastNameCounts.set(key, (lastNameCounts.get(key) || 0) + 1);
  }

  const confirmed = opts.confirmedMatches || new Set<string>();

  const candidates = records.map((rec) => {
    const signals: RubricSignal[] = [];
    const fullName = rec.fullName;
    const last = surname(fullName);
    const firstName = rec.firstName || fullName.split(' ')[0] || '';

    // Shared surname (positive)
    if (last) {
      const count = lastNameCounts.get(last) || 0;
      const weight = 0.18;
      const value = count >= 2 ? 1 : count === 1 ? 0.2 : 0;
      if (value > 0) {
        signals.push({
          id: 'shared_surname',
          label: 'Shared surname',
          weight,
          value,
          score: weight * value,
          reason: count >= 2 ? 'Surname shared with other occupants' : 'Single occurrence of surname',
        });
      }
    }

    // Long tenure
    const tenure = computeTenure(rec.firstSeenYear, rec.lastSeenYear);
    if (tenure != null) {
      const weight = 0.22;
      let value = 0;
      if (tenure >= 7) value = 1;
      else if (tenure >= 5) value = 0.7;
      else if (tenure >= 3) value = 0.4;
      else if (tenure <= 1) value = -0.5;
      signals.push({
        id: 'tenure',
        label: 'Occupancy tenure',
        weight,
        value,
        score: weight * value,
        reason: `Registered for ~${tenure} years`,
      });
    }

    // Age signals
    const age = calcAgeFromBand(rec.ageBand, rec.birthYear);
    if (age != null) {
      const weight = 0.2;
      let value = 0;
      let reason = '';
      if (age >= 35) {
        value = 1;
        reason = `Estimated age ${age}`;
      } else if (age >= 30) {
        value = 0.5;
        reason = `Estimated age ${age}`;
      } else if (age < 27) {
        value = -0.6;
        reason = `Estimated age ${age}`;
      }
      if (value !== 0) {
        signals.push({
          id: 'age_band',
          label: 'Age profile',
          weight,
          value,
          score: weight * value,
          reason,
        });
      }
    }

    // Homeowner indicator flag
    const homeownerFlag = (rec.indicators || []).some((flag) => /homeowner/i.test(flag)) ||
      (rec.dataSources || []).some((src) => src === 'HOMEOWNER' || src === 'BROKER_HOMEOWNER');
    if (homeownerFlag) {
      const weight = 0.28;
      signals.push({
        id: 'homeowner_flag',
        label: 'Homeowner flag',
        weight,
        value: 1,
        score: weight,
        reason: 'Third-party data flags as homeowner',
      });
    }

    // Confirmed company house match (treat as decisive when name + address align)
    const isConfirmed = confirmed.has(fullName.toLowerCase());
    if (isConfirmed) {
      const weight = 1.0;
      signals.push({
        id: 'ch_match',
        label: 'Name + address officer match',
        weight,
        value: 1,
        score: weight,
        reason: 'Registered officer/company at address',
      });
    }
    
    // Negative signal: multiple unrelated adults (skip if confirmed match already exists)
    const uniqueSurnames = lastNameCounts.size;
    if (!isConfirmed && uniqueSurnames >= 3 && (rec.dataSources || []).length <= 1) {
      const weight = 0.16;
      signals.push({
        id: 'unrelated_household',
        label: 'Unrelated household pattern',
        weight,
        value: -0.7,
        score: -0.7 * weight,
        reason: 'Multiple unrelated surnames at property',
      });
    }

    const score = signals.reduce((acc, sig) => acc + sig.score, 0);
    return {
      fullName,
      firstName,
      lastName: last,
      score,
      rank: 0,
      signals,
      sources: rec.dataSources || [],
    } as RubricCandidate;
  });

  const sorted = candidates
    .map((c) => ({ ...c }))
    .sort((a, b) => b.score - a.score);
  sorted.forEach((candidate, idx) => {
    candidate.rank = idx + 1;
  });

  logger.debug({
    occupants: records.length,
    bestScore: sorted[0]?.score ?? 0,
  }, 'Rubric scoring complete');
  return sorted;
}
