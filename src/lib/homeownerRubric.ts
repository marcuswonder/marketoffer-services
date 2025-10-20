import { OccupantRecord } from './openRegister.js';
import { logger } from './logger.js';

export type RubricSignal = {
  id: string;
  label: string;
  weight: number;
  value: number; // 1 for positive, -1 for negative, or a scalar in [-1,1]
  score: number; // weight * value (clamped)
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
  confirmedMatches?: Set<string>; // normalised full names
  corporateOccupancy?: boolean;
  latestSaleYear?: number;
};

const NOW_YEAR = new Date().getFullYear();

function normalizeName(name?: string): string {
  return String(name || '')
    .toLowerCase()
    .replace(/^(mr|mrs|ms|miss|dr|prof|sir|dame|lady|lord)\b\.?\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function clamp01(x: number): number {
  return Math.max(0, Math.min(1, x));
}

function pushSignal(arr: RubricSignal[], args: { id: string; label: string; weight: number; value: number; reason?: string; }) {
  const v = typeof args.value === 'number' ? args.value : 1;
  const w = typeof args.weight === 'number' ? args.weight : 0;
  const signed = v >= 0 ? clamp01(v) : -clamp01(-v);
  const score = signed * w;
  arr.push({ id: args.id, label: args.label, weight: w, value: v, score, reason: args.reason || '' });
}

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
  if (!records || !records.length) return [];

  // Pre-compute surname frequencies for weak household signals
  const lastNameCounts = new Map<string, number>();
  for (const rec of records) {
    const key = surname(rec.fullName || '');
    if (!key) continue;
    lastNameCounts.set(key, (lastNameCounts.get(key) || 0) + 1);
  }

  // Normalise confirmed matches to our normalised full-name key
  const confirmed = new Set<string>();
  if (opts.confirmedMatches) {
    for (const n of opts.confirmedMatches) confirmed.add(normalizeName(n));
  }
  const latestSaleYear = typeof opts.latestSaleYear === 'number' && Number.isFinite(opts.latestSaleYear)
    ? opts.latestSaleYear
    : null;

  const candidates: RubricCandidate[] = records.map((rec) => {
    const signals: RubricSignal[] = [];
    const fullName = rec.fullName;
    const last = surname(fullName);
    const firstName = rec.firstName || fullName.split(' ')[0] || '';

    const dataSources = Array.isArray(rec.dataSources) ? rec.dataSources : [];
    const indicators  = Array.isArray(rec.indicators)  ? rec.indicators  : [];

    const hasOR       = indicators.includes('open_register');
    const hasOfficer  = dataSources.includes('companies_house_officer');
    const hasDirector = dataSources.includes('companies_house_director');
    const hasPsc      = dataSources.includes('companies_house_psc');
    // In this pipeline, director/PSC entries only exist if their personal address == prompt
    const chPersonalAtPrompt = hasOfficer || hasDirector || hasPsc;

    // --- Anchor evidence ---
    if (hasOfficer) {
      pushSignal(signals, { id: 'ch_officer_at_prompt', label: 'CH officer personal address at prompt', weight: 0.40, value: 1, reason: 'Officer at prompt address' });
    }
    if (hasDirector) {
      pushSignal(signals, { id: 'ch_director_at_prompt', label: 'CH director personal address at prompt', weight: 0.20, value: 1 });
    }
    if (hasPsc) {
      pushSignal(signals, { id: 'ch_psc_at_prompt', label: 'CH PSC personal address at prompt', weight: 0.20, value: 1 });
    }
    if (hasOR) {
      pushSignal(signals, { id: 'or_present', label: 'Open Register presence', weight: 0.25, value: 1 });
    }
    if (hasOR && chPersonalAtPrompt) {
      pushSignal(signals, { id: 'or_ch_overlap', label: 'Open Register + CH corroboration', weight: 0.10, value: 1 });
    }

    // Confirmed match name boost
    const nm = normalizeName(fullName);
    if (nm && confirmed.has(nm)) {
      pushSignal(signals, { id: 'confirmed_match', label: 'Confirmed by CH name match', weight: 0.10, value: 1 });
    }

    // --- Recency & tenure ---
    const tenure = computeTenure(rec.firstSeenYear, rec.lastSeenYear);
    if (tenure != null) {
      // Long tenure mild positive; very short tenure mild negative
      const weight = 0.10;
      let value = 0;
      if (tenure >= 10) value = 1;
      else if (tenure >= 5) value = 0.6;
      else if (tenure >= 3) value = 0.3;
      else if (tenure <= 1) value = -0.5;
      pushSignal(signals, { id: 'tenure', label: 'Occupancy tenure', weight, value, reason: `~${tenure}y at address` });
    }

    const lastY  = typeof rec.lastSeenYear  === 'number' ? rec.lastSeenYear  : null;
    const firstY = typeof rec.firstSeenYear === 'number' ? rec.firstSeenYear : null;

    if (latestSaleYear && firstY) {
      const diff = firstY - latestSaleYear;
      const reasonBase = `saleYear=${latestSaleYear}, firstSeen=${firstY}${lastY ? `, lastSeen=${lastY}` : ''}`;
      if (diff >= 0) {
        let weight = 0;
        let value = 0;
        if (diff <= 1) {
          weight = 0.18;
          value = 1;
        } else if (diff <= 3) {
          weight = 0.14;
          value = 0.7;
        } else if (diff <= 6) {
          weight = 0.10;
          value = 0.4;
        } else if (diff <= 10) {
          weight = 0.06;
          value = 0.2;
        }
        if (weight > 0) {
          pushSignal(signals, {
            id: 'sale_firstseen_alignment',
            label: 'First seen aligns with latest sale',
            weight,
            value,
            reason: reasonBase,
          });
        }
      } else {
        const yearsBefore = Math.abs(diff);
        let value = -0.3;
        if (yearsBefore >= 5) value = -0.6;
        else if (yearsBefore >= 3) value = -0.4;
        let weight = 0.12;
        if (lastY && lastY >= latestSaleYear) {
          value *= 0.5; // Occupant persisted after sale, soften penalty
        }
        pushSignal(signals, {
          id: 'sale_firstseen_mismatch',
          label: 'Open Register presence predates latest sale',
          weight,
          value,
          reason: reasonBase,
        });
      }
    }

    // Compute age at the time the person was first seen at the property
    const ageAtFirstSeen: number | null = (function() {
      if (!firstY) return null;
      // Prefer birthYear when available for an exact age at firstSeen
      if (typeof rec.birthYear === 'number' && rec.birthYear > 1900 && rec.birthYear <= NOW_YEAR) {
        const age = firstY - rec.birthYear;
        return age >= 0 && age <= 120 ? age : null;
      }
      // Otherwise approximate using current age (from ageBand/birthYear inference) minus elapsed years
      const approxCurrentAge = calcAgeFromBand(rec.ageBand, rec.birthYear);
      if (approxCurrentAge != null) {
        const delta = NOW_YEAR - firstY; // years between firstSeen and now
        const approxAtFirst = approxCurrentAge - delta;
        return approxAtFirst >= 0 && approxAtFirst <= 120 ? approxAtFirst : null;
      }
      return null;
    })();

    if (lastY) {
      const age = NOW_YEAR - lastY;
      let w = 0;
      if (age <= 2) w = 0.20;
      else if (age <= 5) w = 0.15;
      else if (age <= 10) w = 0.10;
      if (w > 0) pushSignal(signals, { id: 'recency', label: 'Recency of occupancy', weight: w, value: 1, reason: `lastSeen=${lastY}` });
    }

    // Historical decay – gentle if we have CH personal-address evidence
    const veryOld = !!(firstY && lastY && firstY < 1995 && lastY < 2010);
    if (veryOld) {
      const penalty = chPersonalAtPrompt ? 0.02 : 0.05;
      pushSignal(signals, { id: 'historical_decay', label: 'Very old register presence', weight: penalty, value: -1, reason: `first=${firstY}, last=${lastY}` });
    }

    // Legacy owner bonus – still plausible owner even if moved long ago (or no recent OR)
    if (chPersonalAtPrompt && (!lastY || (NOW_YEAR - lastY) > 10)) {
      pushSignal(signals, { id: 'legacy_owner_bonus', label: 'Legacy owner evidence (historical CH personal address)', weight: 0.08, value: 1 });
    }

    // Long tenure explicit mild bonus (separate from the general tenure curve)
    if (firstY && lastY && (lastY - firstY) >= 10) {
      pushSignal(signals, { id: 'long_tenure', label: 'Long tenure at address', weight: 0.05, value: 1, reason: `${firstY}-${lastY}` });
    }

    // --- Age / career plausibility (light-touch; use AGE AT FIRST SEEN) ---
    ageAtFirstSeen;
    if (ageAtFirstSeen != null) {
      // Highest-scored band is 35–65 **at the time they first appeared at the property**
      if (ageAtFirstSeen >= 35 && ageAtFirstSeen <= 65) {
        pushSignal(signals, { id: 'age_band_first_seen', label: 'Age 35–65 at first seen', weight: 0.05, value: 1, reason: `~${Math.round(ageAtFirstSeen)} at firstSeen ${firstY}` });
      } else if (ageAtFirstSeen < 30) {
        pushSignal(signals, { id: 'young_at_first_seen', label: 'Under 30 at first seen', weight: 0.05, value: -1, reason: `~${Math.round(ageAtFirstSeen)} at firstSeen ${firstY}` });
      }
    }
    // Optional: basic career tags if you set them upstream
    if ((rec as any).careerTag === 'exec')     pushSignal(signals, { id: 'career_exec', label: 'Senior/stable career', weight: 0.05, value: 1 });
    if ((rec as any).careerTag === 'property') pushSignal(signals, { id: 'career_property', label: 'Property investor/developer', weight: 0.07, value: 1 });
    if ((rec as any).careerTag === 'student')  pushSignal(signals, { id: 'career_student', label: 'Student/early career', weight: 0.05, value: -1 });

    // --- Conflicts / negatives ---
    const uniqueSurnames = lastNameCounts.size;
    if (uniqueSurnames >= 3 && (rec.dataSources || []).length <= 1) {
      // Only as a weak hint, and do not over-penalise if strong CH/OR evidence exists
      const penalty = chPersonalAtPrompt || hasOR ? 0.00 : 0.12;
      if (penalty > 0) pushSignal(signals, { id: 'unrelated_household', label: 'Unrelated household pattern', weight: penalty, value: -0.7, reason: 'Multiple unrelated surnames' });
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

  const sorted = candidates.slice().sort((a, b) => b.score - a.score);
  sorted.forEach((c, i) => { c.rank = i + 1; });

  logger.debug({
    occupants: records.length,
    bestScore: sorted[0]?.score ?? 0,
  }, 'Rubric scoring complete');

  return sorted;
}
