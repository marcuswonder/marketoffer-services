import { query, ensureConnection } from './db.js';
import { logger } from './logger.js';

// Cache jobId -> queue/root to enrich events without extra DB lookups
const jobQueueCache = new Map<string, string>();
const jobRootCache = new Map<string, string>();

function slugify(s: string) {
  return (s || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 64);
}

let initDbRun: Promise<void> | null = null;

export async function initDb() {
  if (initDbRun) {
    await initDbRun;
    return;
  }
  initDbRun = (async () => {
    await ensureConnection();
    const LOCK_KEY = 902173; // arbitrary constant to serialize schema migrations
    await query('SELECT pg_advisory_lock($1)', [LOCK_KEY]);
    try {
      await query(`
        CREATE TABLE IF NOT EXISTS job_progress (
          job_id TEXT PRIMARY KEY,
          queue TEXT NOT NULL,
          name TEXT NOT NULL,
          status TEXT NOT NULL,
          data JSONB,
          root_job_id TEXT,
          parent_job_id TEXT,
          request_source TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS job_events (
          id BIGSERIAL PRIMARY KEY,
          job_id TEXT NOT NULL,
          root_job_id TEXT,
          ts TIMESTAMPTZ NOT NULL DEFAULT now(),
          level TEXT NOT NULL,
          message TEXT NOT NULL,
          data JSONB
        );

        CREATE INDEX IF NOT EXISTS job_events_job_id_idx ON job_events(job_id);

        -- Store enriched CH people and their appointments for downstream processing
        CREATE TABLE IF NOT EXISTS ch_companies (
          id BIGSERIAL PRIMARY KEY,
          company_number TEXT NOT NULL UNIQUE,
          name TEXT,
          registered_address TEXT,
          registered_postcode TEXT,
          status TEXT,
          sic_codes TEXT[],
          discovery_status TEXT NOT NULL DEFAULT 'pending',
          discovery_job_id TEXT,
          discovery_error TEXT,
          discovered_websites JSONB,
          discovered_linkedins JSONB,
          metadata JSONB,
          first_discovered_at TIMESTAMPTZ,
          last_discovered_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS ch_people (
          id BIGSERIAL PRIMARY KEY,
          job_id TEXT NOT NULL,
          root_job_id TEXT,
          person_key TEXT NOT NULL,
          contact_id TEXT,
          first_name TEXT,
          middle_names TEXT,
          last_name TEXT,
          full_name TEXT,
          nationality TEXT,
          dob_month INTEGER,
          dob_year INTEGER,
          dob_string TEXT,
          officer_ids TEXT[],
          status TEXT,
          verified_director_linkedIns JSONB,
          director_linkedIn_verification JSONB,
          discovery_job_ids TEXT[],
          sitefetch_job_ids TEXT[],
          person_linkedin_job_ids TEXT[],
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(job_id, person_key)
        );

        CREATE INDEX IF NOT EXISTS ch_companies_number_idx ON ch_companies(company_number);

        CREATE TABLE IF NOT EXISTS ch_appointments (
          id BIGSERIAL PRIMARY KEY,
          person_id BIGINT NOT NULL REFERENCES ch_people(id) ON DELETE CASCADE,
          appointment_id TEXT NOT NULL,
          company_number TEXT,
          company_name TEXT,
          trading_name TEXT,
          company_status TEXT,
          registered_address TEXT,
          registered_postcode TEXT,
          sic_codes TEXT[],
          verified_company_website JSONB,
          verified_company_linkedIns JSONB,
          verified_director_linkedIns JSONB,
          company_website_verification JSONB,
          company_linkedIn_verification JSONB,
          director_linkedIn_verification JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(person_id, appointment_id)
        );

        -- Backfill columns for existing deployments
        ALTER TABLE job_progress ADD COLUMN IF NOT EXISTS root_job_id TEXT;
        ALTER TABLE job_progress ADD COLUMN IF NOT EXISTS parent_job_id TEXT;
        ALTER TABLE job_progress ADD COLUMN IF NOT EXISTS request_source TEXT;
        ALTER TABLE job_events ADD COLUMN IF NOT EXISTS root_job_id TEXT;

        CREATE INDEX IF NOT EXISTS job_progress_root_idx ON job_progress(root_job_id);
        CREATE INDEX IF NOT EXISTS job_progress_parent_idx ON job_progress(parent_job_id);
        CREATE INDEX IF NOT EXISTS job_events_root_idx ON job_events(root_job_id);

        ALTER TABLE ch_appointments ADD COLUMN IF NOT EXISTS company_id BIGINT REFERENCES ch_companies(id);
        CREATE INDEX IF NOT EXISTS ch_appointments_company_id_idx ON ch_appointments(company_id);

        CREATE TABLE IF NOT EXISTS ch_company_people (
          company_id BIGINT NOT NULL REFERENCES ch_companies(id) ON DELETE CASCADE,
          person_id BIGINT NOT NULL REFERENCES ch_people(id) ON DELETE CASCADE,
          roles TEXT[] DEFAULT '{}'::text[],
          first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(company_id, person_id)
        );
        CREATE INDEX IF NOT EXISTS ch_company_people_person_idx ON ch_company_people(person_id);

        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS nationality TEXT;
        ALTER TABLE ch_appointments ADD COLUMN IF NOT EXISTS company_status TEXT;
        ALTER TABLE ch_appointments ADD COLUMN IF NOT EXISTS trading_name TEXT;
        ALTER TABLE ch_appointments ADD COLUMN IF NOT EXISTS verified_director_linkedIns JSONB;
        ALTER TABLE ch_appointments ADD COLUMN IF NOT EXISTS director_linkedIn_verification JSONB;
        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS verified_director_linkedIns JSONB;
        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS director_linkedIn_verification JSONB;
        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS root_job_id TEXT;
        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS discovery_job_ids TEXT[];
        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS sitefetch_job_ids TEXT[];
        ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS person_linkedin_job_ids TEXT[];
        CREATE INDEX IF NOT EXISTS ch_appointments_company_number_idx ON ch_appointments(company_number);

        CREATE TABLE IF NOT EXISTS owner_properties (
          id BIGSERIAL PRIMARY KEY,
          job_id TEXT NOT NULL,
          root_job_id TEXT,
          address_line1 TEXT NOT NULL,
          address_line2 TEXT,
          city TEXT,
          postcode TEXT NOT NULL,
          country TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          owner_type TEXT,
          corporate_owner JSONB,
          resolution JSONB,
          candidate_summary JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(job_id)
        );

        CREATE TABLE IF NOT EXISTS owner_candidates (
          id BIGSERIAL PRIMARY KEY,
          property_id BIGINT NOT NULL REFERENCES owner_properties(id) ON DELETE CASCADE,
          full_name TEXT NOT NULL,
          first_name TEXT,
          last_name TEXT,
          score NUMERIC(6,4),
          rank INTEGER,
          sources TEXT[],
          evidence JSONB,
          normalized_full_name TEXT,
          company_numbers TEXT[],
          officer_ids TEXT[],
          ch_person_id BIGINT REFERENCES ch_people(id),
          outcome TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS owner_signals (
          id BIGSERIAL PRIMARY KEY,
          candidate_id BIGINT NOT NULL REFERENCES owner_candidates(id) ON DELETE CASCADE,
          signal_id TEXT NOT NULL,
          label TEXT,
          weight NUMERIC(6,4),
          value NUMERIC(6,4),
          score NUMERIC(6,4),
          reason TEXT,
          evidence JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS owner_properties_job_idx ON owner_properties(job_id);
        CREATE INDEX IF NOT EXISTS owner_candidates_property_id_idx ON owner_candidates(property_id);

        CREATE TABLE IF NOT EXISTS land_registry_corporate (
          address_key TEXT PRIMARY KEY,
          owner_name TEXT NOT NULL,
          company_number TEXT,
          dataset_label TEXT,
          raw JSONB
        );

        CREATE TABLE IF NOT EXISTS land_registry_corporate_meta (
          dataset_label TEXT PRIMARY KEY,
          last_refreshed_at TIMESTAMPTZ,
          source_url TEXT,
          row_count INTEGER
        );

        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS discovery_status TEXT NOT NULL DEFAULT 'pending';
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS discovery_job_id TEXT;
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS discovery_error TEXT;
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS discovered_websites JSONB;
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS discovered_linkedins JSONB;
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS metadata JSONB;
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS first_discovered_at TIMESTAMPTZ;
        ALTER TABLE ch_companies ADD COLUMN IF NOT EXISTS last_discovered_at TIMESTAMPTZ;

        ALTER TABLE owner_candidates ADD COLUMN IF NOT EXISTS normalized_full_name TEXT;
        ALTER TABLE owner_candidates ADD COLUMN IF NOT EXISTS company_numbers TEXT[];
        ALTER TABLE owner_candidates ADD COLUMN IF NOT EXISTS officer_ids TEXT[];
        ALTER TABLE owner_candidates ADD COLUMN IF NOT EXISTS ch_person_id BIGINT REFERENCES ch_people(id);
        CREATE INDEX IF NOT EXISTS owner_candidates_normalized_idx ON owner_candidates(normalized_full_name);

        ALTER TABLE land_registry_corporate_meta ADD COLUMN IF NOT EXISTS dataset_label TEXT;
        ALTER TABLE land_registry_corporate_meta ADD COLUMN IF NOT EXISTS last_refreshed_at TIMESTAMPTZ;
        ALTER TABLE land_registry_corporate_meta ADD COLUMN IF NOT EXISTS source_url TEXT;
        ALTER TABLE land_registry_corporate_meta ADD COLUMN IF NOT EXISTS row_count INTEGER;
        UPDATE land_registry_corporate_meta SET dataset_label = COALESCE(dataset_label, 'legacy');
        CREATE UNIQUE INDEX IF NOT EXISTS land_registry_corporate_meta_label_idx ON land_registry_corporate_meta(dataset_label);

        -- Backfill canonical company table and link appointments
        INSERT INTO ch_companies (company_number, name, status, registered_address, registered_postcode, sic_codes)
        SELECT DISTINCT
          COALESCE(NULLIF(TRIM(a.company_number), ''), CONCAT('unknown:', a.id)) AS company_number,
          NULLIF(TRIM(a.company_name), '') AS name,
          NULLIF(TRIM(a.company_status), '') AS status,
          NULLIF(TRIM(a.registered_address), '') AS registered_address,
          NULLIF(TRIM(a.registered_postcode), '') AS registered_postcode,
          CASE WHEN a.sic_codes IS NOT NULL AND CARDINALITY(a.sic_codes) > 0 THEN a.sic_codes ELSE NULL END AS sic_codes
        FROM ch_appointments a
        WHERE COALESCE(TRIM(a.company_number), '') <> ''
        ON CONFLICT (company_number)
        DO UPDATE SET
          name = COALESCE(EXCLUDED.name, ch_companies.name),
          status = COALESCE(EXCLUDED.status, ch_companies.status),
          registered_address = COALESCE(EXCLUDED.registered_address, ch_companies.registered_address),
          registered_postcode = COALESCE(EXCLUDED.registered_postcode, ch_companies.registered_postcode),
          sic_codes = CASE
            WHEN EXCLUDED.sic_codes IS NOT NULL AND CARDINALITY(EXCLUDED.sic_codes) > 0 THEN EXCLUDED.sic_codes
            ELSE ch_companies.sic_codes
          END,
          updated_at = now();

        UPDATE ch_appointments a
        SET company_id = c.id
        FROM ch_companies c
        WHERE c.company_number = a.company_number
          AND a.company_id IS DISTINCT FROM c.id;

        INSERT INTO ch_company_people (company_id, person_id, roles)
        SELECT DISTINCT
          c.id,
          a.person_id,
          ARRAY['officer']::text[]
        FROM ch_appointments a
        JOIN ch_companies c ON c.company_number = a.company_number
        WHERE a.person_id IS NOT NULL
        ON CONFLICT (company_id, person_id)
        DO UPDATE SET
          roles = ARRAY(SELECT DISTINCT UNNEST(COALESCE(ch_company_people.roles,'{}') || EXCLUDED.roles)),
          last_seen_at = now(),
          updated_at = now();
      `);
    } finally {
      await query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]);
    }
  })();

  try {
    await initDbRun;
  } catch (err) {
    initDbRun = null;
    throw err;
  }
}

export type StartJobOptions = {
  jobId: string;
  queue: string;
  name: string;
  payload?: any;
  rootJobId?: string | null;
  parentJobId?: string | null;
  requestSource?: string | null;
};

export async function startJob(opts: StartJobOptions) {
  const { jobId, queue, name, payload, rootJobId, parentJobId, requestSource } = opts;
  const resolvedRoot = (rootJobId && rootJobId.trim()) || jobId;
  const resolvedParent = parentJobId && parentJobId.trim() ? parentJobId.trim() : null;
  const resolvedSource = requestSource && requestSource.trim() ? requestSource.trim() : null;
  console.log('jobId in startJob in lib/progress.ts', jobId);
  console.log('queue in startJob in lib/progress.ts', queue);
  console.log('name in startJob in lib/progress.ts', name);
  console.log('payload in startJob in lib/progress.ts', payload);
  console.log('rootJobId in startJob in lib/progress.ts', resolvedRoot);
  console.log('parentJobId in startJob in lib/progress.ts', resolvedParent);
  console.log('requestSource in startJob in lib/progress.ts', resolvedSource);

  await query(
    `INSERT INTO job_progress(job_id, queue, name, status, data, root_job_id, parent_job_id, request_source)
     VALUES ($1,$2,$3,'running',$4,$5,$6,$7)
     ON CONFLICT (job_id)
     DO UPDATE SET status='running',
                   data=$4,
                   root_job_id=$5,
                   parent_job_id=$6,
                   request_source=$7,
                   updated_at=now()`,
    [
      jobId,
      queue,
      name,
      payload ? JSON.stringify(payload) : null,
      resolvedRoot,
      resolvedParent,
      resolvedSource,
    ]
  );
  try {
    jobQueueCache.set(jobId, queue);
    jobRootCache.set(jobId, resolvedRoot);
  } catch {}
}

async function getJobQueue(jobId: string): Promise<string> {
  let q = jobQueueCache.get(jobId);
  if (q) return q;
  try {
    const { rows } = await query<{ queue: string }>(
      `SELECT queue FROM job_progress WHERE job_id = $1 LIMIT 1`,
      [jobId]
    );
    q = rows?.[0]?.queue || 'general';
  } catch {
    q = 'general';
  }
  jobQueueCache.set(jobId, q);
  return q;
}

async function getJobRoot(jobId: string): Promise<string> {
  let root = jobRootCache.get(jobId);
  if (root) return root;
  try {
    const { rows } = await query<{ root_job_id: string | null }>(
      `SELECT root_job_id FROM job_progress WHERE job_id = $1 LIMIT 1`,
      [jobId]
    );
    root = rows?.[0]?.root_job_id || jobId;
  } catch {
    root = jobId;
  }
  jobRootCache.set(jobId, root);
  return root;
}

export async function logEvent(jobId: string, level: 'debug'|'info'|'warn'|'error', message: string, data?: any) {
  // Enrich with scope/category/code when not provided
  let enriched: any = data && typeof data === 'object' ? { ...data } : {};
  const rootJobId = await getJobRoot(jobId);
  if (!('scope' in enriched)) {
    enriched.scope = level === 'debug' ? 'trace' : (level === 'info' ? 'detail' : 'summary');
  }
  if (!('category' in enriched) || !enriched.category) {
    const q = await getJobQueue(jobId);
    enriched.category = q;
  }
  if (!('code' in enriched) || !enriched.code) {
    enriched.code = `${enriched.category}.${slugify(message)}`;
  }
  if (!('rootJobId' in enriched)) {
    enriched.rootJobId = rootJobId;
  }
  await query(
    `INSERT INTO job_events(job_id, root_job_id, level, message, data) VALUES ($1,$2,$3,$4,$5)`,
    [jobId, rootJobId, level, message, JSON.stringify(enriched)]
  );
}

export async function completeJob(jobId: string, data?: any) {
  await query(
    `UPDATE job_progress SET status='completed', data=$2, updated_at=now() WHERE job_id=$1`,
    [jobId, data ? JSON.stringify(data) : null]
  );
}

export async function failJob(jobId: string, err: any) {
  try {
    await query(
      `UPDATE job_progress SET status='failed', data=$2, updated_at=now() WHERE job_id=$1`,
      [jobId, JSON.stringify({ error: String(err) })]
    );
    await logEvent(jobId, 'error', 'job failed', { error: String(err) });
  } catch (e) {
    logger.error({ jobId, e: String(e) }, 'Failed to record job failure');
  }
}
