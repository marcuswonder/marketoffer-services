import { query, ensureConnection } from './db.js';
import { logger } from './logger.js';

export async function initDb() {
  await ensureConnection();
  await query(`
    CREATE TABLE IF NOT EXISTS job_progress (
      job_id TEXT PRIMARY KEY,
      queue TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL,
      data JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS job_events (
      id BIGSERIAL PRIMARY KEY,
      job_id TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL DEFAULT now(),
      level TEXT NOT NULL,
      message TEXT NOT NULL,
      data JSONB
    );

    CREATE INDEX IF NOT EXISTS job_events_job_id_idx ON job_events(job_id);

    -- Store enriched CH people and their appointments for downstream processing
    CREATE TABLE IF NOT EXISTS ch_people (
      id BIGSERIAL PRIMARY KEY,
      job_id TEXT NOT NULL,
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
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(job_id, person_key)
    );

    CREATE TABLE IF NOT EXISTS ch_appointments (
      id BIGSERIAL PRIMARY KEY,
      person_id BIGINT NOT NULL REFERENCES ch_people(id) ON DELETE CASCADE,
      appointment_id TEXT NOT NULL,
      company_number TEXT,
      company_name TEXT,
      company_status TEXT,
      registered_address TEXT,
      registered_postcode TEXT,
      sic_codes TEXT[],
      verified_company_website JSONB,
      verified_company_linkedIns JSONB,
      company_website_verification JSONB,
      company_linkedIn_verification JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(person_id, appointment_id)
    );

    -- Backfill columns for existing deployments
    ALTER TABLE ch_people ADD COLUMN IF NOT EXISTS nationality TEXT;
    ALTER TABLE ch_appointments ADD COLUMN IF NOT EXISTS company_status TEXT;
  `);
}

export async function startJob(opts: { jobId: string; queue: string; name: string; payload?: any }) {
  const { jobId, queue, name, payload } = opts;
  console.log('jobId in startJob in lib/progress.ts', jobId);
  console.log('queue in startJob in lib/progress.ts', queue);
  console.log('name in startJob in lib/progress.ts', name);
  console.log('payload in startJob in lib/progress.ts', payload);   
  
  await query(
    `INSERT INTO job_progress(job_id, queue, name, status, data)
     VALUES ($1,$2,$3,'running',$4)
     ON CONFLICT (job_id)
     DO UPDATE SET status='running', data=$4, updated_at=now()`,
    [jobId, queue, name, payload ? JSON.stringify(payload) : null]
  );
}

export async function logEvent(jobId: string, level: 'debug'|'info'|'warn'|'error', message: string, data?: any) {
  await query(
    `INSERT INTO job_events(job_id, level, message, data) VALUES ($1,$2,$3,$4)`,
    [jobId, level, message, data ? JSON.stringify(data) : null]
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
