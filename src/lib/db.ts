import { Pool } from 'pg';
import { logger } from './logger.js';

// Build the connection string from environment variables, falling back to sensible defaults.
// This avoids the old hardcoded app:app@app/app values.
const PGHOST = process.env.POSTGRES_HOST || 'postgres';
const PGPORT = process.env.POSTGRES_PORT || '5432';
const PGUSER = process.env.POSTGRES_USER || 'app';
const PGPASSWORD = process.env.POSTGRES_PASSWORD || 'app';
const PGDATABASE = process.env.POSTGRES_DB || 'app';

const FALLBACK_URL = `postgres://${encodeURIComponent(PGUSER)}:${encodeURIComponent(PGPASSWORD)}@${PGHOST}:${PGPORT}/${PGDATABASE}`;
const DATABASE_URL = process.env.DATABASE_URL || FALLBACK_URL;

export const pool = new Pool({ connectionString: DATABASE_URL });

export async function query<T = any>(text: string, params?: any[]): Promise<{ rows: T[] }> {
  const client = await pool.connect();
  try {
    const res = await client.query(text, params);
    // console.log('res in query in lib/db.ts', res);
    return res as any;
  } finally {
    client.release();
  }
}

export async function ensureConnection(retries = 10, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      await query('select 1');
      return;
    } catch (err) {
      const wait = Math.min(delayMs * (i + 1), 5000);
      logger.warn({ attempt: i + 1, wait, err: String(err) }, 'Postgres not ready, retrying');
      await new Promise(r => setTimeout(r, wait));
    }
  }
  // final try to throw
  await query('select 1');
}
