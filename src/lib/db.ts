import { Pool } from 'pg';
import { logger } from './logger.js';

const DEFAULT_URL = 'postgres://app:app@postgres:5432/app';
const DATABASE_URL = process.env.DATABASE_URL || DEFAULT_URL;

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

