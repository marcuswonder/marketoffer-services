import { Router } from 'express';
import { z } from 'zod';
import { query } from '../../lib/db.js';

export const router = Router();

router.get('/progress/jobs', async (req, res) => {
  const schema = z.object({
    limit: z.string().optional(),
    queue: z.string().optional(),
    status: z.string().optional(),
  });
  const parsed = schema.parse(req.query);
  const limitRaw = parsed.limit ? parseInt(parsed.limit, 10) : 50;
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 200) : 50;

  const where: string[] = [];
  const params: any[] = [];
  if (parsed.queue) { params.push(parsed.queue); where.push(`queue = $${params.length}`); }
  if (parsed.status) { params.push(parsed.status); where.push(`status = $${params.length}`); }
  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
  const sql = `SELECT job_id, queue, name, status, data, created_at, updated_at
               FROM job_progress
               ${whereSql}
               ORDER BY updated_at DESC
               LIMIT ${limit}`;
  const { rows } = await query(sql, params);
  res.json({ items: rows, count: rows.length });
});

router.get('/progress/jobs/:jobId', async (req, res) => {
  const { jobId } = z.object({ jobId: z.string().min(1) }).parse(req.params as any);
  const progQ = `SELECT job_id, queue, name, status, data, created_at, updated_at FROM job_progress WHERE job_id = $1`;
  const evQ = `SELECT ts, level, message, data FROM job_events WHERE job_id = $1 ORDER BY ts ASC, id ASC`;
  const [{ rows: progRows }, { rows: eventRows }] = await Promise.all([
    query(progQ, [jobId]),
    query(evQ, [jobId])
  ]);
  if (!progRows.length) return res.status(404).json({ error: 'not_found' });
  res.json({ progress: progRows[0], events: eventRows });
});

