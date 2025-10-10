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

// List root workflows (initial ch-appointments jobs) with child counts
router.get('/progress/workflows', async (req, res) => {
  const schema = z.object({ limit: z.string().optional() });
  const parsed = schema.parse(req.query);
  const limitRaw = parsed.limit ? parseInt(parsed.limit, 10) : 25;
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 100) : 25;

  const rootsQ = `SELECT job_id, queue, name, status, data, created_at, updated_at
                    FROM job_progress
                   WHERE queue IN ('ch-appointments','owner-discovery')
                   ORDER BY updated_at DESC
                   LIMIT ${limit}`;
  const { rows: roots } = await query(rootsQ);

  const out: any[] = [];
  for (const r of roots) {
    const rootId = r.job_id as string;
    const countsQ = `SELECT queue, status, COUNT(*)::int AS c
                       FROM job_progress
                      WHERE data->>'rootJobId' = $1
                      GROUP BY queue, status`;
    const { rows: counts } = await query(countsQ, [rootId]);
    const summarize = (q: string) => {
      const byQ = counts.filter((x: any) => x.queue === q);
      const m: any = { total: 0, completed: 0, running: 0, failed: 0, pending: 0 };
      for (const it of byQ) {
        m.total += Number(it.c || 0);
        const st = (it.status || '').toString();
        if (m[st] !== undefined) m[st] += Number(it.c || 0);
      }
      return m;
    };
    const flowType = r.queue === 'owner-discovery' ? 'owner' : 'company';
    out.push({
      rootJobId: rootId,
      root: r,
      flowType,
      discovery: {
        company: summarize('company-discovery'),
        sitefetch: summarize('site-fetch')
      },
      person: summarize('person-linkedin')
    });
  }
  res.json({ items: out, count: out.length });
});

// Workflow detail: jobs grouped and optional events timeline
router.get('/progress/workflows/:rootJobId', async (req, res) => {
  const { rootJobId } = z.object({ rootJobId: z.string().min(1) }).parse(req.params as any);
  const rootQ = `SELECT job_id, queue, name, status, data, created_at, updated_at FROM job_progress WHERE job_id = $1`;
  const { rows: rootRows } = await query(rootQ, [rootJobId]);
  if (!rootRows.length) return res.status(404).json({ error: 'not_found' });

  const jobsQ = `SELECT job_id, queue, name, status, data, created_at, updated_at
                   FROM job_progress
                  WHERE job_id = $1 OR data->>'rootJobId' = $1
                  ORDER BY updated_at DESC`;
  const { rows: jobs } = await query(jobsQ, [rootJobId]);

  const group = (q: string) => jobs.filter((j: any) => j.queue === q);
  res.json({
    root: rootRows[0],
    jobs: {
      ch: group('ch-appointments'),
      company: group('company-discovery'),
      sitefetch: group('site-fetch'),
      person: group('person-linkedin')
    }
  });
});

// Workflow timeline: all events across child jobs ordered by time
router.get('/progress/workflows/:rootJobId/timeline', async (req, res) => {
  const { rootJobId } = z.object({ rootJobId: z.string().min(1) }).parse(req.params as any);
  const jobsQ = `SELECT job_id FROM job_progress WHERE job_id = $1 OR data->>'rootJobId' = $1`;
  const { rows: jobRows } = await query<{ job_id: string }>(jobsQ, [rootJobId]);
  if (!jobRows.length) return res.json({ events: [], jobs: [] });
  const ids = jobRows.map(r => r.job_id);
  const evQ = `SELECT job_id, ts, level, message, data FROM job_events WHERE job_id = ANY($1::text[]) ORDER BY ts ASC, id ASC`;
  const { rows: events } = await query(evQ, [ids]);
  res.json({ jobs: ids, events });
});
