import { Router } from "express";
import { z } from "zod";
import { chQ, companyQ, personQ, ownerQ } from "../../queues/index.js";
import { addressKey } from "../../lib/address.js";

export const router = Router();

router.post("/jobs/ch-appointments", async (req, res) => {
  // Accept payload from JSON body primarily; fall back to querystring for convenience
  const source: any = { ...(req.query || {}), ...(req.body || {}) };
  // Treat empty strings as undefined for optional fields
  const cleaned: any = { ...source };
  ["firstName", "lastName", "contactId"].forEach(k => {
    if (typeof cleaned[k] === 'string' && cleaned[k].trim() === '') delete cleaned[k];
  });
  const schema = z.object({
    companyNumber: z.string().min(2),
    firstName: z.string().min(1).optional(),
    lastName: z.string().min(1).optional(),
    contactId: z.string().min(3).optional(),
  });
  const parsed = schema.safeParse(cleaned);
  if (!parsed.success) {
    return res.status(400).json({ error: 'invalid_request', issues: parsed.error.issues });
  }
  const { companyNumber, firstName, lastName, contactId } = parsed.data;

  const jobId = `ch:${companyNumber}:${(firstName || "").toLowerCase()}:${(lastName || "").toLowerCase()}:${contactId || ""}`;
  await chQ.add(
    "fetch",
    { companyNumber, firstName, lastName, contactId },
    { jobId, attempts: 5, backoff: { type: "exponential", delay: 1000 } }
  );
  res.json({ jobId });
});

router.post("/jobs/company-discovery", async (req, res) => {
  const schema = z.object({
    companyNumber: z.string().optional(),
    companyName: z.string().optional(),
    address: z.string().optional(),
    postcode: z.string().optional(),
    requestSource: z.string().optional(),
  });
  const data = schema.parse(req.body);
  const jobId = `co:${data.companyNumber || data.companyName}`;
  const payload = {
    ...data,
    requestSource: data.requestSource && data.requestSource.trim()
      ? data.requestSource.trim()
      : 'company-request',
  };
  await companyQ.add("discover", payload, { jobId, attempts: 5, backoff: { type: "exponential", delay: 1500 } });
  res.json({ jobId });
});

router.post("/jobs/person-linkedin", async (req, res) => {
  // Accept either { personId } OR a full payload { person, context, rootJobId }
  const body: any = req.body || {};
  const byId = z.object({ personId: z.string().min(3) }).safeParse(body);
  let data: any = null;
  let jobId: string;
  if (byId.success) {
    data = { personId: byId.data.personId };
    jobId = `person:${byId.data.personId}`;
  } else {
    const byPayload = z.object({
      person: z.object({
        firstName: z.string().min(1),
        middleNames: z.string().optional(),
        lastName: z.string().min(1),
        dob: z.string().optional()
      }),
      context: z.object({
        companyNumber: z.string().optional(),
        companyName: z.string().optional(),
        websites: z.array(z.string()).optional(),
        companyLinkedIns: z.array(z.string()).optional(),
        personalLinkedIns: z.array(z.string()).optional()
      }).optional(),
      rootJobId: z.string().optional(),
      jobId: z.string().optional()
    }).safeParse(body);
    if (!byPayload.success) {
      return res.status(400).json({ error: 'invalid_request' });
    }
    data = { person: byPayload.data.person, context: byPayload.data.context, rootJobId: byPayload.data.rootJobId };
    jobId = byPayload.data.jobId || `person:${Date.now()}`;
  }
  await personQ.add("discover", data, { jobId, attempts: 5, backoff: { type: "exponential", delay: 2000 } });
  res.json({ jobId });
});

router.post("/jobs/owner-discovery", async (req, res) => {
  const optionalTrimmed = z.string().trim().transform((val) => (val === '' ? undefined : val)).optional();
  const requiredLine = z.string().trim().min(1);
  const requiredCity = z.string().trim().min(1);
  const requiredPostcode = z.string().trim().min(3);
  const schema = z.object({
    address: z.object({
      unit: optionalTrimmed,
      buildingName: optionalTrimmed,
      line1: requiredLine,
      line2: optionalTrimmed,
      city: requiredCity,
      postcode: requiredPostcode,
      country: optionalTrimmed,
    }),
    rootJobId: z.string().optional(),
    metadata: z.record(z.any()).optional(),
    allowCorporateQueue: z.boolean().optional(),
  });
  const parsed = schema.safeParse(req.body || {});
  if (!parsed.success) {
    return res.status(400).json({ error: 'invalid_request', issues: parsed.error.issues });
  }
  const data = parsed.data;
  const key = addressKey(data.address);
  const jobId = `owner:${key || `${Date.now()}`}`;
  await ownerQ.add(
    'discover',
    data,
    { jobId, attempts: 5, backoff: { type: 'exponential', delay: 1500 } }
  );
  res.json({ jobId });
});
