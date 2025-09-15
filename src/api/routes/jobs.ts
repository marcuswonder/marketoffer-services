import { Router } from "express";
import { z } from "zod";
import { chQ, companyQ, personQ } from "../../queues/index.js";

export const router = Router();

router.post("/jobs/ch-appointments", async (req, res) => {
  // Accept payload from JSON body primarily; fall back to querystring for convenience
  const source: any = { ...(req.query || {}), ...(req.body || {}) };
  const { companyNumber, firstName, lastName, contactId } = z
    .object({
      companyNumber: z.string().min(2),
      firstName: z.string().min(1).optional(),
      lastName: z.string().min(1).optional(),
      contactId: z.string().min(3).optional(),
    })
    .parse(source);

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
    postcode: z.string().optional()
  });
  const data = schema.parse(req.body);
  const jobId = `co:${data.companyNumber || data.companyName}`;
  await companyQ.add("discover", data, { jobId, attempts: 5, backoff: { type: "exponential", delay: 1500 } });
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
