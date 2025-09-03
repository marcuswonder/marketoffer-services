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
  const { personId } = z.object({ personId: z.string().min(3) }).parse(req.body);
  const jobId = `person:${personId}`;
  await personQ.add("discover", { personId }, { jobId, attempts: 5, backoff: { type: "exponential", delay: 2000 } });
  res.json({ jobId });
});
