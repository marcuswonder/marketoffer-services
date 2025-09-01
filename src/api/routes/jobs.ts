import { Router } from "express";
import { z } from "zod";
import { chQ, companyQ, personQ } from "../../queues/index.js";

export const router = Router();

router.post("/jobs/ch-appointments", async (req, res) => {
  const { companyNumber } = z.object({ companyNumber: z.string().min(2) }).parse(req.body);
  const jobId = `ch:${companyNumber}`;
  await chQ.add("fetch", { companyNumber }, { jobId, attempts: 5, backoff: { type: "exponential", delay: 1000 } });
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
