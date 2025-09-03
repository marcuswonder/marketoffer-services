import "dotenv/config";
import express from "express";
import { logger } from "../lib/logger.js";
import { router as jobsRouter } from "./routes/jobs.js";
import { ExpressAdapter } from "@bull-board/express";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { createBullBoard } from "@bull-board/api";
import { chQ, companyQ, personQ } from "../queues/index.js";

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req,res)=>res.json({ ok: true }));

app.use("/api", jobsRouter);

// Bull Board UI at /admin/queues
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");
createBullBoard({
  queues: [new BullMQAdapter(chQ), new BullMQAdapter(companyQ), new BullMQAdapter(personQ)],
  serverAdapter,
});
app.use("/admin/queues", serverAdapter.getRouter());

const port = Number(process.env.PORT || 3000);
app.listen(port, () => logger.info({ port }, "API listening"));
