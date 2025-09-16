import "dotenv/config";
import express from "express";
import { logger } from "../lib/logger.js";
import { router as jobsRouter } from "./routes/jobs.js";
import { router as progressRouter } from "./routes/progress.js";
import { router as uiRouter } from "./routes/ui.js";
import { router as settingsRouter } from "./routes/settings.js";
import { router as requestRouter } from "./routes/request.js";
import { ExpressAdapter } from "@bull-board/express";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { createBullBoard } from "@bull-board/api";
import { chQ, companyQ, personQ, siteFetchQ } from "../queues/index.js";
import { initDb } from "../lib/progress.js";

await initDb();
const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req,res)=>res.json({ ok: true }));

app.use("/api", jobsRouter);
app.use("/api", progressRouter);
app.use("/api", settingsRouter);

// Bull Board UI at /admin/queues
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");
createBullBoard({
  queues: [
    new BullMQAdapter(chQ),
    new BullMQAdapter(companyQ),
    new BullMQAdapter(personQ),
    new BullMQAdapter(siteFetchQ),
  ],
  serverAdapter,
});
app.use("/admin/queues", serverAdapter.getRouter());
app.use("/admin/workflows", uiRouter);
app.use("/admin/request", requestRouter);

const port = Number(process.env.PORT || 3000);
app.listen(port, () => logger.info({ port }, "API listening"));
