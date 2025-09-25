import "dotenv/config";
import express from "express";
import cookieParser from "cookie-parser";
import { logger } from "../lib/logger.js";
import { basicAuth, handleLogin, handleLogout } from "./middleware/basic_auth.js";
import { router as jobsRouter } from "./routes/jobs.js";
import { router as progressRouter } from "./routes/progress.js";
import { router as uiRouter } from "./routes/ui.js";
import { router as requestsUiRouter } from "./routes/requests_ui.js";
import { router as progressUiRouter } from "./routes/progress_ui.js";
import { router as settingsRouter } from "./routes/settings.js";
import { router as requestRouter } from "./routes/request.js";
import { router as settingsUiRouter } from "./routes/settings_ui.js";
import { router as homeRouter } from "./routes/home.js";
import { router as adminRouter } from "./routes/admin.js";
import { ExpressAdapter } from "@bull-board/express";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { createBullBoard } from "@bull-board/api";
import { chQ, companyQ, personQ, siteFetchQ } from "../queues/index.js";
import { initDb } from "../lib/progress.js";

await initDb();
const app = express();
const cookieSecret = process.env.COOKIE_SECRET || "";
app.use(cookieParser(cookieSecret));
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req,res)=>res.json({ ok: true }));
app.get("/login", handleLogin);
app.post("/login", handleLogin);
app.post("/logout", handleLogout);

app.use(["/api", "/admin"], basicAuth);

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
app.use("/admin/requests", requestsUiRouter);
app.use("/admin/progress", progressUiRouter);
app.use("/admin/request", requestRouter);
app.use("/admin/settings", settingsUiRouter);
app.use("/admin", adminRouter);
app.use("/", homeRouter);

const port = Number(process.env.PORT || 3000);
app.listen(port, () => logger.info({ port }, "API listening"));
