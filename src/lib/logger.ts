import pino from "pino";
import { createRequire } from "module";

const env = process.env.NODE_ENV || "development";
const level = process.env.LOG_LEVEL || (env === "production" ? "info" : "debug");

let transport: any;
if (env !== "production") {
  try {
    const require = createRequire(import.meta.url);
    require.resolve("pino-pretty");
    transport = {
      target: "pino-pretty",
      options: {
        colorize: true,
      },
    };
  } catch {
    transport = undefined;
  }
}

export const logger = pino({ level, transport });
