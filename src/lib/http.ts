import { setTimeout as delay } from "timers/promises";
import { fetch } from "undici";
import { logger } from "./logger.js";

export type HttpOpts = {
  headers?: Record<string, string>;
  retries?: number;
  backoffBaseMs?: number;
};

export async function httpGetJson<T = any>(url: string, opts: HttpOpts = {}): Promise<T> {
  const { headers = {}, retries = 4, backoffBaseMs = 500 } = opts;
  let attempt = 0;
  while (true) {
    try {
      const res = await fetch(url, { headers });
      if (res.status >= 200 && res.status < 300) return await res.json() as T;
      if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
        throw new Error(`Transient HTTP ${res.status}`);
      }
      // Non-retryable
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt}`);
    } catch (err:any) {
      attempt++;
      if (attempt > retries) {
        logger.error({ url, err: String(err) }, "HTTP failed");
        throw err;
      }
      const wait = backoffBaseMs * Math.pow(2, attempt - 1);
      logger.warn({ url, attempt, wait }, "HTTP retry");
      await delay(wait + Math.random()*250);
    }
  }
}
