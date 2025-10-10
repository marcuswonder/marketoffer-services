import { setTimeout as delay } from 'timers/promises';

const WINDOW_MS = Number(process.env.CH_RATE_LIMIT_WINDOW_MS || 60000);
const MAX_REQUESTS_PER_WINDOW = Number(process.env.CH_RATE_LIMIT_MAX_PER_WINDOW || 500);
const MIN_INTERVAL_MS = Number(
  process.env.CH_RATE_LIMIT_MIN_INTERVAL_MS ||
    Math.ceil(WINDOW_MS / Math.max(1, MAX_REQUESTS_PER_WINDOW))
);

const timestamps: number[] = [];
let chain: Promise<unknown> = Promise.resolve();

async function acquireSlot() {
  while (true) {
    const now = Date.now();
    while (timestamps.length && now - timestamps[0] >= WINDOW_MS) {
      timestamps.shift();
    }

    const sinceLast = timestamps.length ? now - timestamps[timestamps.length - 1] : Infinity;
    if (timestamps.length < MAX_REQUESTS_PER_WINDOW && sinceLast >= MIN_INTERVAL_MS) {
      timestamps.push(Date.now());
      return;
    }

    const waitForInterval = Math.max(0, MIN_INTERVAL_MS - sinceLast);
    const waitForWindow =
      timestamps.length >= MAX_REQUESTS_PER_WINDOW ? WINDOW_MS - (now - timestamps[0]) : 0;
    const waitMs = Math.max(waitForInterval, waitForWindow, 10);
    await delay(waitMs);
  }
}

export function scheduleChRequest<T>(task: () => Promise<T>): Promise<T> {
  const run = async () => {
    await acquireSlot();
    return task();
  };
  chain = chain.then(run, run);
  return chain as Promise<T>;
}
