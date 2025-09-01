import IORedis from "ioredis";

// Simple token bucket per key using Redis INCR + TTL
// key: rl:{name}:{second}, capacity per second configurable by call site

export async function takeToken(redis: IORedis, name: string, limitPerSecond: number) {
  const key = `rl:${name}:${Math.floor(Date.now()/1000)}`;
  const n = await (redis as any).incr(key);
  if (n === 1) await (redis as any).expire(key, 2);
  if (n > limitPerSecond) return false;
  return true;
}
