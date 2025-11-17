import { isSelfHosted } from "../../lib/deployment";
import { ScrapeJobTimeoutError, TransportableError } from "../../lib/error";
import { logger as _logger } from "../../lib/logger";
import { nuqRedis, semaphoreKeys } from "./redis";
import { createHistogram } from "node:perf_hooks";

let active_semaphores = 0;
let semaphoreAcquireHistogram = createHistogram();

const { scripts, runScript, ensure } = nuqRedis;

const SEMAPHORE_TTL = 30 * 1000;

async function acquire(
  teamId: string,
  holderId: string,
  limit: number,
): Promise<{ granted: boolean; count: number; removed: number }> {
  await ensure();

  const keys = semaphoreKeys(teamId);
  const [granted, count, removed] = await runScript<[number, number, number]>(
    scripts.semaphore.acquire,
    [keys.leases],
    [holderId, limit, SEMAPHORE_TTL],
  );

  return {
    granted: granted === 1,
    count,
    removed,
  };
}

async function acquireBlocking(
  teamId: string,
  holderId: string,
  limit: number,
  options: {
    base_delay_ms: number;
    max_delay_ms: number;
    timeout_ms: number;
    signal: AbortSignal;
  },
): Promise<{ limited: boolean; removed: number }> {
  await ensure();

  const deadline = Date.now() + options.timeout_ms;
  const keys = semaphoreKeys(teamId);

  let delay = options.base_delay_ms;
  let totalRemoved = 0;
  let failedOnce = false;

  let start = process.hrtime.bigint();

  do {
    if (options.signal.aborted) {
      throw new ScrapeJobTimeoutError("Scrape timed out");
    }

    if (deadline < Date.now()) {
      throw new ScrapeJobTimeoutError("Scrape timed out");
    }

    const [granted, _count, _removed] = await runScript<
      [number, number, number]
    >(
      scripts.semaphore.acquire,
      [keys.leases],
      [holderId, limit, SEMAPHORE_TTL],
    );

    totalRemoved++;

    if (granted === 1) {
      const duration = process.hrtime.bigint() - start;
      semaphoreAcquireHistogram.record(duration);
      return { limited: failedOnce, removed: totalRemoved };
    }

    failedOnce = true;

    const jitter = Math.floor(
      Math.random() * Math.max(1, Math.floor(delay / 4)),
    );
    await new Promise(r => setTimeout(r, delay + jitter));

    delay = Math.min(options.max_delay_ms, Math.floor(delay * 1.5));
  } while (true);
}

async function heartbeat(teamId: string, holderId: string): Promise<boolean> {
  await ensure();

  const keys = semaphoreKeys(teamId);
  return (
    (await runScript<number>(
      scripts.semaphore.heartbeat,
      [keys.leases],
      [holderId, SEMAPHORE_TTL],
    )) === 1
  );
}

async function release(teamId: string, holderId: string): Promise<void> {
  await ensure();

  const keys = semaphoreKeys(teamId);
  await runScript<number>(scripts.semaphore.release, [keys.leases], [holderId]);
}

async function count(teamId: string): Promise<number> {
  await ensure();

  const keys = semaphoreKeys(teamId);
  const count = await nuqRedis.zcard(keys.leases);
  return count;
}

function startHeartbeat(teamId: string, holderId: string, intervalMs: number) {
  let stopped = false;

  const promise = (async () => {
    while (!stopped) {
      const ok = await heartbeat(teamId, holderId);
      if (!ok) {
        throw new TransportableError("SCRAPE_TIMEOUT", "heartbeat_failed");
      }
      await new Promise(r => setTimeout(r, intervalMs));
    }
    return Promise.reject(
      new Error("heartbeat loop stopped unexpectedly"),
    ) as never;
  })();

  return {
    promise,
    stop() {
      stopped = true;
    },
  };
}

async function withSemaphore<T>(
  teamId: string,
  holderId: string,
  limit: number,
  signal: AbortSignal,
  timeoutMs: number,
  func: (limited: boolean) => Promise<T>,
): Promise<T> {
  if (isSelfHosted() && limit <= 1) {
    _logger.debug(`Bypassing concurrency limit for ${teamId}`, {
      teamId,
      jobId: holderId,
    });
    return await func(false);
  }

  const { limited } = await acquireBlocking(teamId, holderId, limit, {
    base_delay_ms: 25,
    max_delay_ms: 250,
    timeout_ms: timeoutMs,
    signal,
  });

  const hb = startHeartbeat(teamId, holderId, SEMAPHORE_TTL / 2);

  active_semaphores++;
  try {
    const result = await Promise.race([func(limited), hb.promise]);
    return result;
  } finally {
    active_semaphores--;
    hb.stop();

    await release(teamId, holderId).catch(() => {});
  }
}

const getMetrics = () => {
  const h = semaphoreAcquireHistogram;
  const p50 = h.percentile(50);
  const p90 = h.percentile(90);
  const p99 = h.percentile(99);
  const max = h.max;

  return (
    [
      "# HELP noq_semaphore_active Number of active semaphore holders",
      "# TYPE noq_semaphore_active gauge",
      `noq_semaphore_active ${active_semaphores}`,

      "# HELP noq_semaphore_acquire_duration_seconds Semaphore acquire time",
      "# TYPE noq_semaphore_acquire_duration_seconds gauge",
      `noq_semaphore_acquire_duration_seconds_p50 ${p50 / 1e9}`,
      `noq_semaphore_acquire_duration_seconds_p90 ${p90 / 1e9}`,
      `noq_semaphore_acquire_duration_seconds_p99 ${p99 / 1e9}`,
      `noq_semaphore_acquire_duration_seconds_max ${max / 1e9}`,

      "# HELP noq_semaphore_acquire_observations_total Number of recorded semaphore acquire durations",
      "# TYPE noq_semaphore_acquire_observations_total counter",
      `noq_semaphore_acquire_observations_total ${h.count}`,
    ].join("\n") + "\n"
  );
};

export const teamConcurrencySemaphore = {
  acquire,
  release,
  withSemaphore,
  count,
  getMetrics,
};
