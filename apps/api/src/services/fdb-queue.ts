import * as fdb from "foundationdb";
import { TransactionOptionCode } from "foundationdb";
import { config } from "../config";
import { logger as rootLogger } from "../lib/logger";
import { getCrawl, StoredCrawl } from "../lib/crawl-redis";

const logger = rootLogger.child({ module: "fdb-queue" });

// Subspace prefixes for key organization
const QUEUE_PREFIX = Buffer.from([0x01]); // ("queue", team_id, priority, created_at, job_id) -> job_json
const CRAWL_INDEX_PREFIX = Buffer.from([0x02]); // ("crawl_idx", crawl_id, job_id) -> team_id
const COUNTER_PREFIX = Buffer.from([0x03]); // ("counter", type, id) -> int64
const ACTIVE_PREFIX = Buffer.from([0x04]); // ("active", team_id, job_id) -> expires_at
const ACTIVE_CRAWL_PREFIX = Buffer.from([0x05]); // ("active_crawl", crawl_id, job_id) -> expires_at
const TTL_INDEX_PREFIX = Buffer.from([0x06]); // ("ttl_idx", expires_at, team_id, job_id) -> "" (for efficient expired job cleanup)

// Counter types
const COUNTER_TEAM = Buffer.from([0x01]);
const COUNTER_CRAWL = Buffer.from([0x02]);
const COUNTER_ACTIVE_TEAM = Buffer.from([0x03]);
const COUNTER_ACTIVE_CRAWL = Buffer.from([0x04]);

type FDBQueueJob = {
  id: string;
  data: any;
  priority: number;
  listenable: boolean;
  createdAt: number;
  timesOutAt?: number;
  listenChannelId?: string;
  crawlId?: string;
  teamId: string;
};

// FDB database instance (lazy initialized)
let db: fdb.Database | null = null;

// Circuit breaker state for FDB health
type CircuitState = "closed" | "open" | "half-open";
let circuitState: CircuitState = "closed";
let circuitOpenedAt: number = 0;
let consecutiveFailures: number = 0;
const CIRCUIT_OPEN_DURATION_MS = 5000; // Wait 5 seconds before trying again
const CIRCUIT_FAILURE_THRESHOLD = 3; // Open circuit after 3 consecutive failures

/**
 * Error thrown when FDB circuit breaker is open.
 * Callers should handle this gracefully (e.g., log and retry later).
 */
class FDBCircuitOpenError extends Error {
  constructor() {
    super("FDB circuit breaker is open - FoundationDB is unavailable");
    this.name = "FDBCircuitOpenError";
  }
}

/**
 * Check circuit breaker state and throw if open.
 * Transitions from open to half-open after CIRCUIT_OPEN_DURATION_MS.
 */
function checkCircuit(): void {
  if (circuitState === "open") {
    const now = Date.now();
    if (now - circuitOpenedAt >= CIRCUIT_OPEN_DURATION_MS) {
      circuitState = "half-open";
      logger.info("FDB circuit breaker transitioning to half-open");
    } else {
      throw new FDBCircuitOpenError();
    }
  }
}

/**
 * Record a successful FDB operation. Closes circuit if half-open.
 */
function recordSuccess(): void {
  if (circuitState === "half-open") {
    circuitState = "closed";
    consecutiveFailures = 0;
    logger.info("FDB circuit breaker closed - FoundationDB is healthy");
  } else if (circuitState === "closed") {
    consecutiveFailures = 0;
  }
}

/**
 * Record a failed FDB operation. Opens circuit after threshold failures.
 */
function recordFailure(error: unknown): void {
  consecutiveFailures++;

  if (circuitState === "half-open") {
    // Failed during half-open test, reopen circuit
    circuitState = "open";
    circuitOpenedAt = Date.now();
    logger.error("FDB circuit breaker re-opened after half-open failure", {
      error,
      consecutiveFailures,
    });
  } else if (consecutiveFailures >= CIRCUIT_FAILURE_THRESHOLD) {
    circuitState = "open";
    circuitOpenedAt = Date.now();
    logger.error("FDB circuit breaker opened after consecutive failures", {
      error,
      consecutiveFailures,
      threshold: CIRCUIT_FAILURE_THRESHOLD,
    });
  }
}

/**
 * Get current circuit breaker status for monitoring.
 */
// function getCircuitBreakerStatus(): {
//   state: CircuitState;
//   consecutiveFailures: number;
//   openedAt: number | null;
// } {
//   return {
//     state: circuitState,
//     consecutiveFailures,
//     openedAt: circuitState === "open" ? circuitOpenedAt : null,
//   };
// }

function getDb(): fdb.Database {
  if (!db) {
    if (!config.FDB_CLUSTER_FILE) {
      throw new Error("FDB_CLUSTER_FILE is not configured");
    }
    fdb.setAPIVersion(720); // FDB 7.x API version
    db = fdb.open(config.FDB_CLUSTER_FILE);
    logger.info("FoundationDB connection initialized", {
      clusterFile: config.FDB_CLUSTER_FILE,
    });
  }
  return db;
}

/**
 * Check FDB health by performing a simple read operation.
 * Returns true if healthy, false otherwise.
 * Also updates circuit breaker state.
 */
// async function checkFDBHealth(): Promise<boolean> {
//   if (!config.FDB_CLUSTER_FILE) {
//     return false;
//   }

//   try {
//     const database = getDb();
//     // Perform a simple read (empty key range) to verify connectivity
//     await database.get(Buffer.from("__health_check__"));
//     recordSuccess();
//     return true;
//   } catch (error) {
//     recordFailure(error);
//     logger.error("FDB health check failed", { error });
//     return false;
//   }
// }

// Helper to encode a 64-bit integer as a big-endian buffer for proper ordering
function encodeInt64BE(n: number): Buffer {
  const buf = Buffer.alloc(8);
  // Use BigInt for precise 64-bit encoding
  const bigN = BigInt(Math.floor(n));
  buf.writeBigInt64BE(bigN);
  return buf;
}

// Helper to decode a big-endian 64-bit integer buffer
function decodeInt64BE(buf: Buffer): number {
  return Number(buf.readBigInt64BE(0));
}

// Helper to encode a little-endian 64-bit integer for atomic operations
function encodeInt64LE(n: number): Buffer {
  const buf = Buffer.alloc(8);
  buf.writeBigInt64LE(BigInt(n));
  return buf;
}

// Helper to decode a little-endian 64-bit integer buffer
function decodeInt64LE(buf: Buffer): number {
  if (buf.length === 0) return 0;
  return Number(buf.readBigInt64LE(0));
}

// Build queue key: (prefix, team_id, priority, created_at, job_id)
function buildQueueKey(
  teamId: string,
  priority: number,
  createdAt: number,
  jobId: string,
): Buffer {
  return fdb.tuple.pack([QUEUE_PREFIX, teamId, priority, createdAt, jobId]);
}

// Build crawl index key: (prefix, crawl_id, job_id)
function buildCrawlIndexKey(crawlId: string, jobId: string): Buffer {
  return fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId, jobId]);
}

// Build counter key: (prefix, type, id)
function buildTeamCounterKey(teamId: string): Buffer {
  return fdb.tuple.pack([COUNTER_PREFIX, COUNTER_TEAM, teamId]);
}

function buildCrawlCounterKey(crawlId: string): Buffer {
  return fdb.tuple.pack([COUNTER_PREFIX, COUNTER_CRAWL, crawlId]);
}

function buildActiveTeamCounterKey(teamId: string): Buffer {
  return fdb.tuple.pack([COUNTER_PREFIX, COUNTER_ACTIVE_TEAM, teamId]);
}

function buildActiveCrawlCounterKey(crawlId: string): Buffer {
  return fdb.tuple.pack([COUNTER_PREFIX, COUNTER_ACTIVE_CRAWL, crawlId]);
}

// Build active job key: (prefix, team_id, job_id)
function buildActiveKey(teamId: string, jobId: string): Buffer {
  return fdb.tuple.pack([ACTIVE_PREFIX, teamId, jobId]);
}

// Build active crawl job key: (prefix, crawl_id, job_id)
function buildActiveCrawlKey(crawlId: string, jobId: string): Buffer {
  return fdb.tuple.pack([ACTIVE_CRAWL_PREFIX, crawlId, jobId]);
}

// Build TTL index key: (prefix, expires_at, team_id, job_id)
// Sorted by expires_at for efficient cleanup scanning
function buildTTLIndexKey(
  expiresAt: number,
  teamId: string,
  jobId: string,
): Buffer {
  return fdb.tuple.pack([TTL_INDEX_PREFIX, expiresAt, teamId, jobId]);
}

/**
 * Push a job to the concurrency queue
 */
export async function pushJob(
  teamId: string,
  job: {
    id: string;
    data: any;
    priority: number;
    listenable: boolean;
    listenChannelId?: string;
  },
  timeout: number,
  crawlId?: string,
): Promise<void> {
  checkCircuit(); // Check circuit breaker before operation
  const database = getDb();
  const createdAt = Date.now();
  // For crawl jobs, no timeout (they wait until crawl completes).
  // For non-crawl jobs, if timeout is 0, Infinity, or very large, no timeout.
  const hasTimeout =
    !crawlId && timeout > 0 && timeout < Number.MAX_SAFE_INTEGER;
  const timesOutAt = hasTimeout ? createdAt + timeout : undefined;

  const jobData: FDBQueueJob = {
    id: job.id,
    data: job.data,
    priority: job.priority,
    listenable: job.listenable,
    listenChannelId: job.listenChannelId,
    createdAt,
    timesOutAt,
    crawlId,
    teamId,
  };

  try {
    await database.doTransaction(async tr => {
      const queueKey = buildQueueKey(teamId, job.priority, createdAt, job.id);
      tr.set(queueKey, Buffer.from(JSON.stringify(jobData)));

      // Increment team counter
      const teamCounterKey = buildTeamCounterKey(teamId);
      tr.add(teamCounterKey, encodeInt64LE(1));

      // Add to TTL index if job has a timeout (for efficient cleanup)
      if (timesOutAt) {
        const ttlKey = buildTTLIndexKey(timesOutAt, teamId, job.id);
        tr.set(
          ttlKey,
          Buffer.from(
            JSON.stringify({ priority: job.priority, createdAt, crawlId }),
          ),
        );
      }

      // If crawlId, add to crawl index and increment crawl counter
      if (crawlId) {
        const crawlIndexKey = buildCrawlIndexKey(crawlId, job.id);
        // Store teamId in the value so we can find the queue key later
        tr.set(
          crawlIndexKey,
          Buffer.from(
            JSON.stringify({ teamId, priority: job.priority, createdAt }),
          ),
        );

        const crawlCounterKey = buildCrawlCounterKey(crawlId);
        tr.add(crawlCounterKey, encodeInt64LE(1));
      }
    });
    recordSuccess();
  } catch (error) {
    recordFailure(error);
    throw error;
  }

  logger.debug("Pushed job to FDB queue", {
    teamId,
    jobId: job.id,
    priority: job.priority,
    crawlId,
    timesOutAt,
  });
}

/**
 * Push multiple jobs atomically
 */
// async function pushJobs(
//   jobs: Array<{
//     teamId: string;
//     id: string;
//     data: any;
//     priority: number;
//     listenable: boolean;
//     listenChannelId?: string;
//     timeout: number;
//     crawlId?: string;
//   }>
// ): Promise<void> {
//   if (jobs.length === 0) return;

//   checkCircuit(); // Check circuit breaker before operation
//   const database = getDb();
//   const now = Date.now();

//   try {
//     await database.doTransaction(async (tr) => {
//       // Group by team and crawl for counter increments
//       const teamCounts = new Map<string, number>();
//       const crawlCounts = new Map<string, number>();

//       for (let i = 0; i < jobs.length; i++) {
//         const job = jobs[i];
//         // Use the same timestamp for all jobs in the batch.
//         // The job ID (which is a UUIDv7) provides ordering within the same timestamp
//         // since it includes a timestamp component and random suffix.
//         const createdAt = now;
//         const timesOutAt = job.crawlId ? undefined : createdAt + job.timeout;

//         const jobData: FDBQueueJob = {
//           id: job.id,
//           data: job.data,
//           priority: job.priority,
//           listenable: job.listenable,
//           listenChannelId: job.listenChannelId,
//           createdAt,
//           timesOutAt,
//           crawlId: job.crawlId,
//           teamId: job.teamId,
//         };

//         const queueKey = buildQueueKey(job.teamId, job.priority, createdAt, job.id);
//         tr.set(queueKey, Buffer.from(JSON.stringify(jobData)));

//         teamCounts.set(job.teamId, (teamCounts.get(job.teamId) ?? 0) + 1);

//         // Add to TTL index if job has a timeout
//         if (timesOutAt) {
//           const ttlKey = buildTTLIndexKey(timesOutAt, job.teamId, job.id);
//           tr.set(ttlKey, Buffer.from(JSON.stringify({ priority: job.priority, createdAt, crawlId: job.crawlId })));
//         }

//         if (job.crawlId) {
//           const crawlIndexKey = buildCrawlIndexKey(job.crawlId, job.id);
//           tr.set(
//             crawlIndexKey,
//             Buffer.from(JSON.stringify({ teamId: job.teamId, priority: job.priority, createdAt }))
//           );
//           crawlCounts.set(job.crawlId, (crawlCounts.get(job.crawlId) ?? 0) + 1);
//         }
//       }

//       // Batch increment counters
//       for (const [teamId, count] of teamCounts) {
//         tr.add(buildTeamCounterKey(teamId), encodeInt64LE(count));
//       }
//       for (const [crawlId, count] of crawlCounts) {
//         tr.add(buildCrawlCounterKey(crawlId), encodeInt64LE(count));
//       }
//     });
//     recordSuccess();
//   } catch (error) {
//     recordFailure(error);
//     throw error;
//   }

//   logger.debug("Pushed batch of jobs to FDB queue", { count: jobs.length });
// }

/**
 * Atomically get and remove the first job for a team.
 * Returns null if no valid job is available.
 *
 * Uses a two-phase approach to minimize transaction duration:
 * 1. Phase 1: Quick read-only scan to get candidate jobs
 * 2. External checks: Crawl info lookup and concurrency checks (outside transaction)
 * 3. Phase 2: Atomic removal of the selected job
 *
 * This reduces transaction duration and conflict probability by moving
 * external Redis/FDB lookups outside the main transaction.
 */
export async function popNextJob(
  teamId: string,
  crawlConcurrencyChecker?: (crawlId: string) => Promise<boolean>,
): Promise<FDBQueueJob | null> {
  checkCircuit(); // Check circuit breaker before operation
  const database = getDb();
  const now = Date.now();

  // Cache for crawl info (persists across attempts)
  const crawlCache = new Map<string, StoredCrawl | null>();
  // Cache for concurrency check results (refreshed each attempt)
  const concurrencyCache = new Map<string, boolean>();

  for (let attempt = 0; attempt < 100; attempt++) {
    // Add exponential backoff with jitter to reduce contention
    if (attempt > 0) {
      const backoffMs = Math.min(50 * Math.pow(2, attempt - 1), 1000);
      const jitter = Math.floor(Math.random() * backoffMs);
      await new Promise(resolve => setTimeout(resolve, jitter));
      // Clear concurrency cache on retry (state may have changed)
      concurrencyCache.clear();
    }

    // PHASE 1: Quick read-only transaction to get candidate jobs
    let candidateJobs: Array<{ key: Buffer; job: FDBQueueJob }>;
    try {
      candidateJobs = await database.doTransaction(async tr => {
        tr.setOption(TransactionOptionCode.ReadYourWritesDisable); // Read-only optimization

        const startKey = fdb.tuple.pack([QUEUE_PREFIX, teamId]);
        const endKey = fdb.tuple.pack([
          QUEUE_PREFIX,
          teamId,
          Buffer.from([0xff]),
        ]);

        const entries = await tr.getRangeAll(startKey, endKey, { limit: 50 });

        return entries.map(([key, value]) => ({
          key,
          job: JSON.parse(value.toString()) as FDBQueueJob,
        }));
      });
      recordSuccess();
    } catch (error) {
      recordFailure(error);
      throw error;
    }

    if (candidateJobs.length === 0) {
      return null; // Queue is truly empty
    }

    // PHASE 2: External checks (outside transaction)
    // Pre-fetch crawl info for all crawl jobs
    const crawlIdsToFetch = new Set<string>();
    for (const { job } of candidateJobs) {
      if (job.crawlId && !crawlCache.has(job.crawlId)) {
        crawlIdsToFetch.add(job.crawlId);
      }
    }

    // Fetch crawl info in parallel
    await Promise.all(
      Array.from(crawlIdsToFetch).map(async crawlId => {
        try {
          const sc = await getCrawl(crawlId);
          crawlCache.set(crawlId, sc);
        } catch (error) {
          logger.warn(
            "Failed to fetch crawl info, treating as no concurrency limit",
            { crawlId, error },
          );
          crawlCache.set(crawlId, null);
        }
      }),
    );

    // Find the first valid job
    let selectedJob: { key: Buffer; job: FDBQueueJob } | null = null;
    let skippedDueToConcurrency = 0;
    const expiredJobs: Array<{ key: Buffer; job: FDBQueueJob }> = [];

    for (const candidate of candidateJobs) {
      const { job } = candidate;

      // Check TTL
      if (job.timesOutAt && job.timesOutAt < now) {
        expiredJobs.push(candidate);
        continue;
      }

      // Check crawl concurrency if applicable
      if (job.crawlId) {
        const sc = crawlCache.get(job.crawlId);

        if (sc !== null && sc !== undefined) {
          const maxCrawlConcurrency =
            typeof sc.crawlerOptions?.delay === "number" &&
            sc.crawlerOptions.delay > 0
              ? 1
              : (sc.maxConcurrency ?? null);

          if (maxCrawlConcurrency !== null && crawlConcurrencyChecker) {
            // Check concurrency (use cache if available)
            let canRun = concurrencyCache.get(job.crawlId);
            if (canRun === undefined) {
              canRun = await crawlConcurrencyChecker(job.crawlId);
              concurrencyCache.set(job.crawlId, canRun);
            }

            if (!canRun) {
              skippedDueToConcurrency++;
              continue;
            }
          }
        }
      }

      // Found valid job
      selectedJob = candidate;
      break;
    }

    // PHASE 3: Atomic removal transaction
    try {
      const result = await database.doTransaction(async tr => {
        // Clean up expired jobs we found
        for (const { key, job } of expiredJobs) {
          // Verify job still exists before removing
          const exists = await tr.get(key);
          if (exists) {
            tr.clear(key);
            tr.add(buildTeamCounterKey(teamId), encodeInt64LE(-1));
            if (job.timesOutAt) {
              tr.clear(buildTTLIndexKey(job.timesOutAt, teamId, job.id));
            }
            if (job.crawlId) {
              tr.clear(buildCrawlIndexKey(job.crawlId, job.id));
              tr.add(buildCrawlCounterKey(job.crawlId), encodeInt64LE(-1));
            }
          }
        }

        if (!selectedJob) {
          return { type: "no_valid_job" as const, skippedDueToConcurrency };
        }

        // Verify selected job still exists (another worker might have taken it)
        const jobExists = await tr.get(selectedJob.key);
        if (!jobExists) {
          return { type: "job_gone" as const };
        }

        // Remove the job
        const { key, job } = selectedJob;
        tr.clear(key);
        tr.add(buildTeamCounterKey(teamId), encodeInt64LE(-1));
        if (job.timesOutAt) {
          tr.clear(buildTTLIndexKey(job.timesOutAt, teamId, job.id));
        }
        if (job.crawlId) {
          tr.clear(buildCrawlIndexKey(job.crawlId, job.id));
          tr.add(buildCrawlCounterKey(job.crawlId), encodeInt64LE(-1));
        }

        return { type: "found" as const, job };
      });
      recordSuccess();

      if (result.type === "found") {
        return result.job;
      }

      if (result.type === "job_gone") {
        // Job was taken by another worker, retry
        continue;
      }

      if (
        result.type === "no_valid_job" &&
        result.skippedDueToConcurrency === 0
      ) {
        // No valid jobs and none were blocked by concurrency - queue is exhausted
        return null;
      }

      // Jobs were blocked by concurrency, retry with backoff
    } catch (error) {
      recordFailure(error);
      throw error;
    }
  }

  logger.error("Failed to pop job after 100 attempts", { teamId });
  return null;
}

/**
 * Get count of queued jobs for a team
 */
export async function getTeamQueueCount(teamId: string): Promise<number> {
  const database = getDb();
  const counterKey = buildTeamCounterKey(teamId);
  const value = await database.get(counterKey);
  return value ? decodeInt64LE(value) : 0;
}

/**
 * Get queued job IDs for a team with pagination support.
 * Returns a Set for efficient lookups.
 *
 * @param teamId The team ID to get jobs for
 * @param limit Maximum number of job IDs to return (default 10000, max 100000)
 * @returns Set of job IDs and a flag indicating if results were truncated
 */
export async function getTeamQueuedJobIds(
  teamId: string,
  limit: number = 10000,
): Promise<Set<string>> {
  const database = getDb();
  const jobIds = new Set<string>();
  const effectiveLimit = Math.min(limit, 100000); // Cap at 100k to prevent OOM

  const startKey = fdb.tuple.pack([QUEUE_PREFIX, teamId]);
  const endKey = fdb.tuple.pack([QUEUE_PREFIX, teamId, Buffer.from([0xff])]);

  // Use batched reads to prevent memory pressure
  const BATCH_SIZE = 1000;
  let lastKey: Buffer | undefined;
  let totalRead = 0;

  while (totalRead < effectiveLimit) {
    const rangeStart = lastKey
      ? Buffer.concat([lastKey, Buffer.from([0x00])])
      : startKey;

    const batchLimit = Math.min(BATCH_SIZE, effectiveLimit - totalRead);
    const entries = await database.getRangeAll(rangeStart, endKey, {
      limit: batchLimit,
    });

    if (entries.length === 0) break;

    for (const [key] of entries) {
      const parts = fdb.tuple.unpack(key);
      // Key structure: (prefix, teamId, priority, createdAt, jobId)
      const jobId = parts[4] as string;
      jobIds.add(jobId);
    }

    totalRead += entries.length;
    lastKey = entries[entries.length - 1][0];

    if (entries.length < batchLimit) break;
  }

  if (totalRead >= effectiveLimit) {
    logger.warn("getTeamQueuedJobIds hit limit, results may be truncated", {
      teamId,
      limit: effectiveLimit,
      returned: jobIds.size,
    });
  }

  return jobIds;
}

/**
 * Check if a specific job is in the queue for a crawl.
 * Uses the crawl index for O(1) lookup instead of scanning all team jobs.
 */
// async function isJobInCrawlQueue(crawlId: string, jobId: string): Promise<boolean> {
//   const database = getDb();
//   const indexKey = buildCrawlIndexKey(crawlId, jobId);
//   const value = await database.get(indexKey);
//   return value !== undefined;
// }

/**
 * Get queued job IDs for a specific crawl (not all team jobs).
 * More efficient than getTeamQueuedJobIds when you only need jobs for a specific crawl.
 */
// async function getCrawlQueuedJobIds(crawlId: string): Promise<Set<string>> {
//   const database = getDb();
//   const jobIds = new Set<string>();

//   const startKey = fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId]);
//   const endKey = fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId, Buffer.from([0xff])]);

//   // Use batched reads
//   const BATCH_SIZE = 1000;
//   let lastKey: Buffer | undefined;

//   while (true) {
//     const rangeStart = lastKey
//       ? Buffer.concat([lastKey, Buffer.from([0x00])])
//       : startKey;

//     const entries = await database.getRangeAll(rangeStart, endKey, { limit: BATCH_SIZE });

//     if (entries.length === 0) break;

//     for (const [key] of entries) {
//       const parts = fdb.tuple.unpack(key);
//       // Key structure: (prefix, crawlId, jobId)
//       const jobId = parts[2] as string;
//       jobIds.add(jobId);
//     }

//     lastKey = entries[entries.length - 1][0];

//     if (entries.length < BATCH_SIZE) break;
//   }

//   return jobIds;
// }

/**
 * Get count of queued jobs for a crawl
 */
export async function getCrawlQueueCount(crawlId: string): Promise<number> {
  const database = getDb();
  const counterKey = buildCrawlCounterKey(crawlId);
  const value = await database.get(counterKey);
  return value ? decodeInt64LE(value) : 0;
}

/**
 * Remove all jobs for a crawl (for cancellation)
 * Returns number of jobs removed
 */
// async function removeJobsForCrawl(crawlId: string): Promise<number> {
//   checkCircuit(); // Check circuit breaker before operation
//   const database = getDb();
//   let removed = 0;

//   // FDB has a transaction size limit, so we need to batch this
//   while (true) {
//     let batchRemoved: number;
//     try {
//       batchRemoved = await database.doTransaction(async (tr) => {
//       const indexStart = fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId]);
//       const indexEnd = fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId, Buffer.from([0xff])]);

//       const indexEntries = await tr.getRangeAll(indexStart, indexEnd, { limit: 100 });
//       let count = 0;

//       for (const [indexKey, indexValue] of indexEntries) {
//         const { teamId, priority, createdAt } = JSON.parse(indexValue.toString());
//         const parts = fdb.tuple.unpack(indexKey);
//         const jobId = parts[2] as string;

//         // Remove from queue
//         const queueKey = buildQueueKey(teamId, priority, createdAt, jobId);
//         tr.clear(queueKey);

//         // Remove from index
//         tr.clear(indexKey);

//         // Decrement counters
//         tr.add(buildTeamCounterKey(teamId), encodeInt64LE(-1));
//         tr.add(buildCrawlCounterKey(crawlId), encodeInt64LE(-1));

//         count++;
//       }

//       return count;
//     });
//       recordSuccess();
//     } catch (error) {
//       recordFailure(error);
//       throw error;
//     }

//     removed += batchRemoved;

//     if (batchRemoved < 100) {
//       break; // No more jobs
//     }
//   }

//   if (removed > 0) {
//     logger.info("Removed jobs for cancelled crawl", { crawlId, removed });
//   }

//   return removed;
// }

// ============= Active Job Tracking =============

/**
 * Push an active job (team level)
 */
export async function pushActiveJob(
  teamId: string,
  jobId: string,
  timeout: number,
): Promise<void> {
  checkCircuit(); // Check circuit breaker before operation
  const database = getDb();
  const expiresAt = Date.now() + timeout;

  try {
    await database.doTransaction(async tr => {
      const key = buildActiveKey(teamId, jobId);
      tr.set(key, encodeInt64BE(expiresAt));
      // Increment active counter
      tr.add(buildActiveTeamCounterKey(teamId), encodeInt64LE(1));
    });
    recordSuccess();
  } catch (error) {
    recordFailure(error);
    throw error;
  }
}

/**
 * Remove an active job (team level)
 */
export async function removeActiveJob(
  teamId: string,
  jobId: string,
): Promise<void> {
  checkCircuit(); // Check circuit breaker before operation
  const database = getDb();

  try {
    await database.doTransaction(async tr => {
      const key = buildActiveKey(teamId, jobId);
      // Only decrement if key exists
      const exists = await tr.get(key);
      if (exists) {
        tr.clear(key);
        tr.add(buildActiveTeamCounterKey(teamId), encodeInt64LE(-1));
      }
    });
    recordSuccess();
  } catch (error) {
    recordFailure(error);
    throw error;
  }
}

/**
 * Get active job count for a team.
 * Uses an atomic counter for O(1) performance.
 *
 * Note: Counter may temporarily exceed actual active jobs if entries expire
 * without explicit removal. The cleanExpiredActiveJobs function reconciles this.
 */
export async function getActiveJobCount(teamId: string): Promise<number> {
  const database = getDb();
  const counterKey = buildActiveTeamCounterKey(teamId);
  const value = await database.get(counterKey);
  const count = value ? decodeInt64LE(value) : 0;
  // Counter can't be negative
  return Math.max(0, count);
}

/**
 * Get active job IDs for a team (non-expired only)
 */
export async function getActiveJobs(teamId: string): Promise<string[]> {
  const database = getDb();
  const now = Date.now();

  const startKey = fdb.tuple.pack([ACTIVE_PREFIX, teamId]);
  const endKey = fdb.tuple.pack([ACTIVE_PREFIX, teamId, Buffer.from([0xff])]);

  const jobs: string[] = [];
  const entries = await database.getRangeAll(startKey, endKey);
  for (const [key, value] of entries) {
    const expiresAt = decodeInt64BE(value);
    if (expiresAt > now) {
      const parts = fdb.tuple.unpack(key);
      jobs.push(parts[2] as string);
    }
  }
  return jobs;
}

/**
 * Push an active job (crawl level)
 */
export async function pushCrawlActiveJob(
  crawlId: string,
  jobId: string,
  timeout: number,
): Promise<void> {
  checkCircuit(); // Check circuit breaker before operation
  const database = getDb();
  const expiresAt = Date.now() + timeout;

  try {
    await database.doTransaction(async tr => {
      const key = buildActiveCrawlKey(crawlId, jobId);
      tr.set(key, encodeInt64BE(expiresAt));
      tr.add(buildActiveCrawlCounterKey(crawlId), encodeInt64LE(1));
    });
    recordSuccess();
  } catch (error) {
    recordFailure(error);
    throw error;
  }
}

/**
 * Remove an active job (crawl level)
 */
export async function removeCrawlActiveJob(
  crawlId: string,
  jobId: string,
): Promise<void> {
  checkCircuit(); // Check circuit breaker before operation
  const database = getDb();

  try {
    await database.doTransaction(async tr => {
      const key = buildActiveCrawlKey(crawlId, jobId);
      const exists = await tr.get(key);
      if (exists) {
        tr.clear(key);
        tr.add(buildActiveCrawlCounterKey(crawlId), encodeInt64LE(-1));
      }
    });
    recordSuccess();
  } catch (error) {
    recordFailure(error);
    throw error;
  }
}

/**
 * Get active job count for a crawl.
 * Uses an atomic counter for O(1) performance.
 */
// async function getCrawlActiveJobCount(crawlId: string): Promise<number> {
//   const database = getDb();
//   const counterKey = buildActiveCrawlCounterKey(crawlId);
//   const value = await database.get(counterKey);
//   const count = value ? decodeInt64LE(value) : 0;
//   return Math.max(0, count);
// }

/**
 * Get active job IDs for a crawl (non-expired only)
 * Note: This still does a scan since we need the actual job IDs.
 */
export async function getCrawlActiveJobs(crawlId: string): Promise<string[]> {
  const database = getDb();
  const now = Date.now();

  const startKey = fdb.tuple.pack([ACTIVE_CRAWL_PREFIX, crawlId]);
  const endKey = fdb.tuple.pack([
    ACTIVE_CRAWL_PREFIX,
    crawlId,
    Buffer.from([0xff]),
  ]);

  const jobs: string[] = [];
  const entries = await database.getRangeAll(startKey, endKey);
  for (const [key, value] of entries) {
    const expiresAt = decodeInt64BE(value);
    if (expiresAt > now) {
      const parts = fdb.tuple.unpack(key);
      jobs.push(parts[2] as string);
    }
  }
  return jobs;
}

// ============= TTL Cleanup =============

/**
 * Clean expired jobs from the queue using the TTL index.
 * Should be called periodically (every 60 seconds).
 *
 * This uses a TTL index keyed by (expires_at, team_id, job_id) for efficient
 * cleanup. Instead of scanning all jobs, we only scan jobs that have expired
 * based on their TTL timestamp, which is O(expired_jobs) instead of O(all_jobs).
 *
 * Current implementation limits work per invocation to prevent long-running operations.
 */
export async function cleanExpiredJobs(): Promise<number> {
  const database = getDb();
  const now = Date.now();
  let cleaned = 0;
  const MAX_BATCHES = 10; // Limit total work per invocation
  let batches = 0;

  // Scan the TTL index for expired jobs (sorted by expires_at)
  // Only scan entries where expires_at < now
  while (batches < MAX_BATCHES) {
    batches++;
    const batchCleaned = await database.doTransaction(async tr => {
      // Scan TTL index from beginning up to current time
      const startKey = fdb.tuple.pack([TTL_INDEX_PREFIX]);
      const endKey = fdb.tuple.pack([TTL_INDEX_PREFIX, now]);

      // Get batch of expired entries from TTL index
      const entries = await tr.getRangeAll(startKey, endKey, { limit: 100 });
      let count = 0;

      for (const [ttlKey, ttlValue] of entries) {
        // Parse the TTL key to get expires_at, team_id, job_id
        const parts = fdb.tuple.unpack(ttlKey);
        const expiresAt = parts[1] as number;
        const teamId = parts[2] as string;
        const jobId = parts[3] as string;

        // Parse TTL value to get priority, createdAt, crawlId
        const { priority, createdAt, crawlId } = JSON.parse(
          ttlValue.toString(),
        );

        // Clear the main queue entry
        const queueKey = buildQueueKey(teamId, priority, createdAt, jobId);
        tr.clear(queueKey);

        // Decrement team counter
        tr.add(buildTeamCounterKey(teamId), encodeInt64LE(-1));

        // Clear crawl index and decrement crawl counter if applicable
        if (crawlId) {
          tr.clear(buildCrawlIndexKey(crawlId, jobId));
          tr.add(buildCrawlCounterKey(crawlId), encodeInt64LE(-1));
        }

        // Clear the TTL index entry
        tr.clear(ttlKey);

        count++;
      }

      return count;
    });

    cleaned += batchCleaned;

    if (batchCleaned < 100) {
      break; // No more expired jobs in TTL index
    }
  }

  if (cleaned > 0) {
    logger.info("Cleaned expired jobs from FDB queue via TTL index", {
      cleaned,
      batches,
    });
  }

  return cleaned;
}

/**
 * Clean expired active job entries and reconcile counters
 */
export async function cleanExpiredActiveJobs(): Promise<number> {
  const database = getDb();
  const now = Date.now();
  let cleaned = 0;

  // Clean team active jobs
  while (true) {
    const batchCleaned = await database.doTransaction(async tr => {
      const startKey = fdb.tuple.pack([ACTIVE_PREFIX]);
      const endKey = fdb.tuple.pack([ACTIVE_PREFIX, Buffer.from([0xff])]);

      const entries = await tr.getRangeAll(startKey, endKey, { limit: 100 });
      let count = 0;

      for (const [key, value] of entries) {
        const expiresAt = decodeInt64BE(value);
        if (expiresAt < now) {
          // Extract teamId from key: (prefix, teamId, jobId)
          const parts = fdb.tuple.unpack(key);
          const teamId = parts[1] as string;

          tr.clear(key);
          // Decrement the counter to reconcile
          tr.add(buildActiveTeamCounterKey(teamId), encodeInt64LE(-1));
          count++;
        }
      }

      return count;
    });

    cleaned += batchCleaned;
    if (batchCleaned < 100) break;
  }

  // Clean crawl active jobs
  while (true) {
    const batchCleaned = await database.doTransaction(async tr => {
      const startKey = fdb.tuple.pack([ACTIVE_CRAWL_PREFIX]);
      const endKey = fdb.tuple.pack([ACTIVE_CRAWL_PREFIX, Buffer.from([0xff])]);

      const entries = await tr.getRangeAll(startKey, endKey, { limit: 100 });
      let count = 0;

      for (const [key, value] of entries) {
        const expiresAt = decodeInt64BE(value);
        if (expiresAt < now) {
          // Extract crawlId from key: (prefix, crawlId, jobId)
          const parts = fdb.tuple.unpack(key);
          const crawlId = parts[1] as string;

          tr.clear(key);
          // Decrement the counter to reconcile
          tr.add(buildActiveCrawlCounterKey(crawlId), encodeInt64LE(-1));
          count++;
        }
      }

      return count;
    });

    cleaned += batchCleaned;
    if (batchCleaned < 100) break;
  }

  if (cleaned > 0) {
    logger.debug("Cleaned expired active job entries from FDB", { cleaned });
  }

  return cleaned;
}

// ============= Counter Reconciliation =============

/**
 * Get a sample of team IDs that have counters.
 * Uses cursor-based pagination to avoid scanning everything at once.
 */
export async function sampleTeamCounters(
  limit: number,
  afterTeamId?: string,
): Promise<string[]> {
  const database = getDb();

  const startKey = afterTeamId
    ? fdb.tuple.pack([
        COUNTER_PREFIX,
        COUNTER_TEAM,
        afterTeamId,
        Buffer.from([0x00]),
      ])
    : fdb.tuple.pack([COUNTER_PREFIX, COUNTER_TEAM]);
  const endKey = fdb.tuple.pack([
    COUNTER_PREFIX,
    COUNTER_TEAM,
    Buffer.from([0xff]),
  ]);

  const entries = await database.getRangeAll(startKey, endKey, { limit });
  const teamIds: string[] = [];

  for (const [key] of entries) {
    const parts = fdb.tuple.unpack(key);
    const teamId = parts[2] as string;
    teamIds.push(teamId);
  }

  return teamIds;
}

/**
 * Get a sample of crawl IDs that have counters.
 * Uses cursor-based pagination to avoid scanning everything at once.
 */
export async function sampleCrawlCounters(
  limit: number,
  afterCrawlId?: string,
): Promise<string[]> {
  const database = getDb();

  const startKey = afterCrawlId
    ? fdb.tuple.pack([
        COUNTER_PREFIX,
        COUNTER_CRAWL,
        afterCrawlId,
        Buffer.from([0x00]),
      ])
    : fdb.tuple.pack([COUNTER_PREFIX, COUNTER_CRAWL]);
  const endKey = fdb.tuple.pack([
    COUNTER_PREFIX,
    COUNTER_CRAWL,
    Buffer.from([0xff]),
  ]);

  const entries = await database.getRangeAll(startKey, endKey, { limit });
  const crawlIds: string[] = [];

  for (const [key] of entries) {
    const parts = fdb.tuple.unpack(key);
    const crawlId = parts[2] as string;
    crawlIds.push(crawlId);
  }

  return crawlIds;
}

/**
 * Reconcile a team's queue counter by counting actual jobs and fixing discrepancies.
 * Returns the correction made (positive if counter was too low, negative if too high).
 */
export async function reconcileTeamQueueCounter(
  teamId: string,
): Promise<number> {
  const database = getDb();

  // Count actual jobs for this team
  const startKey = fdb.tuple.pack([QUEUE_PREFIX, teamId]);
  const endKey = fdb.tuple.pack([QUEUE_PREFIX, teamId, Buffer.from([0xff])]);

  // Count in batches to avoid memory pressure
  let actualCount = 0;
  let lastKey: Buffer | undefined;

  while (true) {
    const rangeStart = lastKey
      ? Buffer.concat([lastKey, Buffer.from([0x00])])
      : startKey;

    const entries = await database.getRangeAll(rangeStart, endKey, {
      limit: 1000,
    });
    actualCount += entries.length;

    if (entries.length < 1000) break;
    lastKey = entries[entries.length - 1][0];
  }

  // Get current counter value
  const counterKey = buildTeamCounterKey(teamId);
  const counterValue = await database.get(counterKey);
  const currentCount = counterValue ? decodeInt64LE(counterValue) : 0;

  // If they match, nothing to do
  if (actualCount === currentCount) {
    return 0;
  }

  const correction = actualCount - currentCount;

  // Set the counter to the correct value
  await database.doTransaction(async tr => {
    tr.set(counterKey, encodeInt64LE(actualCount));
  });

  logger.info("Reconciled team queue counter", {
    teamId,
    previousCount: currentCount,
    actualCount,
    correction,
  });

  return correction;
}

/**
 * Reconcile a crawl's queue counter by counting actual jobs and fixing discrepancies.
 * Returns the correction made.
 */
export async function reconcileCrawlQueueCounter(
  crawlId: string,
): Promise<number> {
  const database = getDb();

  // Count actual jobs for this crawl using the crawl index
  const startKey = fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId]);
  const endKey = fdb.tuple.pack([
    CRAWL_INDEX_PREFIX,
    crawlId,
    Buffer.from([0xff]),
  ]);

  // Count in batches
  let actualCount = 0;
  let lastKey: Buffer | undefined;

  while (true) {
    const rangeStart = lastKey
      ? Buffer.concat([lastKey, Buffer.from([0x00])])
      : startKey;

    const entries = await database.getRangeAll(rangeStart, endKey, {
      limit: 1000,
    });
    actualCount += entries.length;

    if (entries.length < 1000) break;
    lastKey = entries[entries.length - 1][0];
  }

  // Get current counter value
  const counterKey = buildCrawlCounterKey(crawlId);
  const counterValue = await database.get(counterKey);
  const currentCount = counterValue ? decodeInt64LE(counterValue) : 0;

  // If they match, nothing to do
  if (actualCount === currentCount) {
    return 0;
  }

  const correction = actualCount - currentCount;

  // Set the counter to the correct value
  await database.doTransaction(async tr => {
    tr.set(counterKey, encodeInt64LE(actualCount));
  });

  logger.info("Reconciled crawl queue counter", {
    crawlId,
    previousCount: currentCount,
    actualCount,
    correction,
  });

  return correction;
}

/**
 * Reconcile a team's active job counter.
 * Returns the correction made.
 */
export async function reconcileTeamActiveCounter(
  teamId: string,
): Promise<number> {
  const database = getDb();
  const now = Date.now();

  // Count actual non-expired active jobs for this team
  const startKey = fdb.tuple.pack([ACTIVE_PREFIX, teamId]);
  const endKey = fdb.tuple.pack([ACTIVE_PREFIX, teamId, Buffer.from([0xff])]);

  let actualCount = 0;
  const entries = await database.getRangeAll(startKey, endKey);

  for (const [, value] of entries) {
    const expiresAt = decodeInt64BE(value);
    if (expiresAt > now) {
      actualCount++;
    }
  }

  // Get current counter value
  const counterKey = buildActiveTeamCounterKey(teamId);
  const counterValue = await database.get(counterKey);
  const currentCount = counterValue ? decodeInt64LE(counterValue) : 0;

  if (actualCount === currentCount) {
    return 0;
  }

  const correction = actualCount - currentCount;

  await database.doTransaction(async tr => {
    tr.set(counterKey, encodeInt64LE(actualCount));
  });

  logger.info("Reconciled team active counter", {
    teamId,
    previousCount: currentCount,
    actualCount,
    correction,
  });

  return correction;
}

/**
 * Reconcile a crawl's active job counter.
 * Returns the correction made.
 */
export async function reconcileCrawlActiveCounter(
  crawlId: string,
): Promise<number> {
  const database = getDb();
  const now = Date.now();

  // Count actual non-expired active jobs for this crawl
  const startKey = fdb.tuple.pack([ACTIVE_CRAWL_PREFIX, crawlId]);
  const endKey = fdb.tuple.pack([
    ACTIVE_CRAWL_PREFIX,
    crawlId,
    Buffer.from([0xff]),
  ]);

  let actualCount = 0;
  const entries = await database.getRangeAll(startKey, endKey);

  for (const [, value] of entries) {
    const expiresAt = decodeInt64BE(value);
    if (expiresAt > now) {
      actualCount++;
    }
  }

  // Get current counter value
  const counterKey = buildActiveCrawlCounterKey(crawlId);
  const counterValue = await database.get(counterKey);
  const currentCount = counterValue ? decodeInt64LE(counterValue) : 0;

  if (actualCount === currentCount) {
    return 0;
  }

  const correction = actualCount - currentCount;

  await database.doTransaction(async tr => {
    tr.set(counterKey, encodeInt64LE(actualCount));
  });

  logger.info("Reconciled crawl active counter", {
    crawlId,
    previousCount: currentCount,
    actualCount,
    correction,
  });

  return correction;
}

/**
 * Clean up counters for teams/crawls that have no jobs.
 * This removes stale counter entries that would never be reconciled.
 * Returns the number of counters cleaned up.
 */
export async function cleanStaleCounters(): Promise<number> {
  const database = getDb();
  let cleaned = 0;
  const BATCH_SIZE = 50;

  // Clean stale team queue counters
  let lastTeamId: string | undefined;
  while (true) {
    const teamIds = await sampleTeamCounters(BATCH_SIZE, lastTeamId);
    if (teamIds.length === 0) break;

    for (const teamId of teamIds) {
      const startKey = fdb.tuple.pack([QUEUE_PREFIX, teamId]);
      const endKey = fdb.tuple.pack([
        QUEUE_PREFIX,
        teamId,
        Buffer.from([0xff]),
      ]);

      const entries = await database.getRangeAll(startKey, endKey, {
        limit: 1,
      });
      if (entries.length === 0) {
        // No jobs for this team, clean up the counter
        await database.doTransaction(async tr => {
          tr.clear(buildTeamCounterKey(teamId));
        });
        cleaned++;
      }
    }

    lastTeamId = teamIds[teamIds.length - 1];
    if (teamIds.length < BATCH_SIZE) break;
  }

  // Clean stale crawl queue counters
  let lastCrawlId: string | undefined;
  while (true) {
    const crawlIds = await sampleCrawlCounters(BATCH_SIZE, lastCrawlId);
    if (crawlIds.length === 0) break;

    for (const crawlId of crawlIds) {
      const startKey = fdb.tuple.pack([CRAWL_INDEX_PREFIX, crawlId]);
      const endKey = fdb.tuple.pack([
        CRAWL_INDEX_PREFIX,
        crawlId,
        Buffer.from([0xff]),
      ]);

      const entries = await database.getRangeAll(startKey, endKey, {
        limit: 1,
      });
      if (entries.length === 0) {
        // No jobs for this crawl, clean up the counter
        await database.doTransaction(async tr => {
          tr.clear(buildCrawlCounterKey(crawlId));
        });
        cleaned++;
      }
    }

    lastCrawlId = crawlIds[crawlIds.length - 1];
    if (crawlIds.length < BATCH_SIZE) break;
  }

  if (cleaned > 0) {
    logger.info("Cleaned stale counters", { cleaned });
  }

  return cleaned;
}

/**
 * Check if FDB is configured and available
 */
export function isFDBConfigured(): boolean {
  return !!config.FDB_CLUSTER_FILE;
}

/**
 * Initialize FDB connection (call on startup if configured)
 */
export function initFDB(): boolean {
  if (!config.FDB_CLUSTER_FILE) {
    logger.info("FDB not configured, skipping initialization");
    return false;
  }
  try {
    getDb();
    return true;
  } catch (error) {
    logger.error("Failed to initialize FDB", { error });
    return false;
  }
}
