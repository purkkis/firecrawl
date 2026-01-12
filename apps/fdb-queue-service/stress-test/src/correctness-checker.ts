import type {
  PushedJobRecord,
  JobLifecycleRecord,
  ViolationType,
  CorrectnessViolation,
  CorrectnessSummary,
  ClaimedJob,
} from './types.js';

const MAX_VIOLATIONS_STORED = 1000;

const ALL_VIOLATION_TYPES: ViolationType[] = [
  'duplicate_claim',
  'orphan_claim',
  'wrong_team',
  'priority_mismatch',
  'data_corruption',
  'crawl_id_mismatch',
  'lost_job',
  'incomplete_job',
];

export class CorrectnessChecker {
  // Job tracking - use Map for O(1) lookups
  private jobRegistry: Map<string, JobLifecycleRecord> = new Map();

  // Set for O(1) duplicate detection
  private claimedJobIds: Set<string> = new Set();

  // Jobs where push() was called but not yet confirmed (handles async timing)
  private pendingPushes: Map<string, PushedJobRecord> = new Map();

  // Track in-flight jobs (claimed but not completed) for incomplete detection
  private inFlightJobs: Map<string, JobLifecycleRecord> = new Map();

  // Violation storage (capped)
  private violations: CorrectnessViolation[] = [];

  // Counters for summary
  private counters = {
    pushed: 0,
    claimed: 0,
    completed: 0,
  };

  // Violation counts by type
  private violationCounts: Record<ViolationType, number>;

  constructor() {
    // Initialize violation counts
    this.violationCounts = {} as Record<ViolationType, number>;
    for (const type of ALL_VIOLATION_TYPES) {
      this.violationCounts[type] = 0;
    }
  }

  /**
   * Record a job push attempt. Call BEFORE the HTTP request.
   * Returns the record for later use if push succeeds.
   */
  recordPush(
    jobId: string,
    teamId: string,
    priority: number,
    dataTimestamp: number,
    crawlId?: string,
  ): PushedJobRecord {
    const record: PushedJobRecord = {
      jobId,
      teamId,
      priority,
      crawlId,
      dataTimestamp,
    };

    // Track as pending until confirmed
    this.pendingPushes.set(jobId, record);

    return record;
  }

  /**
   * Confirm a push succeeded. Updates job state to 'pushed'.
   * Call AFTER successful HTTP response.
   */
  confirmPush(record: PushedJobRecord): void {
    this.pendingPushes.delete(record.jobId);
    this.counters.pushed++;

    this.jobRegistry.set(record.jobId, {
      jobId: record.jobId,
      state: 'pushed',
      pushedRecord: record,
    });
  }

  /**
   * Record and validate a job claim (pop).
   * Performs all validations and records violations.
   * Returns true if claim is valid, false otherwise.
   */
  recordClaim(claimedJob: ClaimedJob, poppingTeamId: string): boolean {
    const { job, queueKey } = claimedJob;
    const jobId = job.id;
    let isValid = true;

    this.counters.claimed++;

    // 1. Check for duplicate claim
    if (this.claimedJobIds.has(jobId)) {
      this.recordViolation({
        type: 'duplicate_claim',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `Job ${jobId} was already claimed`,
      });
      isValid = false;
    }
    this.claimedJobIds.add(jobId);

    // 2. Check if job is still pending (push in flight) - handle async timing
    if (this.pendingPushes.has(jobId)) {
      const pendingRecord = this.pendingPushes.get(jobId)!;
      this.confirmPush(pendingRecord);
      // Decrement pushed since confirmPush incremented it
      // but we already counted this claim
    }

    // 3. Check if job was pushed (orphan detection)
    const existingRecord = this.jobRegistry.get(jobId);
    if (!existingRecord) {
      this.recordViolation({
        type: 'orphan_claim',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `Claimed job ${jobId} was never pushed`,
      });
      isValid = false;

      // Create a synthetic record for tracking
      const syntheticRecord: JobLifecycleRecord = {
        jobId,
        state: 'claimed',
        pushedRecord: {
          jobId,
          teamId: job.teamId,
          priority: job.priority,
          crawlId: job.crawlId,
          dataTimestamp: 0, // unknown
        },
        claimTimestamp: Date.now(),
      };
      this.inFlightJobs.set(jobId, syntheticRecord);
      return isValid;
    }

    const pushed = existingRecord.pushedRecord;

    // 4. Validate teamId matches
    if (job.teamId !== pushed.teamId) {
      this.recordViolation({
        type: 'wrong_team',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `Job teamId mismatch`,
        expected: pushed.teamId,
        actual: job.teamId,
      });
      isValid = false;
    }

    // 5. Validate priority
    if (job.priority !== pushed.priority) {
      this.recordViolation({
        type: 'priority_mismatch',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `Priority changed`,
        expected: pushed.priority,
        actual: job.priority,
      });
      isValid = false;
    }

    // 6. Validate crawlId
    if (job.crawlId !== pushed.crawlId) {
      this.recordViolation({
        type: 'crawl_id_mismatch',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `CrawlId changed`,
        expected: pushed.crawlId,
        actual: job.crawlId,
      });
      isValid = false;
    }

    // 7. Validate data payload
    const data = job.data as { stress?: boolean; timestamp?: number } | null;
    if (!data || data.stress !== true) {
      this.recordViolation({
        type: 'data_corruption',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `data.stress is not true`,
        expected: true,
        actual: data?.stress,
      });
      isValid = false;
    }
    if (!data || data.timestamp !== pushed.dataTimestamp) {
      this.recordViolation({
        type: 'data_corruption',
        timestamp: Date.now(),
        jobId,
        teamId: poppingTeamId,
        details: `data.timestamp changed`,
        expected: pushed.dataTimestamp,
        actual: data?.timestamp,
      });
      isValid = false;
    }

    // Update state
    existingRecord.state = 'claimed';
    existingRecord.claimTimestamp = Date.now();

    // Track as in-flight for incomplete detection
    this.inFlightJobs.set(jobId, existingRecord);

    return isValid;
  }

  /**
   * Record job completion.
   */
  recordComplete(jobId: string): void {
    this.counters.completed++;

    const inFlight = this.inFlightJobs.get(jobId);
    if (inFlight) {
      inFlight.state = 'completed';
      inFlight.completeTimestamp = Date.now();
      this.inFlightJobs.delete(jobId);
    }

    // Update full registry
    const record = this.jobRegistry.get(jobId);
    if (record) {
      record.state = 'completed';
      record.completeTimestamp = Date.now();
    }
  }

  /**
   * Run end-of-test verification.
   * Checks for lost jobs and incomplete jobs.
   */
  runEndOfTestVerification(): void {
    // 1. Check for lost jobs (pushed but never claimed)
    for (const [jobId, record] of this.jobRegistry) {
      if (record.state === 'pushed') {
        this.recordViolation({
          type: 'lost_job',
          timestamp: Date.now(),
          jobId,
          teamId: record.pushedRecord.teamId,
          details: `Job was pushed but never claimed`,
        });
      }
    }

    // 2. Check for incomplete jobs (claimed but never completed)
    for (const [jobId, record] of this.inFlightJobs) {
      this.recordViolation({
        type: 'incomplete_job',
        timestamp: Date.now(),
        jobId,
        teamId: record.pushedRecord.teamId,
        details: `Job was claimed but never completed`,
      });
    }
  }

  /**
   * Get the summary of correctness checking results.
   */
  getSummary(): CorrectnessSummary {
    let totalViolations = 0;
    for (const count of Object.values(this.violationCounts)) {
      totalViolations += count;
    }

    return {
      totalPushed: this.counters.pushed,
      totalClaimed: this.counters.claimed,
      totalCompleted: this.counters.completed,
      violationCounts: { ...this.violationCounts },
      totalViolations,
      isPassing: totalViolations === 0,
    };
  }

  /**
   * Get all recorded violations (up to the storage cap).
   */
  getViolations(): CorrectnessViolation[] {
    return [...this.violations];
  }

  /**
   * Get the most recent violations.
   */
  getRecentViolations(count: number): CorrectnessViolation[] {
    return this.violations.slice(-count);
  }

  /**
   * Check if the correctness test is passing (no violations).
   */
  isPassing(): boolean {
    return this.getSummary().isPassing;
  }

  /**
   * Record a violation.
   */
  private recordViolation(violation: CorrectnessViolation): void {
    this.violationCounts[violation.type]++;

    // Store violation (capped)
    if (this.violations.length < MAX_VIOLATIONS_STORED) {
      this.violations.push(violation);
    }
  }
}
