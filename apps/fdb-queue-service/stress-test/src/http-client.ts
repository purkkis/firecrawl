import type {
  ClaimedJob,
  PushJobRequest,
  PopJobRequest,
  PushActiveJobRequest,
  RemoveActiveJobRequest,
  CompleteJobRequest,
  ReleaseJobRequest,
  OperationType,
} from './types.js';
import type { MetricsCollector } from './metrics.js';
import type { CorrectnessChecker } from './correctness-checker.js';

export interface HttpClientOptions {
  baseUrl: string;
  metrics: MetricsCollector;
  verbose: boolean;
  correctnessChecker?: CorrectnessChecker;
}

export class FDBQueueClient {
  private baseUrl: string;
  private metrics: MetricsCollector;
  private verbose: boolean;
  private workerId: string;
  private correctnessChecker?: CorrectnessChecker;

  constructor(options: HttpClientOptions) {
    this.baseUrl = options.baseUrl;
    this.metrics = options.metrics;
    this.verbose = options.verbose;
    this.correctnessChecker = options.correctnessChecker;
    this.workerId = `stress-${process.pid}-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;
  }

  private async request<T>(
    method: 'GET' | 'POST' | 'DELETE',
    path: string,
    operationType: OperationType,
    body?: unknown,
  ): Promise<{ data: T | null; success: boolean; error?: string }> {
    const url = `${this.baseUrl}${path}`;
    const startTime = performance.now();

    try {
      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json',
        },
        body: body ? JSON.stringify(body) : undefined,
      });

      const latencyMs = performance.now() - startTime;

      if (!response.ok) {
        const responseBody = await response.text();
        const errorMessage = `HTTP ${response.status} ${response.statusText}: ${path}`;
        this.metrics.record(
          operationType,
          latencyMs,
          false,
          response.status,
          errorMessage,
          responseBody,
        );

        if (this.verbose) {
          console.error(`[${operationType}] ${errorMessage}`);
          console.error(`  Response: ${responseBody.substring(0, 200)}`);
        }

        return {
          data: null,
          success: false,
          error: `${errorMessage}\n${responseBody}`,
        };
      }

      const data = await response.json() as T;
      this.metrics.record(operationType, latencyMs, true);

      return { data, success: true };
    } catch (error) {
      const latencyMs = performance.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.metrics.record(
        operationType,
        latencyMs,
        false,
        undefined,
        `Network error: ${errorMessage}`,
        undefined,
      );

      if (this.verbose) {
        console.error(`[${operationType}] Network error: ${errorMessage}`);
      }

      return {
        data: null,
        success: false,
        error: errorMessage,
      };
    }
  }

  // === Queue Operations ===

  async pushJob(
    teamId: string,
    jobId: string,
    priority: number,
    timeout: number,
    crawlId?: string,
  ): Promise<boolean> {
    const dataTimestamp = Date.now();

    // Record push attempt for correctness tracking
    const pushRecord = this.correctnessChecker?.recordPush(
      jobId,
      teamId,
      priority,
      dataTimestamp,
      crawlId,
    );

    const body: PushJobRequest = {
      teamId,
      job: {
        id: jobId,
        data: { stress: true, timestamp: dataTimestamp },
        priority,
        listenable: false,
      },
      timeout,
      crawlId,
    };

    const result = await this.request<void>('POST', '/queue/push', 'push', body);

    // Confirm push on success
    if (result.success && pushRecord) {
      this.correctnessChecker?.confirmPush(pushRecord);
    }

    return result.success;
  }

  async popJob(teamId: string): Promise<ClaimedJob | null> {
    const body: PopJobRequest = {
      workerId: this.workerId,
      blockedCrawlIds: [],
    };

    const result = await this.request<ClaimedJob | null>(
      'POST',
      `/queue/pop/${encodeURIComponent(teamId)}`,
      'pop',
      body,
    );

    // Validate the claimed job for correctness
    if (result.data && result.data.job) {
      this.correctnessChecker?.recordClaim(result.data, teamId);
    }

    return result.data;
  }

  async completeJob(queueKey: string): Promise<boolean> {
    const body: CompleteJobRequest = { queueKey };
    const result = await this.request<{ success: boolean }>(
      'POST',
      '/queue/complete',
      'complete',
      body,
    );
    return result.success && (result.data?.success ?? false);
  }

  async releaseJob(jobId: string): Promise<boolean> {
    const body: ReleaseJobRequest = { jobId };
    const result = await this.request<{ success: boolean }>(
      'POST',
      '/queue/release',
      'release',
      body,
    );
    return result.success;
  }

  // === Active Job Tracking ===

  async pushActiveJob(teamId: string, jobId: string, timeout: number): Promise<boolean> {
    const body: PushActiveJobRequest = { teamId, jobId, timeout };
    const result = await this.request<void>('POST', '/active/push', 'activePush', body);
    return result.success;
  }

  async removeActiveJob(teamId: string, jobId: string): Promise<boolean> {
    const body: RemoveActiveJobRequest = { teamId, jobId };
    const result = await this.request<void>('DELETE', '/active/remove', 'activeRemove', body);
    return result.success;
  }

  async getActiveCount(teamId: string): Promise<number | null> {
    const result = await this.request<{ count: number }>(
      'GET',
      `/active/count/${encodeURIComponent(teamId)}`,
      'activeCount',
    );
    return result.data?.count ?? null;
  }

  // === Queue Count ===

  async getTeamQueueCount(teamId: string): Promise<number | null> {
    const result = await this.request<{ count: number }>(
      'GET',
      `/queue/count/team/${encodeURIComponent(teamId)}`,
      'teamQueueCount',
    );
    return result.data?.count ?? null;
  }

  // === Health Check ===

  async healthCheck(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/health`);
      return response.ok;
    } catch {
      return false;
    }
  }
}
