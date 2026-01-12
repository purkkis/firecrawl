import type {
  StressTestConfig,
  OperationType,
  FinalReport,
  TierStats,
  ViolationType,
} from './types.js';
import type { MetricsCollector } from './metrics.js';
import type { TeamSimulator } from './team-simulator.js';
import type { CorrectnessChecker } from './correctness-checker.js';
import { getTotalTeams, getExpectedOpsPerSecond } from './config.js';

const OPERATIONS_ORDER: OperationType[] = [
  'push',
  'pop',
  'complete',
  'release',
  'activePush',
  'activeRemove',
  'activeCount',
  'teamQueueCount',
];

function formatNumber(n: number): string {
  return n.toLocaleString('en-US');
}

function formatPercent(n: number): string {
  return (n * 100).toFixed(1) + '%';
}

function formatMs(n: number): string {
  if (n < 1) {
    return n.toFixed(2);
  } else if (n < 10) {
    return n.toFixed(1);
  } else {
    return Math.round(n).toString();
  }
}

function padRight(s: string, len: number): string {
  return s.padEnd(len);
}

function padLeft(s: string, len: number): string {
  return s.padStart(len);
}

export function printHeader(config: StressTestConfig): void {
  console.log('');
  console.log('='.repeat(60));
  console.log('  FDB Queue Service Stress Test');
  console.log('='.repeat(60));
  console.log('');
  console.log('Configuration:');
  console.log(`  Service URL:     ${config.serviceUrl}`);
  console.log(`  Duration:        ${config.durationSeconds} seconds`);
  console.log(`  Total teams:     ${formatNumber(getTotalTeams(config))}`);
  console.log(`  Worker pool:     ${config.workerConcurrency} concurrent requests`);
  console.log(`  Job delay:       ${config.jobProcessingDelayMs}ms`);
  console.log(`  Expected ops/s:  ~${formatNumber(getExpectedOpsPerSecond(config))}`);
  console.log('');
  console.log('Team Tiers:');

  for (const tier of config.teamTiers) {
    console.log(
      `  ${padRight(tier.name, 12)} | ` +
      `${padLeft(formatNumber(tier.teamCount), 5)} teams | ` +
      `concurrency: ${padLeft(tier.concurrencyLimit.toString(), 3)} | ` +
      `${tier.jobsPerSecond} jobs/sec`
    );
  }

  console.log('');
  console.log('-'.repeat(60));
  console.log('');
}

export function printLiveStats(
  metrics: MetricsCollector,
  simulator: TeamSimulator,
  config: StressTestConfig,
): void {
  const elapsedMs = metrics.getElapsedMs();
  const elapsedSec = elapsedMs / 1000;
  const progress = Math.min(100, (elapsedSec / config.durationSeconds) * 100);
  const opsPerSec = metrics.getOpsPerSecond();

  // Clear previous output (move cursor up)
  process.stdout.write('\x1B[2J\x1B[0f'); // Clear screen and move to top

  console.log(`=== FDB Queue Service Stress Test ===`);
  console.log(
    `Elapsed: ${Math.floor(elapsedSec)}s / ${config.durationSeconds}s (${progress.toFixed(1)}%)`
  );
  console.log(
    `Ops/sec: ${formatNumber(Math.round(opsPerSec))} ` +
    `(target: ~${formatNumber(getExpectedOpsPerSecond(config))})`
  );
  console.log('');

  // Operation table header
  console.log(
    padRight('Operation', 14) + ' | ' +
    padLeft('Requests', 10) + ' | ' +
    padLeft('Success', 8) + ' | ' +
    padLeft('p50ms', 7) + ' | ' +
    padLeft('p95ms', 7) + ' | ' +
    padLeft('p99ms', 7)
  );
  console.log('-'.repeat(14) + '-+-' + '-'.repeat(10) + '-+-' + '-'.repeat(8) + '-+-' +
              '-'.repeat(7) + '-+-' + '-'.repeat(7) + '-+-' + '-'.repeat(7));

  // Operation rows
  for (const op of OPERATIONS_ORDER) {
    const stats = metrics.getOperationStats(op);
    if (stats.totalRequests === 0) continue;

    console.log(
      padRight(op, 14) + ' | ' +
      padLeft(formatNumber(stats.totalRequests), 10) + ' | ' +
      padLeft(formatPercent(stats.successRate), 8) + ' | ' +
      padLeft(formatMs(stats.percentiles.p50), 7) + ' | ' +
      padLeft(formatMs(stats.percentiles.p95), 7) + ' | ' +
      padLeft(formatMs(stats.percentiles.p99), 7)
    );
  }

  console.log('');
  console.log(`Active teams: ${formatNumber(simulator.getTotalTeams())}`);
  console.log(`Active jobs:  ${formatNumber(simulator.getTotalActiveJobs())}`);
  console.log(`Completed:    ${formatNumber(simulator.getTotalCompletedJobs())}`);
  console.log(`Errors:       ${formatNumber(metrics.getTotalErrors())}`);

  // Show last 3 errors if any
  const recentErrors = metrics.getRecentErrors(3);
  if (recentErrors.length > 0) {
    console.log('');
    console.log('Recent errors:');
    for (const err of recentErrors) {
      const status = err.httpStatus ? `HTTP ${err.httpStatus}` : 'Network';
      console.log(`  [${err.operationType}] ${status}: ${err.errorMessage.substring(0, 60)}`);
    }
  }
}

export function printFinalReport(
  metrics: MetricsCollector,
  simulator: TeamSimulator,
  config: StressTestConfig,
): void {
  const elapsedMs = metrics.getElapsedMs();
  const elapsedSec = elapsedMs / 1000;
  const allStats = metrics.getAllOperationStats();
  const errorBreakdown = metrics.getErrorBreakdown();
  const tierStats = simulator.getTierStats();

  console.log('');
  console.log('');
  console.log('='.repeat(70));
  console.log('  FDB Queue Service Stress Test - Final Report');
  console.log('='.repeat(70));
  console.log('');

  // Configuration
  console.log('Configuration:');
  console.log(`  Service URL:        ${config.serviceUrl}`);
  console.log(`  Duration:           ${config.durationSeconds} seconds (actual: ${elapsedSec.toFixed(1)}s)`);
  console.log(`  Teams:              ${formatNumber(getTotalTeams(config))}`);
  console.log(`  Worker concurrency: ${config.workerConcurrency}`);
  console.log(`  Job delay:          ${config.jobProcessingDelayMs}ms`);
  console.log('');

  // Overall results
  console.log('Overall Results:');
  console.log(`  Total operations:   ${formatNumber(metrics.getTotalOperations())}`);
  console.log(`  Actual ops/sec:     ${formatNumber(Math.round(metrics.getOpsPerSecond()))}`);
  console.log(`  Overall success:    ${formatPercent(metrics.getOverallSuccessRate())}`);
  console.log(`  Total errors:       ${formatNumber(metrics.getTotalErrors())}`);
  console.log(`  Jobs completed:     ${formatNumber(simulator.getTotalCompletedJobs())}`);
  console.log('');

  // Per-operation table
  console.log('Per-Operation Statistics:');
  console.log(
    '+' + '-'.repeat(14) + '+' + '-'.repeat(11) + '+' + '-'.repeat(9) + '+' +
    '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+'
  );
  console.log(
    '| ' + padRight('Operation', 12) + ' | ' +
    padLeft('Requests', 9) + ' | ' +
    padLeft('Success', 7) + ' | ' +
    padLeft('Errors', 6) + ' | ' +
    padLeft('p50', 6) + ' | ' +
    padLeft('p95', 6) + ' | ' +
    padLeft('p99', 6) + ' | ' +
    padLeft('max', 6) + ' |'
  );
  console.log(
    '+' + '-'.repeat(14) + '+' + '-'.repeat(11) + '+' + '-'.repeat(9) + '+' +
    '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+'
  );

  for (const op of OPERATIONS_ORDER) {
    const stats = allStats[op];
    if (stats.totalRequests === 0) continue;

    console.log(
      '| ' + padRight(op, 12) + ' | ' +
      padLeft(formatNumber(stats.totalRequests), 9) + ' | ' +
      padLeft(formatPercent(stats.successRate), 7) + ' | ' +
      padLeft(formatNumber(stats.errorCount), 6) + ' | ' +
      padLeft(formatMs(stats.percentiles.p50) + 'ms', 6) + ' | ' +
      padLeft(formatMs(stats.percentiles.p95) + 'ms', 6) + ' | ' +
      padLeft(formatMs(stats.percentiles.p99) + 'ms', 6) + ' | ' +
      padLeft(formatMs(stats.percentiles.max) + 'ms', 6) + ' |'
    );
  }

  console.log(
    '+' + '-'.repeat(14) + '+' + '-'.repeat(11) + '+' + '-'.repeat(9) + '+' +
    '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+' + '-'.repeat(8) + '+'
  );
  console.log('');

  // Error breakdown
  const totalErrors = metrics.getTotalErrors();
  if (totalErrors > 0) {
    console.log('Error Breakdown:');
    if (errorBreakdown.http4xx > 0) {
      console.log(`  HTTP 4xx:    ${formatNumber(errorBreakdown.http4xx)}`);
    }
    if (errorBreakdown.http5xx > 0) {
      console.log(`  HTTP 5xx:    ${formatNumber(errorBreakdown.http5xx)}`);
    }
    if (errorBreakdown.network > 0) {
      console.log(`  Network:     ${formatNumber(errorBreakdown.network)}`);
    }
    if (errorBreakdown.timeout > 0) {
      console.log(`  Timeout:     ${formatNumber(errorBreakdown.timeout)}`);
    }
    if (errorBreakdown.other > 0) {
      console.log(`  Other:       ${formatNumber(errorBreakdown.other)}`);
    }
    console.log('');

    // Show recent error samples
    const recentErrors = metrics.getRecentErrors(10);
    if (recentErrors.length > 0) {
      console.log('Recent Error Samples (last 10):');
      console.log('-'.repeat(70));
      for (const err of recentErrors) {
        const time = new Date(err.timestamp).toISOString().substring(11, 23);
        const status = err.httpStatus ? `HTTP ${err.httpStatus}` : 'Network';
        console.log(`  [${time}] ${padRight(err.operationType, 14)} | ${status}`);
        console.log(`    Message: ${err.errorMessage}`);
        if (err.responseBody) {
          const body = err.responseBody.replace(/\n/g, ' ').substring(0, 100);
          console.log(`    Body: ${body}${err.responseBody.length > 100 ? '...' : ''}`);
        }
      }
      console.log('-'.repeat(70));
      console.log('');
    }
  }

  // Tier summary
  console.log('Team Tier Summary:');
  console.log(
    '+' + '-'.repeat(13) + '+' + '-'.repeat(7) + '+' + '-'.repeat(10) + '+' +
    '-'.repeat(12) + '+' + '-'.repeat(14) + '+'
  );
  console.log(
    '| ' + padRight('Tier', 11) + ' | ' +
    padLeft('Teams', 5) + ' | ' +
    padLeft('Conc Lim', 8) + ' | ' +
    padLeft('Completed', 10) + ' | ' +
    padLeft('Avg Job Time', 12) + ' |'
  );
  console.log(
    '+' + '-'.repeat(13) + '+' + '-'.repeat(7) + '+' + '-'.repeat(10) + '+' +
    '-'.repeat(12) + '+' + '-'.repeat(14) + '+'
  );

  for (const tier of tierStats) {
    console.log(
      '| ' + padRight(tier.tierName, 11) + ' | ' +
      padLeft(formatNumber(tier.teamCount), 5) + ' | ' +
      padLeft(tier.concurrencyLimit.toString(), 8) + ' | ' +
      padLeft(formatNumber(tier.totalJobsCompleted), 10) + ' | ' +
      padLeft(Math.round(tier.avgJobTimeMs) + 'ms', 12) + ' |'
    );
  }

  console.log(
    '+' + '-'.repeat(13) + '+' + '-'.repeat(7) + '+' + '-'.repeat(10) + '+' +
    '-'.repeat(12) + '+' + '-'.repeat(14) + '+'
  );
  console.log('');

  console.log('Test completed successfully.');
  console.log('');
}

export function printError(message: string, error?: unknown): void {
  console.error('');
  console.error('ERROR: ' + message);
  if (error) {
    console.error(error instanceof Error ? error.message : String(error));
  }
  console.error('');
}

export function printProgress(message: string): void {
  console.log(`[*] ${message}`);
}

export function printCorrectnessReport(checker: CorrectnessChecker): void {
  const summary = checker.getSummary();

  console.log('');
  console.log('='.repeat(70));
  console.log('  Correctness Verification Results');
  console.log('='.repeat(70));
  console.log('');

  // Status
  const status = summary.isPassing ? 'PASS' : 'FAIL';
  console.log(`Status: ${status}`);
  console.log('');

  // Job lifecycle counters
  console.log('Job Lifecycle:');
  console.log(`  Total Pushed:    ${formatNumber(summary.totalPushed)}`);
  console.log(`  Total Claimed:   ${formatNumber(summary.totalClaimed)}`);
  console.log(`  Total Completed: ${formatNumber(summary.totalCompleted)}`);
  console.log('');

  // Violations
  if (!summary.isPassing) {
    console.log('Violations Found:');

    const violationTypes: ViolationType[] = [
      'duplicate_claim',
      'orphan_claim',
      'wrong_team',
      'priority_mismatch',
      'data_corruption',
      'crawl_id_mismatch',
      'lost_job',
      'incomplete_job',
    ];

    const violationLabels: Record<ViolationType, string> = {
      duplicate_claim: 'Duplicate Claims',
      orphan_claim: 'Orphan Claims',
      wrong_team: 'Wrong Team Claims',
      priority_mismatch: 'Priority Mismatches',
      data_corruption: 'Data Corruptions',
      crawl_id_mismatch: 'CrawlId Mismatches',
      lost_job: 'Lost Jobs',
      incomplete_job: 'Incomplete Jobs',
    };

    for (const type of violationTypes) {
      const count = summary.violationCounts[type];
      if (count > 0) {
        console.log(`  ${padRight(violationLabels[type] + ':', 22)} ${formatNumber(count)}`);
      }
    }

    console.log(`  ${'-'.repeat(30)}`);
    console.log(`  ${padRight('Total:', 22)} ${formatNumber(summary.totalViolations)}`);
    console.log('');

    // Sample violations
    const samples = checker.getRecentViolations(10);
    if (samples.length > 0) {
      console.log('Sample Violations (last 10):');
      console.log('-'.repeat(70));
      for (const v of samples) {
        console.log(`  [${v.type}] Job: ${v.jobId}`);
        console.log(`    ${v.details}`);
        if (v.expected !== undefined) {
          console.log(`    Expected: ${JSON.stringify(v.expected)}`);
          console.log(`    Actual:   ${JSON.stringify(v.actual)}`);
        }
      }
      console.log('-'.repeat(70));
    }
  }

  console.log('');
}
