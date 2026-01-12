import { parseArgs } from './config.js';
import { MetricsCollector } from './metrics.js';
import { FDBQueueClient } from './http-client.js';
import { TeamSimulator } from './team-simulator.js';
import { CorrectnessChecker } from './correctness-checker.js';
import {
  printHeader,
  printLiveStats,
  printFinalReport,
  printCorrectnessReport,
  printError,
  printProgress,
} from './reporter.js';

// Simple semaphore for concurrency control
class Semaphore {
  private permits: number;
  private waiting: (() => void)[] = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return;
    }
    return new Promise<void>(resolve => {
      this.waiting.push(resolve);
    });
  }

  release(): void {
    const next = this.waiting.shift();
    if (next) {
      next();
    } else {
      this.permits++;
    }
  }

  get available(): number {
    return this.permits;
  }

  get pending(): number {
    return this.waiting.length;
  }
}

async function runSimulation(): Promise<void> {
  // Parse CLI arguments
  const config = parseArgs(process.argv.slice(2));
  if (!config) {
    process.exit(0);
  }

  // Initialize correctness checker if enabled
  const correctnessChecker = config.correctnessChecking
    ? new CorrectnessChecker()
    : undefined;

  // Initialize components
  const metrics = new MetricsCollector(config.metricsBufferSize);
  const client = new FDBQueueClient({
    baseUrl: config.serviceUrl,
    metrics,
    verbose: config.verbose,
    correctnessChecker,
  });
  const simulator = new TeamSimulator(config, correctnessChecker);
  const semaphore = new Semaphore(config.workerConcurrency);

  // Print configuration
  printHeader(config);

  // Health check
  printProgress('Checking FDB Queue Service health...');
  const healthy = await client.healthCheck();
  if (!healthy) {
    printError(`Cannot connect to FDB Queue Service at ${config.serviceUrl}`);
    printError('Make sure the service is running and accessible.');
    process.exit(1);
  }
  printProgress('Service is healthy.');
  console.log('');

  // Set up graceful shutdown
  let running = true;
  const shutdown = () => {
    if (running) {
      running = false;
      console.log('\nShutting down gracefully...');
    }
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // Start metrics collection
  metrics.start();
  const startTime = Date.now();
  const endTime = startTime + config.durationSeconds * 1000;

  // Start periodic reporting
  const reportInterval = setInterval(() => {
    printLiveStats(metrics, simulator, config);
  }, config.reportIntervalSeconds * 1000);

  printProgress('Starting stress test...');
  console.log('');

  // Track in-flight operations
  let inFlight = 0;

  // Helper to run a task with semaphore
  const runTask = async (task: () => Promise<void>): Promise<void> => {
    await semaphore.acquire();
    inFlight++;
    try {
      await task();
    } finally {
      inFlight--;
      semaphore.release();
    }
  };

  // Main simulation loop - process teams in round-robin
  const teams = Array.from(simulator.getTeams().values());
  let teamIndex = 0;
  let loopCount = 0;

  while (running && Date.now() < endTime) {
    const now = Date.now();
    const team = teams[teamIndex];
    teamIndex = (teamIndex + 1) % teams.length;
    loopCount++;

    // 1. Push new jobs if it's time
    if (simulator.shouldPushJob(team, now)) {
      // Don't await - fire and forget with semaphore
      runTask(async () => {
        await simulator.pushJob(client, team, Date.now());
      });
    }

    // 2. Pop jobs if we have capacity and queued jobs
    if (simulator.hasCapacity(team) && team.queuedJobs > 0) {
      runTask(async () => {
        await simulator.popJob(client, team, Date.now());
      });
    }

    // 3. Complete jobs that are ready
    const completable = simulator.getCompletableJobs(team, now);
    for (const job of completable) {
      runTask(async () => {
        await simulator.completeJob(client, team, job);
      });
    }

    // Yield periodically to prevent blocking
    if (loopCount % 100 === 0) {
      await new Promise(resolve => setImmediate(resolve));
    }

    // Throttle if we're saturating concurrency
    if (semaphore.available === 0 && semaphore.pending > 1000) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  }

  // Stop reporting
  clearInterval(reportInterval);

  // Wait for in-flight operations
  printProgress(`Waiting for ${inFlight} pending operations...`);
  while (inFlight > 0) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Print final report
  printFinalReport(metrics, simulator, config);

  // Run correctness verification and print report
  if (correctnessChecker) {
    printProgress('Running correctness verification...');
    correctnessChecker.runEndOfTestVerification();
    printCorrectnessReport(correctnessChecker);
  }
}

// Run the simulation
runSimulation().catch((error) => {
  printError('Fatal error', error);
  process.exit(1);
});
