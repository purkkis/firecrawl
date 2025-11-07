import { Response } from "express";
import { v4 as uuidv4 } from "uuid";
import {
  BatchScrapeRequest,
  batchScrapeRequestSchema,
  batchScrapeRequestSchemaNoURLValidation,
  URL as urlSchema,
  RequestWithAuth,
  ScrapeOptions,
  BatchScrapeResponse,
} from "./types";
import {
  addCrawlJobs,
  finishCrawlKickoff,
  getCrawl,
  lockURLs,
  markCrawlActive,
  saveCrawl,
  StoredCrawl,
} from "../../lib/crawl-redis";
import { getJobPriority } from "../../lib/job-priority";
import { addScrapeJobs } from "../../services/queue-jobs";
import { createWebhookSender, WebhookEvent } from "../../services/webhook";
import { logger as _logger } from "../../lib/logger";
import { BLOCKLISTED_URL_MESSAGE } from "../../lib/strings";
import { isUrlBlocked } from "../../scraper/WebScraper/utils/blocklist";
import { checkPermissions } from "../../lib/permissions";
import { crawlGroup } from "../../services/worker/nuq";

export async function batchScrapeController(
  req: RequestWithAuth<{}, BatchScrapeResponse, BatchScrapeRequest>,
  res: Response<BatchScrapeResponse>,
) {
  const preNormalizedBody = { ...req.body };
  const parsedBody =
    preNormalizedBody?.ignoreInvalidURLs === true
      ? batchScrapeRequestSchemaNoURLValidation.parse(preNormalizedBody)
      : batchScrapeRequestSchema.parse(preNormalizedBody);

  const permissions = checkPermissions(parsedBody, req.acuc?.flags);
  if (permissions.error) {
    return res.status(403).json({
      success: false,
      error: permissions.error,
    });
  }

  const zeroDataRetention =
    req.acuc?.flags?.forceZDR || parsedBody.zeroDataRetention;

  const id = parsedBody.appendToId ?? uuidv4();
  const logger = _logger.child({
    crawlId: id,
    batchScrapeId: id,
    module: "api/v2",
    method: "batchScrapeController",
    teamId: req.auth.team_id,
    zeroDataRetention,
  });

  let urls: string[] = parsedBody.urls;
  let invalidURLs: string[] | undefined = undefined;

  if (parsedBody.ignoreInvalidURLs) {
    invalidURLs = [];

    let pendingURLs = urls;
    urls = [];
    const unnormalizedURLs: string[] = [];
    for (const u of pendingURLs) {
      try {
        const nu = urlSchema.parse(u);
        if (!isUrlBlocked(nu, req.acuc?.flags ?? null)) {
          urls.push(nu);
          unnormalizedURLs.push(u);
        } else {
          invalidURLs.push(u);
        }
      } catch (_) {
        invalidURLs.push(u);
      }
    }
  } else {
    if (
      parsedBody.urls?.some((url: string) =>
        isUrlBlocked(url, req.acuc?.flags ?? null),
      )
    ) {
      if (!res.headersSent) {
        return res.status(403).json({
          success: false,
          error: BLOCKLISTED_URL_MESSAGE,
        });
      }
    }
  }

  if (urls.length === 0) {
    return res.status(400).json({
      success: false,
      error: "No valid URLs provided",
    });
  }

  logger.debug("Batch scrape " + id + " starting", {
    urlsLength: urls.length,
    appendToId: parsedBody.appendToId,
    account: req.account,
  });

  // Extract scrapeOptions from parsedBody, excluding non-scrapeOptions fields
  const {
    urls: _urls,
    appendToId: _appendToId,
    webhook: _webhook,
    integration: _integration,
    maxConcurrency: _maxConcurrency,
    zeroDataRetention: _zeroDataRetention,
    ignoreInvalidURLs: _ignoreInvalidURLs,
    ...scrapeOptions
  } = parsedBody;

  const sc: StoredCrawl = parsedBody.appendToId
    ? ((await getCrawl(parsedBody.appendToId)) as StoredCrawl)
    : {
        crawlerOptions: null,
        scrapeOptions: scrapeOptions as ScrapeOptions,
        internalOptions: {
          disableSmartWaitCache: true,
          teamId: req.auth.team_id,
          saveScrapeResultToGCS: process.env.GCS_FIRE_ENGINE_BUCKET_NAME
            ? true
            : false,
          zeroDataRetention,
        }, // NOTE: smart wait disabled for batch scrapes to ensure contentful scrape, speed does not matter
        team_id: req.auth.team_id,
        createdAt: Date.now(),
        maxConcurrency: parsedBody.maxConcurrency,
        zeroDataRetention,
      };

  if (!parsedBody.appendToId) {
    await crawlGroup.addGroup(
      id,
      sc.team_id,
      (req.acuc?.flags?.crawlTtlHours ?? 24) * 60 * 60 * 1000,
    );
    await saveCrawl(id, sc);
    await markCrawlActive(id);
  }

  let jobPriority = 20;

  // If it is over 1000, we need to get the job priority,
  // otherwise we can use the default priority of 20
  if (urls.length > 1000) {
    // set base to 21
    jobPriority = await getJobPriority({
      team_id: req.auth.team_id,
      basePriority: 21,
    });
  }
  logger.debug("Using job priority " + jobPriority, { jobPriority });

  const jobs = urls.map(x => ({
    jobId: uuidv4(),
    data: {
      url: x,
      mode: "single_urls" as const,
      team_id: req.auth.team_id,
      crawlerOptions: null,
      scrapeOptions: scrapeOptions as ScrapeOptions,
      origin: "api",
      integration: parsedBody.integration,
      crawl_id: id,
      sitemapped: true,
      v1: true,
      webhook: parsedBody.webhook,
      internalOptions: sc.internalOptions,
      zeroDataRetention: zeroDataRetention ?? false,
      apiKeyId: req.acuc?.api_key_id ?? null,
    },
    priority: jobPriority,
  }));

  await finishCrawlKickoff(id);

  logger.debug("Locking URLs...");
  await lockURLs(
    id,
    sc,
    jobs.map(x => x.data.url),
    logger,
  );
  logger.debug("Adding scrape jobs to Redis...");
  await addCrawlJobs(
    id,
    jobs.map(x => x.jobId),
    logger,
  );
  logger.debug("Adding scrape jobs to BullMQ...");
  await addScrapeJobs(jobs);

  if (parsedBody.webhook) {
    logger.debug("Calling webhook with batch_scrape.started...", {
      webhook: parsedBody.webhook,
    });
    const sender = await createWebhookSender({
      teamId: req.auth.team_id,
      jobId: id,
      webhook: parsedBody.webhook,
      v0: false,
    });
    await sender?.send(WebhookEvent.BATCH_SCRAPE_STARTED, { success: true });
  }

  const protocol = req.protocol;

  return res.status(200).json({
    success: true,
    id,
    url: `${protocol}://${req.get("host")}/v2/batch/scrape/${id}`,
    invalidURLs,
  });
}
