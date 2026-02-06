import { Meta } from "../..";
import { EngineScrapeResult } from "..";
import * as Sentry from "@sentry/node";
import { readFile } from "node:fs/promises";
import { RemoveFeatureError } from "../../error";
import { AbortManagerThrownError } from "../../lib/abortManager";
import {
  shouldParsePDF,
  getPDFMaxPages,
} from "../../../../controllers/v2/types";
import { PdfAnalysis } from "@mendable/firecrawl-rs";

import {
  scrapePDFWithPdfParse,
  scrapePDFWithMinerU,
  isBadFastOutput,
  isMinerUAvailable,
  PDFProcessorResult,
} from "./parsers";

import {
  handleNonParsingMode,
  getOrDownloadPDF,
  validateContentType,
  analyzePDFFile,
  checkTimeoutConstraints,
  createChildMeta,
  cleanupTempFile,
} from "./utils";

export async function scrapePDF(meta: Meta): Promise<EngineScrapeResult> {
  const shouldParse = shouldParsePDF(meta.options.parsers);
  const maxPages = getPDFMaxPages(meta.options.parsers);

  // Early return for non-parsing mode (just return base64 content)
  if (!shouldParse) {
    return handleNonParsingMode(meta);
  }

  // Download or use prefetched PDF
  const { response, tempFilePath } = await getOrDownloadPDF(meta);

  try {
    // Validate content type
    await validateContentType(response, meta);

    // Analyze PDF for metadata and routing
    const { pdfAnalysis, effectivePageCount } = await analyzePDFFile(
      tempFilePath,
      maxPages,
      meta,
    );

    // Check timeout constraints
    checkTimeoutConstraints(effectivePageCount, meta);

    // Read file content for processing
    const base64Content = (await readFile(tempFilePath)).toString("base64");

    // Route and process the PDF
    const result = await routeAndProcess(
      meta,
      tempFilePath,
      base64Content,
      pdfAnalysis,
      effectivePageCount,
      maxPages,
    );

    return {
      url: response.url ?? meta.rewrittenUrl ?? meta.url,
      statusCode: response.status,
      html: result?.html ?? "",
      markdown: result?.markdown ?? "",
      pdfMetadata: {
        numPages: effectivePageCount,
        title: pdfAnalysis.title,
      },
      proxyUsed: "basic",
    };
  } finally {
    await cleanupTempFile(tempFilePath, meta);
  }
}

/**
 * Route the PDF to the appropriate parser based on analysis.
 */
async function routeAndProcess(
  meta: Meta,
  tempFilePath: string,
  base64Content: string,
  pdfAnalysis: PdfAnalysis,
  effectivePageCount: number,
  maxPages: number | undefined,
): Promise<PDFProcessorResult> {
  const muAvailable = isMinerUAvailable(base64Content.length);
  const route = pdfAnalysis.recommendedRoute;

  meta.logger.info("PDF routing decision", {
    url: meta.rewrittenUrl ?? meta.url,
    route,
    muAvailable,
  });

  if (route === "fast") {
    return processFastRoute(
      meta,
      tempFilePath,
      base64Content,
      effectivePageCount,
      muAvailable,
      maxPages,
    );
  } else {
    // "layout" or "ocr" route
    return processLayoutOrOcrRoute(
      meta,
      tempFilePath,
      base64Content,
      route,
      muAvailable,
      maxPages,
    );
  }
}

/**
 * Process PDF using fast route (pdf-parse first, escalate if needed).
 */
async function processFastRoute(
  meta: Meta,
  tempFilePath: string,
  base64Content: string,
  effectivePageCount: number,
  muAvailable: boolean,
  maxPages: number | undefined,
): Promise<PDFProcessorResult> {
  const fastStartedAt = Date.now();

  try {
    const result = await scrapePDFWithPdfParse(
      createChildMeta(meta, "scrapePDF/scrapePDFWithPdfParse"),
      tempFilePath,
    );
    const fastDurationMs = Date.now() - fastStartedAt;

    // Quality check
    const qualityCheck = isBadFastOutput(
      result.markdown ?? "",
      effectivePageCount,
    );

    if (qualityCheck.bad && muAvailable) {
      meta.logger.info("Fast parser quality check failed, escalating to MU", {
        url: meta.rewrittenUrl ?? meta.url,
        reason: qualityCheck.reason,
        fastDurationMs,
      });

      return escalateToMinerU(
        meta,
        tempFilePath,
        base64Content,
        maxPages,
        qualityCheck.reason,
        result,
      );
    }

    meta.logger.info("Fast parser succeeded", {
      url: meta.rewrittenUrl ?? meta.url,
      durationMs: fastDurationMs,
      qualityOk: !qualityCheck.bad,
    });

    return result;
  } catch (error) {
    if (
      error instanceof RemoveFeatureError ||
      error instanceof AbortManagerThrownError
    ) {
      throw error;
    }

    meta.logger.warn("Fast parser failed", {
      url: meta.rewrittenUrl ?? meta.url,
      error,
    });
    Sentry.captureException(error);

    // Fallback to MU if available
    if (muAvailable) {
      return scrapePDFWithMinerU(
        createChildMeta(meta, "scrapePDF/scrapePDFWithMinerU"),
        tempFilePath,
        base64Content,
        maxPages,
      );
    }

    throw error;
  }
}

/**
 * Escalate from fast parser to MinerU after quality check failure.
 */
async function escalateToMinerU(
  meta: Meta,
  tempFilePath: string,
  base64Content: string,
  maxPages: number | undefined,
  escalationReason: string | undefined,
  fallbackResult: PDFProcessorResult,
): Promise<PDFProcessorResult> {
  const muStartedAt = Date.now();

  try {
    const result = await scrapePDFWithMinerU(
      createChildMeta(meta, "scrapePDF/scrapePDFWithMinerU"),
      tempFilePath,
      base64Content,
      maxPages,
    );
    const muDurationMs = Date.now() - muStartedAt;

    meta.logger.info("Escalation to MU succeeded", {
      url: meta.rewrittenUrl ?? meta.url,
      muDurationMs,
      escalationReason,
    });

    return result;
  } catch (error) {
    if (
      error instanceof RemoveFeatureError ||
      error instanceof AbortManagerThrownError
    ) {
      throw error;
    }

    meta.logger.warn("Escalation to MU failed, using fast parser result", {
      url: meta.rewrittenUrl ?? meta.url,
      error,
    });
    Sentry.captureException(error);

    return fallbackResult;
  }
}

/**
 * Process PDF using layout or OCR route (MU first, fallback to fast).
 */
async function processLayoutOrOcrRoute(
  meta: Meta,
  tempFilePath: string,
  base64Content: string,
  route: string,
  muAvailable: boolean,
  maxPages: number | undefined,
): Promise<PDFProcessorResult> {
  // Try MU first if available
  if (muAvailable) {
    const muStartedAt = Date.now();
    try {
      const result = await scrapePDFWithMinerU(
        createChildMeta(meta, "scrapePDF/scrapePDFWithMinerU"),
        tempFilePath,
        base64Content,
        maxPages,
      );
      const muDurationMs = Date.now() - muStartedAt;

      meta.logger.info("MU processing succeeded", {
        url: meta.rewrittenUrl ?? meta.url,
        route,
        durationMs: muDurationMs,
      });

      return result;
    } catch (error) {
      if (
        error instanceof RemoveFeatureError ||
        error instanceof AbortManagerThrownError
      ) {
        throw error;
      }

      meta.logger.warn("MU failed, falling back to fast parser", {
        url: meta.rewrittenUrl ?? meta.url,
        route,
        error,
      });
      Sentry.captureException(error);
    }
  }

  // Fallback to fast parser
  meta.logger.info("Using fast parser as fallback", {
    url: meta.rewrittenUrl ?? meta.url,
    route,
    muAvailable,
  });

  return scrapePDFWithPdfParse(
    createChildMeta(meta, "scrapePDF/scrapePDFWithPdfParse"),
    tempFilePath,
  );
}

export function pdfMaxReasonableTime(meta: Meta): number {
  return 120000; // Infinity, really
}
