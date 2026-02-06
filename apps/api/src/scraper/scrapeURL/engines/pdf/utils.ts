import { Meta } from "../..";
import { EngineScrapeResult } from "..";
import { downloadFile, fetchFileToBuffer } from "../utils/downloadFile";
import {
  PDFAntibotError,
  PDFInsufficientTimeError,
  PDFPrefetchFailed,
  EngineUnsuccessfulError,
} from "../../error";
import { readFile, unlink } from "node:fs/promises";
import type { Response } from "undici";
import { analyzePdf, PdfAnalysis } from "@mendable/firecrawl-rs";

const MILLISECONDS_PER_PAGE = 150;

/**
 * Handle non-parsing mode - just return base64 encoded content.
 */
export async function handleNonParsingMode(
  meta: Meta,
): Promise<EngineScrapeResult> {
  if (meta.pdfPrefetch !== undefined && meta.pdfPrefetch !== null) {
    const content = (await readFile(meta.pdfPrefetch.filePath)).toString(
      "base64",
    );
    return {
      url: meta.pdfPrefetch.url ?? meta.rewrittenUrl ?? meta.url,
      statusCode: meta.pdfPrefetch.status,
      html: content,
      markdown: content,
      proxyUsed: meta.pdfPrefetch.proxyUsed,
    };
  }

  const file = await fetchFileToBuffer(
    meta.rewrittenUrl ?? meta.url,
    meta.options.skipTlsVerification,
    {
      headers: meta.options.headers,
      signal: meta.abort.asSignal(),
    },
  );

  const ct = file.response.headers.get("Content-Type");
  if (ct && !ct.includes("application/pdf")) {
    if (meta.pdfPrefetch === undefined) {
      if (!meta.featureFlags.has("pdf")) {
        throw new EngineUnsuccessfulError("pdf");
      } else {
        throw new PDFAntibotError();
      }
    } else {
      throw new PDFPrefetchFailed();
    }
  }

  const content = file.buffer.toString("base64");
  return {
    url: file.response.url,
    statusCode: file.response.status,
    html: content,
    markdown: content,
    proxyUsed: "basic",
  };
}

/**
 * Get or download the PDF file.
 */
export async function getOrDownloadPDF(meta: Meta) {
  if (meta.pdfPrefetch !== undefined && meta.pdfPrefetch !== null) {
    return {
      response: meta.pdfPrefetch,
      tempFilePath: meta.pdfPrefetch.filePath,
    };
  }

  return downloadFile(
    meta.id,
    meta.rewrittenUrl ?? meta.url,
    meta.options.skipTlsVerification,
    {
      headers: meta.options.headers,
      signal: meta.abort.asSignal(),
    },
  );
}

/**
 * Validate the content type of the downloaded file.
 */
export async function validateContentType(
  response: any,
  meta: Meta,
): Promise<void> {
  if ((response as any).headers) {
    const r: Response = response as any;
    const ct = r.headers.get("Content-Type");
    if (ct && !ct.includes("application/pdf")) {
      if (meta.pdfPrefetch === undefined) {
        if (!meta.featureFlags.has("pdf")) {
          throw new EngineUnsuccessfulError("pdf");
        } else {
          throw new PDFAntibotError();
        }
      } else {
        throw new PDFPrefetchFailed();
      }
    }
  }
}

/**
 * Analyze PDF file for metadata and routing recommendation.
 */
export async function analyzePDFFile(
  tempFilePath: string,
  maxPages: number | undefined,
  meta: Meta,
): Promise<{
  pdfAnalysis: PdfAnalysis;
  effectivePageCount: number;
  analysisDurationMs: number;
}> {
  const analysisStartedAt = Date.now();
  const pdfAnalysis: PdfAnalysis = analyzePdf(tempFilePath);
  const analysisDurationMs = Date.now() - analysisStartedAt;

  const effectivePageCount = maxPages
    ? Math.min(pdfAnalysis.numPages, maxPages)
    : pdfAnalysis.numPages;

  meta.logger.info("PDF analysis completed", {
    durationMs: analysisDurationMs,
    url: meta.rewrittenUrl ?? meta.url,
    numPages: pdfAnalysis.numPages,
    effectivePageCount,
    recommendedRoute: pdfAnalysis.recommendedRoute,
    likelyScanned: pdfAnalysis.likelyScanned,
    extractedCharCount: pdfAnalysis.extractedCharCount,
    samplePages: pdfAnalysis.samplePages,
    imageXobjectCount: pdfAnalysis.imageXobjectCount,
    isEncrypted: pdfAnalysis.isEncrypted,
    emptyTextPages: pdfAnalysis.emptyTextPages,
  });

  return { pdfAnalysis, effectivePageCount, analysisDurationMs };
}

/**
 * Check if we have enough time to process this PDF.
 */
export function checkTimeoutConstraints(
  effectivePageCount: number,
  meta: Meta,
): void {
  if (
    effectivePageCount * MILLISECONDS_PER_PAGE >
    (meta.abort.scrapeTimeout() ?? Infinity)
  ) {
    throw new PDFInsufficientTimeError(
      effectivePageCount,
      effectivePageCount * MILLISECONDS_PER_PAGE + 5000,
    );
  }
}

/**
 * Create a child meta with a specific method logger.
 */
export function createChildMeta(meta: Meta, method: string): Meta {
  return {
    ...meta,
    logger: meta.logger.child({ method }),
  };
}

/**
 * Clean up temporary file.
 */
export async function cleanupTempFile(
  tempFilePath: string,
  meta: Meta,
): Promise<void> {
  try {
    await unlink(tempFilePath);
  } catch (error) {
    meta.logger?.warn("Failed to clean up temporary PDF file", {
      error,
      tempFilePath,
    });
  }
}
