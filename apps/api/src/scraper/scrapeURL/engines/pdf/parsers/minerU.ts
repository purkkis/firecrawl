import path from "node:path";
import { z } from "zod";
import * as marked from "marked";
import { config } from "../../../../../config";
import { robustFetch } from "../../../lib/fetch";
import {
  getPdfResultFromCache,
  savePdfResultToCache,
} from "../../../../../lib/gcs-pdf-cache";
import { Meta } from "../../..";
import { fireMinerUV2Experiment } from "./minerUExperiment";
import type { PDFProcessorResult } from "./pdfParse";

const MAX_FILE_SIZE = 19 * 1024 * 1024; // 19MB

/**
 * Check if MinerU is available for use.
 */
export function isMinerUAvailable(base64ContentLength: number): boolean {
  return (
    base64ContentLength < MAX_FILE_SIZE &&
    !!config.RUNPOD_MU_API_KEY &&
    !!config.RUNPOD_MU_POD_ID
  );
}

/**
 * Parse PDF using MinerU (RunPod MU) - handles complex layouts and scanned PDFs.
 */
export async function scrapePDFWithMinerU(
  meta: Meta,
  tempFilePath: string,
  base64Content: string,
  maxPages?: number,
): Promise<PDFProcessorResult> {
  meta.logger.debug("Processing PDF document with RunPod MU", {
    tempFilePath,
  });

  // Check cache first (only when not limiting pages)
  if (!maxPages) {
    try {
      const cachedResult = await getPdfResultFromCache(base64Content);
      if (cachedResult) {
        meta.logger.info("Using cached RunPod MU result for PDF", {
          tempFilePath,
        });
        return cachedResult;
      }
    } catch (error) {
      meta.logger.warn("Error checking PDF cache, proceeding with RunPod MU", {
        error,
        tempFilePath,
      });
    }
  }

  meta.abort.throwIfAborted();

  meta.logger.info("Max Pdf pages", {
    tempFilePath,
    maxPages,
  });

  // Fire MU v2 experiment in background (only for layout/ocr routes)
  fireMinerUV2Experiment(meta, tempFilePath, base64Content, maxPages);

  // Call MU v1
  const muV1StartedAt = Date.now();
  const podStart = await robustFetch({
    url: "https://api.runpod.ai/v2/" + config.RUNPOD_MU_POD_ID + "/runsync",
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.RUNPOD_MU_API_KEY}`,
    },
    body: {
      input: {
        file_content: base64Content,
        filename: path.basename(tempFilePath) + ".pdf",
        timeout: meta.abort.scrapeTimeout(),
        created_at: Date.now(),
        ...(maxPages !== undefined && { max_pages: maxPages }),
      },
    },
    logger: meta.logger.child({
      method: "scrapePDFWithMinerU/runsync/robustFetch",
    }),
    schema: z.object({
      id: z.string(),
      status: z.string(),
      output: z
        .object({
          markdown: z.string(),
        })
        .optional(),
    }),
    mock: meta.mock,
    abort: meta.abort.asSignal(),
  });

  let status: string = podStart.status;
  let result: { markdown: string } | undefined = podStart.output;

  // Poll for completion if still in progress
  if (status === "IN_QUEUE" || status === "IN_PROGRESS") {
    do {
      meta.abort.throwIfAborted();
      await new Promise(resolve => setTimeout(resolve, 2500));
      meta.abort.throwIfAborted();

      const podStatus = await robustFetch({
        url: `https://api.runpod.ai/v2/${config.RUNPOD_MU_POD_ID}/status/${podStart.id}`,
        method: "GET",
        headers: {
          Authorization: `Bearer ${config.RUNPOD_MU_API_KEY}`,
        },
        logger: meta.logger.child({
          method: "scrapePDFWithMinerU/status/robustFetch",
        }),
        schema: z.object({
          status: z.string(),
          output: z
            .object({
              markdown: z.string(),
            })
            .optional(),
        }),
        mock: meta.mock,
        abort: meta.abort.asSignal(),
      });

      status = podStatus.status;
      result = podStatus.output;
    } while (status !== "COMPLETED" && status !== "FAILED");
  }

  if (status === "FAILED") {
    const durationMs = Date.now() - muV1StartedAt;
    meta.logger.child({ method: "scrapePDF/MUv1" }).warn("MU v1 failed", {
      durationMs,
      url: meta.rewrittenUrl ?? meta.url,
    });
    throw new Error("RunPod MU failed to parse PDF");
  }

  if (!result) {
    const durationMs = Date.now() - muV1StartedAt;
    meta.logger.child({ method: "scrapePDF/MUv1" }).warn("MU v1 failed", {
      durationMs,
      url: meta.rewrittenUrl ?? meta.url,
    });
    throw new Error("RunPod MU returned no result");
  }

  const processorResult = {
    markdown: result.markdown,
    html: await marked.parse(result.markdown, { async: true }),
  };

  // Cache the result
  if (!meta.internalOptions.zeroDataRetention) {
    try {
      await savePdfResultToCache(base64Content, processorResult);
    } catch (error) {
      meta.logger.warn("Error saving PDF to cache", {
        error,
        tempFilePath,
      });
    }
  }

  const durationMs = Date.now() - muV1StartedAt;
  meta.logger.child({ method: "scrapePDF/MUv1" }).info("MU v1 completed", {
    durationMs,
    url: meta.rewrittenUrl ?? meta.url,
  });

  return processorResult;
}
