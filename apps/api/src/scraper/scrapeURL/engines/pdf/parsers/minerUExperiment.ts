import path from "node:path";
import { z } from "zod";
import { config } from "../../../../../config";
import { robustFetch } from "../../../lib/fetch";
import { Meta } from "../../..";

/**
 * Fire the MU v2 experiment in the background (non-blocking).
 * This is used to gather data for MU v2 without affecting the main flow.
 * Only fires for layout/ocr routes, not fast route.
 */
export function fireMinerUV2Experiment(
  meta: Meta,
  tempFilePath: string,
  base64Content: string,
  maxPages?: number,
): void {
  if (
    config.PDF_MU_V2_EXPERIMENT !== "true" ||
    !config.PDF_MU_V2_BASE_URL ||
    Math.random() * 100 >= config.PDF_MU_V2_EXPERIMENT_PERCENT
  ) {
    return;
  }

  // Fire and forget - don't await
  (async () => {
    const pdfParseId = crypto.randomUUID();
    const startedAt = Date.now();
    const logger = meta.logger.child({ method: "scrapePDF/MUv2Experiment" });

    logger.info("MU v2 experiment started", {
      scrapeId: meta.id,
      pdfParseId,
      url: meta.rewrittenUrl ?? meta.url,
      maxPages,
    });

    try {
      const resp = await robustFetch({
        url: config.PDF_MU_V2_BASE_URL ?? "",
        method: "POST",
        headers: config.PDF_MU_V2_API_KEY
          ? { Authorization: `Bearer ${config.PDF_MU_V2_API_KEY}` }
          : undefined,
        body: {
          input: {
            file_content: base64Content,
            filename: path.basename(tempFilePath) + ".pdf",
            timeout: meta.abort.scrapeTimeout(),
            created_at: Date.now(),
            id: pdfParseId,
            ...(maxPages !== undefined && { max_pages: maxPages }),
          },
        },
        logger,
        schema: z.any(),
        mock: meta.mock,
        abort: meta.abort.asSignal(),
      });

      const body: any = resp as any;
      const tokensIn = body?.metadata?.["total-input-tokens"];
      const tokensOut = body?.metadata?.["total-output-tokens"];
      const pages = body?.metadata?.["pdf-total-pages"];
      const durationMs = Date.now() - startedAt;

      logger.info("MU v2 experiment completed", {
        durationMs,
        url: meta.rewrittenUrl ?? meta.url,
        tokensIn,
        tokensOut,
        pages,
      });
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      logger.warn("MU v2 experiment failed", { error, durationMs });
    }
  })();
}
