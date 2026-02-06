import { PDFParse } from "pdf-parse";
import { readFile } from "node:fs/promises";
import escapeHtml from "escape-html";
import { Meta } from "../../..";

export type PDFProcessorResult = { html: string; markdown?: string };

/**
 * Parse PDF using pdf-parse library (fast text extraction).
 */
export async function scrapePDFWithPdfParse(
  meta: Meta,
  tempFilePath: string,
): Promise<PDFProcessorResult> {
  meta.logger.debug("Processing PDF document with pdf-parse", { tempFilePath });

  const parser = new PDFParse({ data: await readFile(tempFilePath) });
  try {
    const result = await parser.getText();
    const escaped = escapeHtml(result.text);

    return {
      markdown: escaped,
      html: escaped,
    };
  } finally {
    await parser.destroy();
  }
}

/**
 * Check if fast parser output is bad quality and needs escalation to MU.
 * Returns { bad: boolean, reason?: string }
 */
export function isBadFastOutput(
  text: string,
  pages: number,
): { bad: boolean; reason?: string } {
  // Empty output
  if (!text || text.trim().length === 0) {
    return { bad: true, reason: "empty_output" };
  }

  // Low chars per page (< 200)
  const charCount = text.length;
  const charsPerPage = pages > 0 ? charCount / pages : charCount;
  if (charsPerPage < 200) {
    return {
      bad: true,
      reason: `low_chars_per_page:${Math.round(charsPerPage)}`,
    };
  }

  // High replacement character ratio (> 0.005)
  const replacementCharCount = (text.match(/\uFFFD/g) || []).length;
  const replacementRatio = charCount > 0 ? replacementCharCount / charCount : 0;
  if (replacementRatio > 0.005) {
    return {
      bad: true,
      reason: `high_replacement_ratio:${replacementRatio.toFixed(4)}`,
    };
  }

  // Fragmented text check: >60% of non-empty lines are < 20 chars
  const lines = text.split("\n").filter(line => line.trim().length > 0);
  if (lines.length > 0) {
    const shortLines = lines.filter(line => line.trim().length < 20).length;
    const shortLineRatio = shortLines / lines.length;
    if (shortLineRatio > 0.6) {
      return {
        bad: true,
        reason: `fragmented_text:${shortLineRatio.toFixed(2)}`,
      };
    }
  }

  return { bad: false };
}
