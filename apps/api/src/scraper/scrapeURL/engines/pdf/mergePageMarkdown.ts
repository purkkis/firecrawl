const PAGE_MARKER_RE = /<!-- Page (\d+) -->/;

/**
 * Split a markdown string containing `<!-- Page N -->` markers into a map
 * of page number → content.
 */
export function splitByPageMarkers(markdown: string): Map<number, string> {
  const pages = new Map<number, string>();
  const lines = markdown.split("\n");

  let currentPage: number | null = null;
  let currentLines: string[] = [];

  for (const line of lines) {
    const match = line.match(PAGE_MARKER_RE);
    if (match) {
      if (currentPage !== null) {
        pages.set(currentPage, currentLines.join("\n").trim());
      }
      currentPage = parseInt(match[1], 10);
      currentLines = [];
    } else {
      currentLines.push(line);
    }
  }

  // Flush last page
  if (currentPage !== null) {
    pages.set(currentPage, currentLines.join("\n").trim());
  }

  return pages;
}

/**
 * Map MinerU OCR output (a single markdown string) back to the original
 * page numbers it was generated from.
 *
 * MinerU processes only the subset of pages we send, so its output pages
 * are numbered 1..N sequentially. We map them back to the original
 * page numbers provided in `ocrPageNumbers`.
 */
export function mapOcrResultToPages(
  ocrMarkdown: string,
  ocrPageNumbers: number[],
): Map<number, string> {
  const pages = new Map<number, string>();

  // If MinerU inserted page markers, split on them
  const markerSplit = splitByPageMarkers(ocrMarkdown);

  if (markerSplit.size > 0) {
    // MinerU output has page markers — map sequential pages to original numbers
    const sortedKeys = [...markerSplit.keys()].sort((a, b) => a - b);
    for (let i = 0; i < sortedKeys.length && i < ocrPageNumbers.length; i++) {
      const content = markerSplit.get(sortedKeys[i]);
      if (content !== undefined) {
        pages.set(ocrPageNumbers[i], content);
      }
    }
  } else {
    // No markers — treat entire output as covering all OCR pages.
    // Split evenly by double-newline paragraphs as best effort, or assign all
    // to first page if only one page.
    if (ocrPageNumbers.length === 1) {
      pages.set(ocrPageNumbers[0], ocrMarkdown.trim());
    } else {
      // Assign entire output to first OCR page as fallback
      pages.set(ocrPageNumbers[0], ocrMarkdown.trim());
    }
  }

  return pages;
}

/**
 * Merge Rust-extracted text pages and MinerU OCR pages into a single
 * ordered markdown string.
 *
 * Pages are ordered by page number. Rust pages take priority for text pages,
 * OCR pages fill in for scanned pages.
 */
export function mergePageMarkdown(
  rustPages: Map<number, string>,
  ocrPages: Map<number, string>,
  totalPages: number,
): string {
  const parts: string[] = [];

  for (let page = 1; page <= totalPages; page++) {
    const content = rustPages.get(page) ?? ocrPages.get(page);
    if (content !== undefined && content.length > 0) {
      parts.push(content);
    }
  }

  return parts.join("\n\n");
}
