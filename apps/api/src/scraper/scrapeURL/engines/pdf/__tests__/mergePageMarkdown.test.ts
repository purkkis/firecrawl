import {
  splitByPageMarkers,
  mapOcrResultToPages,
  mergePageMarkdown,
} from "../mergePageMarkdown";

describe("splitByPageMarkers", () => {
  it("splits markdown with page markers into a map", () => {
    const md = [
      "<!-- Page 1 -->",
      "Page one content",
      "",
      "<!-- Page 2 -->",
      "Page two content",
      "",
      "<!-- Page 3 -->",
      "Page three content",
    ].join("\n");

    const result = splitByPageMarkers(md);
    expect(result.size).toBe(3);
    expect(result.get(1)).toBe("Page one content");
    expect(result.get(2)).toBe("Page two content");
    expect(result.get(3)).toBe("Page three content");
  });

  it("returns empty map for markdown without markers", () => {
    const result = splitByPageMarkers("Just some text without markers");
    expect(result.size).toBe(0);
  });

  it("handles content with multiple lines per page", () => {
    const md = [
      "<!-- Page 1 -->",
      "Line 1",
      "Line 2",
      "Line 3",
      "",
      "<!-- Page 2 -->",
      "Other content",
    ].join("\n");

    const result = splitByPageMarkers(md);
    expect(result.size).toBe(2);
    expect(result.get(1)).toBe("Line 1\nLine 2\nLine 3");
    expect(result.get(2)).toBe("Other content");
  });

  it("handles non-sequential page numbers", () => {
    const md = [
      "<!-- Page 2 -->",
      "Page two",
      "",
      "<!-- Page 5 -->",
      "Page five",
    ].join("\n");

    const result = splitByPageMarkers(md);
    expect(result.size).toBe(2);
    expect(result.get(2)).toBe("Page two");
    expect(result.get(5)).toBe("Page five");
  });

  it("trims whitespace from page content", () => {
    const md = [
      "<!-- Page 1 -->",
      "",
      "  Content with spaces  ",
      "",
      "<!-- Page 2 -->",
      "Content",
    ].join("\n");

    const result = splitByPageMarkers(md);
    expect(result.get(1)).toBe("Content with spaces");
  });

  it("handles empty pages", () => {
    const md = [
      "<!-- Page 1 -->",
      "Content",
      "<!-- Page 2 -->",
      "<!-- Page 3 -->",
      "More content",
    ].join("\n");

    const result = splitByPageMarkers(md);
    expect(result.size).toBe(3);
    expect(result.get(1)).toBe("Content");
    expect(result.get(2)).toBe("");
    expect(result.get(3)).toBe("More content");
  });
});

describe("mapOcrResultToPages", () => {
  it("maps sequentially-numbered MinerU output to original page numbers", () => {
    const ocrMd = [
      "<!-- Page 1 -->",
      "OCR page content A",
      "",
      "<!-- Page 2 -->",
      "OCR page content B",
    ].join("\n");

    const result = mapOcrResultToPages(ocrMd, [3, 7]);
    expect(result.size).toBe(2);
    expect(result.get(3)).toBe("OCR page content A");
    expect(result.get(7)).toBe("OCR page content B");
  });

  it("handles single page OCR without markers", () => {
    const result = mapOcrResultToPages("Single page OCR content", [5]);
    expect(result.size).toBe(1);
    expect(result.get(5)).toBe("Single page OCR content");
  });

  it("handles multi-page OCR without markers (fallback)", () => {
    const result = mapOcrResultToPages("All OCR content together", [2, 4]);
    expect(result.size).toBe(1);
    expect(result.get(2)).toBe("All OCR content together");
  });

  it("handles empty OCR output", () => {
    const result = mapOcrResultToPages("", [1, 3]);
    expect(result.size).toBe(1);
    expect(result.get(1)).toBe("");
  });
});

describe("mergePageMarkdown", () => {
  it("merges rust and OCR pages in order", () => {
    const rustPages = new Map<number, string>([
      [1, "Text page 1"],
      [2, "Text page 2"],
      [4, "Text page 4"],
    ]);
    const ocrPages = new Map<number, string>([
      [3, "OCR page 3"],
      [5, "OCR page 5"],
    ]);

    const result = mergePageMarkdown(rustPages, ocrPages, 5);
    expect(result).toBe(
      "Text page 1\n\nText page 2\n\nOCR page 3\n\nText page 4\n\nOCR page 5",
    );
  });

  it("rust pages take priority over OCR pages", () => {
    const rustPages = new Map<number, string>([[1, "Rust version"]]);
    const ocrPages = new Map<number, string>([[1, "OCR version"]]);

    const result = mergePageMarkdown(rustPages, ocrPages, 1);
    expect(result).toBe("Rust version");
  });

  it("skips pages with no content from either source", () => {
    const rustPages = new Map<number, string>([[1, "Page 1"]]);
    const ocrPages = new Map<number, string>([[3, "Page 3"]]);

    const result = mergePageMarkdown(rustPages, ocrPages, 3);
    expect(result).toBe("Page 1\n\nPage 3");
  });

  it("handles all rust pages (no OCR)", () => {
    const rustPages = new Map<number, string>([
      [1, "Page 1"],
      [2, "Page 2"],
    ]);
    const ocrPages = new Map<number, string>();

    const result = mergePageMarkdown(rustPages, ocrPages, 2);
    expect(result).toBe("Page 1\n\nPage 2");
  });

  it("handles all OCR pages (no rust)", () => {
    const rustPages = new Map<number, string>();
    const ocrPages = new Map<number, string>([
      [1, "OCR 1"],
      [2, "OCR 2"],
    ]);

    const result = mergePageMarkdown(rustPages, ocrPages, 2);
    expect(result).toBe("OCR 1\n\nOCR 2");
  });

  it("skips empty content pages", () => {
    const rustPages = new Map<number, string>([
      [1, "Content"],
      [2, ""],
    ]);
    const ocrPages = new Map<number, string>([[3, "More content"]]);

    const result = mergePageMarkdown(rustPages, ocrPages, 3);
    expect(result).toBe("Content\n\nMore content");
  });

  it("returns empty string when no pages have content", () => {
    const result = mergePageMarkdown(new Map(), new Map(), 3);
    expect(result).toBe("");
  });
});
