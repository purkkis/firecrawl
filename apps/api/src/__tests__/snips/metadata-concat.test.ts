import { extractMetadata } from "../../scraper/scrapeURL/lib/extractMetadata";
import { jest, describe, it, expect } from "@jest/globals";

describe("Metadata concatenation", () => {
  it("should concatenate description field into a string while preserving arrays for other metadata fields", async () => {
    const html = `
      <html>
        <head>
          <meta name="description" content="First description">
          <meta name="description" content="Second description">
          <meta property="og:locale:alternate" content="en_US">
          <meta property="og:locale:alternate" content="fr_FR">
          <meta name="keywords" content="first keyword">
          <meta name="keywords" content="second keyword">
        </head>
        <body></body>
      </html>
    `;

    const meta: any = {
      url: "https://example.com",
      id: "test-id",
      logger: {
        warn: jest.fn(),
        error: jest.fn(),
      },
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.description).toBeDefined();
    expect(Array.isArray(metadata.description)).toBe(false);
    expect(typeof metadata.description).toBe("string");
    expect(metadata.description).toBe("First description, Second description");

    expect(metadata.ogLocaleAlternate).toBeDefined();
    expect(Array.isArray(metadata.ogLocaleAlternate)).toBe(true);
    expect(metadata.ogLocaleAlternate).toEqual(["en_US", "fr_FR"]);

    expect(metadata.keywords).toBeDefined();
    expect(Array.isArray(metadata.keywords)).toBe(true);
    expect(metadata.keywords).toEqual(["first keyword", "second keyword"]);
  });

  it("should handle invalid favicon URLs gracefully", async () => {
    const html = `
      <html>
        <head>
          <link rel="icon" href="//#DOMAIN#/favicon.ico">
          <title>Test Page</title>
        </head>
        <body></body>
      </html>
    `;

    const mockLogger = {
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    };

    const meta: any = {
      url: "https://example.com",
      rewrittenUrl: "https://example.com",
      id: "test-id",
      logger: mockLogger,
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.favicon).toBeUndefined();
    expect(mockLogger.debug).toHaveBeenCalledWith(
      "Failed to resolve favicon URL",
      expect.objectContaining({
        favicon: expect.stringContaining("#DOMAIN#"),
        error: expect.any(Error),
      }),
    );
  });

  it("should successfully resolve valid favicon URLs", async () => {
    const html = `
      <html>
        <head>
          <link rel="icon" href="/favicon.ico">
          <title>Test Page</title>
        </head>
        <body></body>
      </html>
    `;

    const meta: any = {
      url: "https://example.com",
      id: "test-id",
      logger: {
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.favicon).toBe("https://example.com/favicon.ico");
  });

  it("should extract single JSON-LD script tag", async () => {
    const html = `
      <html>
        <head>
          <title>Product Page</title>
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Product",
              "name": "Test Product",
              "price": "99.99"
            }
          </script>
        </head>
        <body></body>
      </html>
    `;

    const meta: any = {
      url: "https://example.com",
      id: "test-id",
      logger: {
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.jsonLd).toBeDefined();
    expect(metadata.jsonLd["@type"]).toBe("Product");
    expect(metadata.jsonLd.name).toBe("Test Product");
    expect(metadata.jsonLd.price).toBe("99.99");
  });

  it("should extract multiple JSON-LD script tags as an array", async () => {
    const html = `
      <html>
        <head>
          <title>Product Page</title>
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Product",
              "name": "Test Product"
            }
          </script>
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Organization",
              "name": "Test Company"
            }
          </script>
        </head>
        <body></body>
      </html>
    `;

    const meta: any = {
      url: "https://example.com",
      id: "test-id",
      logger: {
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.jsonLd).toBeDefined();
    expect(Array.isArray(metadata.jsonLd)).toBe(true);
    expect(metadata.jsonLd.length).toBe(2);
    expect(metadata.jsonLd[0]["@type"]).toBe("Product");
    expect(metadata.jsonLd[1]["@type"]).toBe("Organization");
  });

  it("should skip invalid JSON-LD and extract valid ones", async () => {
    const html = `
      <html>
        <head>
          <title>Product Page</title>
          <script type="application/ld+json">
            { invalid json here }
          </script>
          <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "Product",
              "name": "Valid Product"
            }
          </script>
        </head>
        <body></body>
      </html>
    `;

    const meta: any = {
      url: "https://example.com",
      id: "test-id",
      logger: {
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.jsonLd).toBeDefined();
    expect(metadata.jsonLd["@type"]).toBe("Product");
    expect(metadata.jsonLd.name).toBe("Valid Product");
  });

  it("should return undefined jsonLd when no JSON-LD scripts exist", async () => {
    const html = `
      <html>
        <head>
          <title>Simple Page</title>
        </head>
        <body></body>
      </html>
    `;

    const meta: any = {
      url: "https://example.com",
      id: "test-id",
      logger: {
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
    };

    const metadata = await extractMetadata(meta, html);

    expect(metadata.jsonLd).toBeUndefined();
  });
});
