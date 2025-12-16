import { describeIf, HAS_AI, TEST_PRODUCTION } from "../lib";
import { extract, idmux, Identity, scrapeTimeout } from "./lib";

let identity: Identity;

beforeAll(async () => {
  identity = await idmux({
    name: "asml-investor-extract",
    concurrency: 100,
    credits: 1000000,
  });
}, 10000);

const ASML_URLS = {
  financialCalendar: "https://www.asml.com/en/investors/financial-calendar",
  shareholderMeetings:
    "https://www.asml.com/en/investors/shares/shareholder-meetings",
  dividend:
    "https://www.asml.com/en/investors/why-invest-in-asml/capital-return-and-financing",
};

const asmlInvestorSchema = {
  type: "object",
  properties: {
    ex_dividend_date: {
      type: "string",
      description:
        "The ex-dividend date (the date on which the stock starts trading without the dividend). Can be for Euronext or Nasdaq.",
    },
    dividend_record_date: {
      type: "string",
      description:
        "The dividend record date (the date by which you must be a shareholder to receive the dividend).",
    },
    dividend_payment_date: {
      type: "string",
      description:
        "The dividend payment date (the date when the dividend is actually paid to shareholders).",
    },
    annual_general_meeting_date: {
      type: "string",
      description:
        "The date of the Annual General Meeting (AGM) of shareholders.",
    },
    extraordinary_general_meeting_date: {
      type: "string",
      description:
        "The date of any Extraordinary General Meeting (EGM) of shareholders, if scheduled.",
    },
  },
  required: [
    "ex_dividend_date",
    "dividend_record_date",
    "dividend_payment_date",
    "annual_general_meeting_date",
  ],
};

describeIf(TEST_PRODUCTION || HAS_AI)(
  "ASML Investor Page Extraction tests",
  () => {
    it.concurrent(
      "extracts dividend dates from capital return page",
      async () => {
        const res = await extract(
          {
            urls: [ASML_URLS.dividend],
            schema: {
              type: "object",
              properties: {
                ex_dividend_date_euronext: {
                  type: "string",
                  description: "The ex-dividend date for Euronext exchange",
                },
                ex_dividend_date_nasdaq: {
                  type: "string",
                  description: "The ex-dividend date for Nasdaq exchange",
                },
                dividend_record_date: {
                  type: "string",
                  description: "The dividend record date",
                },
                dividend_payment_date: {
                  type: "string",
                  description: "The dividend payment date",
                },
              },
              required: [
                "ex_dividend_date_euronext",
                "dividend_record_date",
                "dividend_payment_date",
              ],
            },
            scrapeOptions: {
              timeout: 75000,
            },
            origin: "api-sdk",
          },
          identity,
        );

        expect(res.data).toHaveProperty("ex_dividend_date_euronext");
        expect(typeof res.data.ex_dividend_date_euronext).toBe("string");
        expect(res.data).toHaveProperty("dividend_record_date");
        expect(typeof res.data.dividend_record_date).toBe("string");
        expect(res.data).toHaveProperty("dividend_payment_date");
        expect(typeof res.data.dividend_payment_date).toBe("string");
      },
      scrapeTimeout + 90000,
    );

    it.concurrent(
      "extracts AGM date from financial calendar page",
      async () => {
        const res = await extract(
          {
            urls: [ASML_URLS.financialCalendar],
            schema: {
              type: "object",
              properties: {
                annual_general_meeting_date: {
                  type: "string",
                  description: "The date of the Annual General Meeting (AGM)",
                },
                annual_general_meeting_location: {
                  type: "string",
                  description:
                    "The location of the Annual General Meeting (AGM)",
                },
              },
              required: ["annual_general_meeting_date"],
            },
            scrapeOptions: {
              timeout: 75000,
            },
            origin: "api-sdk",
          },
          identity,
        );

        expect(res.data).toHaveProperty("annual_general_meeting_date");
        expect(typeof res.data.annual_general_meeting_date).toBe("string");
      },
      scrapeTimeout + 90000,
    );

    it.concurrent(
      "extracts AGM dates from shareholder meetings page",
      async () => {
        const res = await extract(
          {
            urls: [ASML_URLS.shareholderMeetings],
            schema: {
              type: "object",
              properties: {
                latest_agm_date: {
                  type: "string",
                  description:
                    "The date of the most recent Annual General Meeting (AGM)",
                },
                latest_agm_year: {
                  type: "string",
                  description:
                    "The year of the most recent Annual General Meeting (AGM)",
                },
              },
              required: ["latest_agm_date"],
            },
            scrapeOptions: {
              timeout: 75000,
            },
            origin: "api-sdk",
          },
          identity,
        );

        expect(res.data).toHaveProperty("latest_agm_date");
        expect(typeof res.data.latest_agm_date).toBe("string");
      },
      scrapeTimeout + 90000,
    );

    it.concurrent(
      "extracts all investor dates from multiple ASML pages",
      async () => {
        const res = await extract(
          {
            urls: [ASML_URLS.financialCalendar, ASML_URLS.dividend],
            schema: asmlInvestorSchema,
            scrapeOptions: {
              timeout: 75000,
            },
            origin: "api-sdk",
          },
          identity,
        );

        expect(res.data).toHaveProperty("ex_dividend_date");
        expect(typeof res.data.ex_dividend_date).toBe("string");
        expect(res.data).toHaveProperty("dividend_record_date");
        expect(typeof res.data.dividend_record_date).toBe("string");
        expect(res.data).toHaveProperty("dividend_payment_date");
        expect(typeof res.data.dividend_payment_date).toBe("string");
        expect(res.data).toHaveProperty("annual_general_meeting_date");
        expect(typeof res.data.annual_general_meeting_date).toBe("string");
      },
      scrapeTimeout + 120000,
    );
  },
);
