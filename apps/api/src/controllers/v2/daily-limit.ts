import { Response } from "express";
import { ErrorResponse, RequestWithAuth } from "./types";
import {
  getDailyLimitStatus,
  updateDailyLimitSettings,
} from "../../services/billing/daily_limit";
import { z } from "zod";
import { logger } from "../../lib/logger";

// GET /v2/team/daily-limit response
interface DailyLimitResponse {
  success: true;
  data: {
    enabled: boolean;
    limit: number | null;
    dailyUsed: number;
    dailyRemaining: number | null;
    resetsAt: string;
  };
}

// PATCH /v2/team/daily-limit request schema
export const updateDailyLimitSchema = z
  .object({
    enabled: z.boolean(),
    limit: z.number().int().min(100).nullable().optional(),
  })
  .refine(
    data => {
      // If enabling, limit must be provided and valid
      if (data.enabled && (data.limit === null || data.limit === undefined)) {
        return false;
      }
      return true;
    },
    {
      message: "limit is required when enabling daily credit limit",
    },
  );

export type UpdateDailyLimitRequest = z.infer<typeof updateDailyLimitSchema>;

/**
 * GET /v2/team/daily-limit
 * Returns current daily limit settings and usage for the authenticated team.
 */
export async function dailyLimitController(
  req: RequestWithAuth,
  res: Response<DailyLimitResponse | ErrorResponse>,
): Promise<void> {
  const teamId = req.auth.team_id;

  try {
    const status = await getDailyLimitStatus(teamId);

    res.json({
      success: true,
      data: {
        enabled: status.enabled,
        limit: status.limit,
        dailyUsed: status.dailyUsed,
        dailyRemaining: status.dailyRemaining,
        resetsAt: status.resetsAt,
      },
    });
  } catch (error) {
    logger.error("Error fetching daily limit status", {
      teamId,
      error: error instanceof Error ? error.message : String(error),
    });
    res.status(500).json({
      success: false,
      error: "Failed to fetch daily limit status",
    });
  }
}

/**
 * PATCH /v2/team/daily-limit
 * Updates daily limit settings for the authenticated team.
 */
export async function updateDailyLimitController(
  req: RequestWithAuth<{}, UpdateDailyLimitRequest>,
  res: Response<DailyLimitResponse | ErrorResponse>,
): Promise<void> {
  const teamId = req.auth.team_id;

  // Validate request body
  const parseResult = updateDailyLimitSchema.safeParse(req.body);
  if (!parseResult.success) {
    res.status(400).json({
      success: false,
      error: parseResult.error.errors[0]?.message ?? "Invalid request body",
      details: parseResult.error.errors,
    });
    return;
  }

  const { enabled, limit } = parseResult.data;

  try {
    const result = await updateDailyLimitSettings(
      teamId,
      enabled,
      enabled ? limit! : null,
    );

    if (!result.success) {
      res.status(400).json({
        success: false,
        error: result.error ?? "Failed to update daily limit settings",
      });
      return;
    }

    // Return updated status
    const status = await getDailyLimitStatus(teamId);

    res.json({
      success: true,
      data: {
        enabled: status.enabled,
        limit: status.limit,
        dailyUsed: status.dailyUsed,
        dailyRemaining: status.dailyRemaining,
        resetsAt: status.resetsAt,
      },
    });
  } catch (error) {
    logger.error("Error updating daily limit settings", {
      teamId,
      enabled,
      limit,
      error: error instanceof Error ? error.message : String(error),
    });
    res.status(500).json({
      success: false,
      error: "Failed to update daily limit settings",
    });
  }
}
