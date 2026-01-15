import { supabase_rr_service } from "../supabase";
import { getValue, setValue } from "../redis";
import { redisRateLimitClient } from "../rate-limiter";
import { logger } from "../../lib/logger";

export interface DailyLimitSettings {
  enabled: boolean;
  limit: number | null;
}

export interface DailyLimitCheckResult {
  success: boolean;
  dailyUsed: number;
  dailyLimit: number | null;
  dailyRemaining: number | null;
  resetsAt: string;
  message?: string;
}

/**
 * Get the Redis key for daily usage tracking.
 * Uses UTC date to ensure consistent reset at midnight UTC.
 */
function getDailyUsageKey(teamId: string): string {
  const utcDate = new Date().toISOString().split("T")[0]; // YYYY-MM-DD
  return `daily_usage:${teamId}:${utcDate}`;
}

/**
 * Get the next UTC midnight as ISO string.
 */
function getNextMidnightUTC(): string {
  const now = new Date();
  const tomorrow = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1),
  );
  return tomorrow.toISOString();
}

/**
 * Get seconds until next UTC midnight.
 */
function getSecondsUntilMidnight(): number {
  const now = new Date();
  const tomorrow = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1),
  );
  return Math.ceil((tomorrow.getTime() - now.getTime()) / 1000);
}

/**
 * Get the current daily usage for a team.
 */
export async function getDailyUsage(teamId: string): Promise<number> {
  const key = getDailyUsageKey(teamId);
  const value = await redisRateLimitClient.get(key);
  return value ? parseInt(value, 10) : 0;
}

/**
 * Increment the daily usage for a team.
 * Uses INCRBY for atomic increment.
 * Sets TTL to expire after UTC midnight (plus buffer).
 */
export async function incrementDailyUsage(
  teamId: string,
  credits: number,
): Promise<number> {
  const key = getDailyUsageKey(teamId);

  // Use INCRBY for atomic increment
  const newValue = await redisRateLimitClient.incrby(key, credits);

  // Set expiry to 48 hours (to handle edge cases and provide buffer)
  // Only set if not already set (NX)
  const TTL_SECONDS = 48 * 60 * 60; // 48 hours
  await redisRateLimitClient.expire(key, TTL_SECONDS, "NX");

  return newValue;
}

/**
 * Get daily limit settings for a team from database (with caching).
 */
export async function getDailyLimitSettings(
  teamId: string,
): Promise<DailyLimitSettings> {
  const cacheKey = `team_daily_limit_${teamId}`;
  const cachedData = await getValue(cacheKey);

  if (cachedData) {
    const parsed = JSON.parse(cachedData);
    return {
      enabled: parsed.daily_credit_limit_enabled ?? false,
      limit: parsed.daily_credit_limit ?? null,
    };
  }

  const { data, error } = await supabase_rr_service
    .from("teams")
    .select("daily_credit_limit, daily_credit_limit_enabled")
    .eq("id", teamId)
    .single();

  if (error) {
    logger.error("Failed to fetch daily limit settings", {
      teamId,
      error: error.message,
    });
    return { enabled: false, limit: null };
  }

  const settings = {
    enabled: data?.daily_credit_limit_enabled ?? false,
    limit: data?.daily_credit_limit ?? null,
  };

  // Cache for 5 minutes (consistent with auto_recharge caching)
  await setValue(
    cacheKey,
    JSON.stringify({
      daily_credit_limit_enabled: settings.enabled,
      daily_credit_limit: settings.limit,
    }),
    300,
  );

  return settings;
}

/**
 * Clear the daily limit settings cache for a team.
 * Call this when settings are updated.
 */
export async function clearDailyLimitCache(teamId: string): Promise<void> {
  const cacheKey = `team_daily_limit_${teamId}`;
  await redisRateLimitClient.del(cacheKey);
}

/**
 * Check if a team's daily limit would be exceeded by the requested credits.
 */
export async function checkDailyLimit(
  teamId: string,
  creditsToUse: number,
): Promise<DailyLimitCheckResult> {
  const settings = await getDailyLimitSettings(teamId);
  const dailyUsed = await getDailyUsage(teamId);
  const resetsAt = getNextMidnightUTC();

  // If daily limit is not enabled, always allow
  if (!settings.enabled || settings.limit === null) {
    return {
      success: true,
      dailyUsed,
      dailyLimit: null,
      dailyRemaining: null,
      resetsAt,
    };
  }

  const dailyLimit = settings.limit;
  const dailyRemaining = Math.max(0, dailyLimit - dailyUsed);
  const wouldExceed = dailyUsed + creditsToUse > dailyLimit;

  if (wouldExceed) {
    logger.warn("Daily credit limit would be exceeded", {
      teamId,
      creditsToUse,
      dailyUsed,
      dailyLimit,
      dailyRemaining,
    });

    return {
      success: false,
      dailyUsed,
      dailyLimit,
      dailyRemaining,
      resetsAt,
      message: `Daily credit limit exceeded. You have used ${dailyUsed} of your ${dailyLimit} daily credit limit. Your limit resets at midnight UTC (${resetsAt}). To adjust your daily limit, visit https://firecrawl.dev/app/settings?tab=billing`,
    };
  }

  return {
    success: true,
    dailyUsed,
    dailyLimit,
    dailyRemaining,
    resetsAt,
  };
}

/**
 * Get current daily limit status for API response.
 */
export async function getDailyLimitStatus(teamId: string): Promise<{
  enabled: boolean;
  limit: number | null;
  dailyUsed: number;
  dailyRemaining: number | null;
  resetsAt: string;
}> {
  const settings = await getDailyLimitSettings(teamId);
  const dailyUsed = await getDailyUsage(teamId);
  const resetsAt = getNextMidnightUTC();

  return {
    enabled: settings.enabled,
    limit: settings.limit,
    dailyUsed,
    dailyRemaining:
      settings.limit !== null ? Math.max(0, settings.limit - dailyUsed) : null,
    resetsAt,
  };
}

/**
 * Update daily limit settings for a team.
 */
export async function updateDailyLimitSettings(
  teamId: string,
  enabled: boolean,
  limit: number | null,
): Promise<{ success: boolean; error?: string }> {
  // Validate limit when enabled
  if (enabled && (limit === null || limit < 100)) {
    return {
      success: false,
      error: "Daily credit limit must be at least 100 when enabled",
    };
  }

  const { error } = await supabase_rr_service
    .from("teams")
    .update({
      daily_credit_limit_enabled: enabled,
      daily_credit_limit: enabled ? limit : null,
    })
    .eq("id", teamId);

  if (error) {
    logger.error("Failed to update daily limit settings", {
      teamId,
      error: error.message,
    });
    return { success: false, error: error.message };
  }

  // Clear cache so new settings take effect immediately
  await clearDailyLimitCache(teamId);

  logger.info("Daily limit settings updated", {
    teamId,
    enabled,
    limit,
  });

  return { success: true };
}
