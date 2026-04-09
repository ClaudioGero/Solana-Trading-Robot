import { createClient } from "redis";
import { loadConfig } from "./config.js";
import { logger } from "./logger.js";
import { getBirdeyeWalletPnlSummary } from "./providers/birdeye.js";
import {
  getMobulaWalletPortfolioSummary,
  type MobulaWalletPortfolioSummary,
} from "./providers/mobula.js";

const WALLET_ANALYSIS_NAMESPACE = "sb:wallet_analysis";

export type WalletAnalysisRequest = {
  includeDisabled?: boolean;
  forceRefresh?: boolean;
  maxWallets?: number;
  requestDelayMs?: number;
  retryCount?: number;
  retryDelayMs?: number;
  persist?: boolean;
};

export type WalletSnapshot = {
  label: string;
  address: string;
  enabled: boolean;
  strategy: string | null;
  collectedAt: string;
  metrics: {
    winRate30d: number | null;
    pnl30dUsd: number | null;
    totalWalletBalanceUsd: number | null;
    nativeSolBalance: number | null;
    nativeSolBalanceUsd: number | null;
    assetCount: number | null;
    avgTradeSizeUsd: number | null;
    buySellRatio: number | null;
    avgHoldMinutes: number | null;
    recentActivityScore: number | null;
    consistencyScore: number | null;
    copytradeFriendliness: number | null;
    rateLimitConfidence: number;
  };
  providers: {
    birdeye: ProviderResult;
    mobula: ProviderResult;
  };
  ranking: {
    score: number;
    verdict: "follow" | "watch" | "ignore";
    summary: string;
    reasons: string[];
  };
  deltas: {
    scoreVsPrevious: number | null;
    pnl30dUsdVsPrevious: number | null;
    winRate30dVsPrevious: number | null;
  };
};

type ProviderResult = {
  ok: boolean;
  cached: boolean;
  attempts: number;
  error: string | null;
};

type ProviderCacheEntry<T> = {
  expiresAt: number;
  value: T;
};

type PreviousWalletSnapshot = {
  collectedAt?: string;
  metrics?: {
    winRate30d?: number | null;
    pnl30dUsd?: number | null;
  };
  ranking?: {
    score?: number;
  };
};

export type WalletAnalysisReport = {
  generatedAt: string;
  request: Required<Pick<WalletAnalysisRequest, "includeDisabled" | "forceRefresh" | "persist">> &
    Pick<WalletAnalysisRequest, "maxWallets" | "requestDelayMs" | "retryCount" | "retryDelayMs">;
  totals: {
    totalWallets: number;
    enabledWallets: number;
    analyzedWallets: number;
    follow: number;
    watch: number;
    ignore: number;
    withErrors: number;
  };
  topWallets: WalletSnapshot[];
  degradingWallets: WalletSnapshot[];
  newCandidates: WalletSnapshot[];
  wallets: WalletSnapshot[];
  openClawContext: OpenClawWalletResearchContext;
};

export type OpenClawWalletResearchContext = {
  generatedAt: string;
  summary: {
    totalWallets: number;
    follow: number;
    watch: number;
    ignore: number;
    withErrors: number;
  };
  wallets: Array<{
    label: string;
    address: string;
    enabled: boolean;
    strategy: string | null;
    verdict: "follow" | "watch" | "ignore";
    score: number;
    summary: string;
    reasons: string[];
    metrics: WalletSnapshot["metrics"];
    deltas: WalletSnapshot["deltas"];
  }>;
};

const providerCache = new Map<string, ProviderCacheEntry<unknown>>();

export async function analyzeAlphaWallets(
  input: WalletAnalysisRequest = {},
): Promise<WalletAnalysisReport> {
  const request = normalizeRequest(input);
  const config = loadConfig();
  const generatedAt = new Date().toISOString();
  const selectedWallets = config.alphaWallets.wallets
    .filter((wallet) => request.includeDisabled || wallet.enabled)
    .slice(0, request.maxWallets ?? Number.MAX_SAFE_INTEGER);

  const persistence = request.persist ? await createPersistence() : null;
  const snapshots: WalletSnapshot[] = [];

  for (let index = 0; index < selectedWallets.length; index += 1) {
    const wallet = selectedWallets[index];
    const previous = persistence ? await persistence.getLatestSnapshot(wallet.address) : null;
    const snapshot = await collectWalletSnapshot(wallet, previous, request, generatedAt);
    snapshots.push(snapshot);

    if (persistence) {
      await persistence.writeSnapshot(snapshot);
    }

    if (request.requestDelayMs > 0 && index < selectedWallets.length - 1) {
      await sleep(request.requestDelayMs);
    }
  }

  snapshots.sort((left, right) => right.ranking.score - left.ranking.score);
  const report = buildReport(snapshots, request, generatedAt);

  if (persistence) {
    await persistence.writeLatestReport(report);
    await persistence.close();
  }

  return report;
}

export async function getStoredLatestWalletAnalysis(): Promise<WalletAnalysisReport | null> {
  const persistence = await createPersistence();
  if (!persistence) {
    return null;
  }

  try {
    return await persistence.getLatestReport();
  } finally {
    await persistence.close();
  }
}

async function collectWalletSnapshot(
  wallet: ReturnType<typeof loadConfig>["alphaWallets"]["wallets"][number],
  previous: PreviousWalletSnapshot | null,
  request: ReturnType<typeof normalizeRequest>,
  collectedAt: string,
): Promise<WalletSnapshot> {
  const [birdeye, mobula] = await Promise.all([
    withProviderResilience(
      `birdeye:${wallet.address}`,
      request,
      async () => getBirdeyeWalletPnlSummary(wallet.address),
    ),
    withProviderResilience(
      `mobula:${wallet.address}`,
      request,
      async () => getMobulaWalletPortfolioSummary(wallet.address),
    ),
  ]);

  const metrics = deriveMetrics(birdeye.value, mobula.value, birdeye.provider, mobula.provider);
  const ranking = rankWallet(wallet, metrics, birdeye.provider, mobula.provider);
  const deltas = {
    scoreVsPrevious: numberDelta(ranking.score, previous?.ranking?.score),
    pnl30dUsdVsPrevious: numberDelta(metrics.pnl30dUsd, previous?.metrics?.pnl30dUsd),
    winRate30dVsPrevious: numberDelta(metrics.winRate30d, previous?.metrics?.winRate30d),
  };

  return {
    label: wallet.label,
    address: wallet.address,
    enabled: wallet.enabled,
    strategy: wallet.strategy ?? null,
    collectedAt,
    metrics,
    providers: {
      birdeye: birdeye.provider,
      mobula: mobula.provider,
    },
    ranking: {
      ...ranking,
      summary: ranking.summary,
      reasons: ranking.reasons,
    },
    deltas,
  };
}

function deriveMetrics(
  birdeye: Awaited<ReturnType<typeof getBirdeyeWalletPnlSummary>> | null,
  mobula: MobulaWalletPortfolioSummary | null,
  birdeyeProvider: ProviderResult,
  mobulaProvider: ProviderResult,
): WalletSnapshot["metrics"] {
  const winRate30d = normalizeWinRatePercent(birdeye?.winRate ?? null);
  const recentActivityScore = normalizeBoundedScore([
    scoreFromThreshold(mobula?.assetCount, 5, 30),
    scoreFromThreshold(mobula?.nativeSolBalance, 0.5, 15),
  ]);
  const consistencyScore = normalizeBoundedScore([
    scoreFromThreshold(winRate30d, 35, 65),
    scoreFromThreshold(birdeye?.totalPnlUsd, -500, 5000),
  ]);
  const copytradeFriendliness = normalizeBoundedScore([
    scoreFromThreshold(winRate30d, 40, 70),
    scoreFromThreshold(birdeye?.totalPnlUsd, -250, 4000),
    scoreFromThreshold(mobula?.nativeSolBalance, 0.5, 8),
  ]);

  return {
    winRate30d,
    pnl30dUsd: birdeye?.totalPnlUsd ?? null,
    totalWalletBalanceUsd: mobula?.totalWalletBalanceUsd ?? null,
    nativeSolBalance: mobula?.nativeSolBalance ?? null,
    nativeSolBalanceUsd: mobula?.nativeSolBalanceUsd ?? null,
    assetCount: mobula?.assetCount ?? null,
    avgTradeSizeUsd: null,
    buySellRatio: null,
    avgHoldMinutes: null,
    recentActivityScore,
    consistencyScore,
    copytradeFriendliness,
    rateLimitConfidence: providerConfidence([birdeyeProvider, mobulaProvider]),
  };
}

function rankWallet(
  wallet: { enabled: boolean; label: string },
  metrics: WalletSnapshot["metrics"],
  birdeyeProvider: ProviderResult,
  mobulaProvider: ProviderResult,
): WalletSnapshot["ranking"] {
  let score = 50;

  score += weightedScore(metrics.winRate30d, 35, 75, 18);
  score += weightedScore(metrics.pnl30dUsd, -2000, 5000, 22);
  score += weightedScore(metrics.nativeSolBalance, 0.5, 12, 10);
  score += weightedScore(metrics.totalWalletBalanceUsd, 500, 25000, 8);
  score += weightedScore(metrics.consistencyScore, 35, 80, 12);
  score += weightedScore(metrics.copytradeFriendliness, 35, 80, 12);
  score += weightedScore(metrics.recentActivityScore, 20, 80, 6);
  score += weightedScore(metrics.rateLimitConfidence, 0.5, 1, 12);

  if (!birdeyeProvider.ok) {
    score -= 12;
  }
  if (!mobulaProvider.ok) {
    score -= 8;
  }
  if (!wallet.enabled) {
    score -= 3;
  }

  score = clamp(Math.round(score), 0, 100);

  const verdict: WalletSnapshot["ranking"]["verdict"] =
    score >= 70 ? "follow" : score >= 50 ? "watch" : "ignore";
  const reasons = summarizeReasons(metrics, birdeyeProvider, mobulaProvider);

  return {
    score,
    verdict,
    summary: `${wallet.label} is ${verdict} at ${score}/100.`,
    reasons,
  };
}

function summarizeReasons(
  metrics: WalletSnapshot["metrics"],
  birdeyeProvider: ProviderResult,
  mobulaProvider: ProviderResult,
): string[] {
  const reasons: string[] = [];

  if (metrics.winRate30d != null) {
    reasons.push(`30d win rate ${formatPct(metrics.winRate30d)}.`);
  } else {
    reasons.push("30d win rate unavailable.");
  }

  if (metrics.pnl30dUsd != null) {
    reasons.push(`30d PnL ${formatUsd(metrics.pnl30dUsd)}.`);
  } else {
    reasons.push("30d PnL unavailable.");
  }

  if (metrics.nativeSolBalance != null) {
    reasons.push(`SOL balance ${formatNumber(metrics.nativeSolBalance, 2)}.`);
  }

  if (metrics.totalWalletBalanceUsd != null) {
    reasons.push(`Wallet balance ${formatUsd(metrics.totalWalletBalanceUsd)}.`);
  }

  if (!birdeyeProvider.ok || !mobulaProvider.ok) {
    reasons.push("Provider reliability reduced confidence in this pass.");
  } else if (metrics.rateLimitConfidence < 1) {
    reasons.push("Provider cache/retry logic was needed during this pass.");
  }

  return reasons;
}

function buildReport(
  snapshots: WalletSnapshot[],
  request: ReturnType<typeof normalizeRequest>,
  generatedAt: string,
): WalletAnalysisReport {
  const follow = snapshots.filter((wallet) => wallet.ranking.verdict === "follow");
  const watch = snapshots.filter((wallet) => wallet.ranking.verdict === "watch");
  const ignore = snapshots.filter((wallet) => wallet.ranking.verdict === "ignore");
  const withErrors = snapshots.filter(
    (wallet) => !wallet.providers.birdeye.ok || !wallet.providers.mobula.ok,
  );
  const degradingWallets = snapshots
    .filter((wallet) => (wallet.deltas.scoreVsPrevious ?? 0) <= -10)
    .sort((left, right) => (left.deltas.scoreVsPrevious ?? 0) - (right.deltas.scoreVsPrevious ?? 0));
  const newCandidates = snapshots.filter(
    (wallet) => !wallet.enabled && (wallet.ranking.verdict === "follow" || wallet.ranking.verdict === "watch"),
  );

  return {
    generatedAt,
    request,
    totals: {
      totalWallets: snapshots.length,
      enabledWallets: snapshots.filter((wallet) => wallet.enabled).length,
      analyzedWallets: snapshots.length,
      follow: follow.length,
      watch: watch.length,
      ignore: ignore.length,
      withErrors: withErrors.length,
    },
    topWallets: snapshots.slice(0, 10),
    degradingWallets,
    newCandidates,
    wallets: snapshots,
    openClawContext: {
      generatedAt,
      summary: {
        totalWallets: snapshots.length,
        follow: follow.length,
        watch: watch.length,
        ignore: ignore.length,
        withErrors: withErrors.length,
      },
      wallets: snapshots.map((wallet) => ({
        label: wallet.label,
        address: wallet.address,
        enabled: wallet.enabled,
        strategy: wallet.strategy,
        verdict: wallet.ranking.verdict,
        score: wallet.ranking.score,
        summary: wallet.ranking.summary,
        reasons: wallet.ranking.reasons,
        metrics: wallet.metrics,
        deltas: wallet.deltas,
      })),
    },
  };
}

async function withProviderResilience<T>(
  cacheKey: string,
  request: ReturnType<typeof normalizeRequest>,
  fetcher: () => Promise<T>,
): Promise<{ value: T | null; provider: ProviderResult }> {
  const cached = !request.forceRefresh ? getCachedValue<T>(cacheKey) : null;
  if (cached != null) {
    return {
      value: cached,
      provider: {
        ok: true,
        cached: true,
        attempts: 0,
        error: null,
      },
    };
  }

  let attempts = 0;
  let lastError: string | null = null;

  while (attempts <= request.retryCount) {
    attempts += 1;
    try {
      const value = await fetcher();
      setCachedValue(cacheKey, value, 60_000);
      return {
        value,
        provider: {
          ok: true,
          cached: false,
          attempts,
          error: null,
        },
      };
    } catch (error) {
      lastError = error instanceof Error ? error.message : "provider request failed";
      if (attempts > request.retryCount) {
        break;
      }
      await sleep(request.retryDelayMs * attempts);
    }
  }

  return {
    value: null,
    provider: {
      ok: false,
      cached: false,
      attempts,
      error: lastError,
    },
  };
}

function getCachedValue<T>(cacheKey: string): T | null {
  const cached = providerCache.get(cacheKey);
  if (!cached) {
    return null;
  }
  if (cached.expiresAt <= Date.now()) {
    providerCache.delete(cacheKey);
    return null;
  }
  return cached.value as T;
}

function setCachedValue<T>(cacheKey: string, value: T, ttlMs: number): void {
  providerCache.set(cacheKey, {
    expiresAt: Date.now() + ttlMs,
    value,
  });
}

function normalizeRequest(input: WalletAnalysisRequest) {
  return {
    includeDisabled: input.includeDisabled ?? true,
    forceRefresh: input.forceRefresh ?? false,
    maxWallets:
      input.maxWallets != null && Number.isInteger(input.maxWallets) && input.maxWallets > 0
        ? input.maxWallets
        : undefined,
    requestDelayMs:
      input.requestDelayMs != null && input.requestDelayMs >= 0
        ? Math.trunc(input.requestDelayMs)
        : parseEnvInt("WALLET_ANALYSIS_DELAY_MS", 1_200),
    retryCount:
      input.retryCount != null && input.retryCount >= 0
        ? Math.trunc(input.retryCount)
        : parseEnvInt("WALLET_ANALYSIS_RETRY_COUNT", 2),
    retryDelayMs:
      input.retryDelayMs != null && input.retryDelayMs >= 0
        ? Math.trunc(input.retryDelayMs)
        : parseEnvInt("WALLET_ANALYSIS_RETRY_DELAY_MS", 1_500),
    persist: input.persist ?? true,
  };
}

async function createPersistence(): Promise<{
  getLatestSnapshot(walletAddress: string): Promise<PreviousWalletSnapshot | null>;
  writeSnapshot(snapshot: WalletSnapshot): Promise<void>;
  getLatestReport(): Promise<WalletAnalysisReport | null>;
  writeLatestReport(report: WalletAnalysisReport): Promise<void>;
  close(): Promise<void>;
} | null> {
  const redisUrl = process.env.REDIS_URL;
  if (!redisUrl) {
    return null;
  }

  const client = createClient({ url: redisUrl });
  try {
    await client.connect();
  } catch (error) {
    logger.warn({ err: error }, "wallet analysis could not connect to redis; running without persistence");
    return null;
  }

  return {
    async getLatestSnapshot(walletAddress: string): Promise<PreviousWalletSnapshot | null> {
      const raw = await client.get(`${WALLET_ANALYSIS_NAMESPACE}:latest:${walletAddress}`);
      return parseJson<PreviousWalletSnapshot>(raw);
    },
    async writeSnapshot(snapshot: WalletSnapshot): Promise<void> {
      const serialized = JSON.stringify(snapshot);
      const latestKey = `${WALLET_ANALYSIS_NAMESPACE}:latest:${snapshot.address}`;
      const historyKey = `${WALLET_ANALYSIS_NAMESPACE}:history:${snapshot.address}`;
      await client.set(latestKey, serialized);
      await client.lPush(historyKey, serialized);
      await client.lTrim(historyKey, 0, 29);
    },
    async getLatestReport(): Promise<WalletAnalysisReport | null> {
      const raw = await client.get(`${WALLET_ANALYSIS_NAMESPACE}:latest_report`);
      return parseJson<WalletAnalysisReport>(raw);
    },
    async writeLatestReport(report: WalletAnalysisReport): Promise<void> {
      await client.set(`${WALLET_ANALYSIS_NAMESPACE}:latest_report`, JSON.stringify(report));
    },
    async close(): Promise<void> {
      if (client.isOpen) {
        await client.quit();
      }
    },
  };
}

function weightedScore(
  value: number | null,
  min: number,
  max: number,
  weight: number,
): number {
  if (value == null || !Number.isFinite(value)) {
    return 0;
  }
  const normalized = clamp((value - min) / (max - min), 0, 1);
  return (normalized - 0.5) * weight;
}

function normalizeBoundedScore(values: Array<number | null>): number | null {
  const filtered = values.filter((value): value is number => value != null && Number.isFinite(value));
  if (filtered.length === 0) {
    return null;
  }
  return Math.round((filtered.reduce((sum, value) => sum + value, 0) / filtered.length) * 100) / 100;
}

function scoreFromThreshold(value: number | null | undefined, low: number, high: number): number | null {
  if (value == null || !Number.isFinite(value)) {
    return null;
  }
  if (high === low) {
    return value >= high ? 1 : 0;
  }
  return clamp((value - low) / (high - low), 0, 1);
}

function providerConfidence(providers: ProviderResult[]): number {
  const total = providers.reduce((sum, provider) => {
    if (!provider.ok) {
      return sum;
    }
    if (provider.cached) {
      return sum + 0.9;
    }
    return sum + Math.max(0.5, 1 - (provider.attempts - 1) * 0.2);
  }, 0);
  return Math.round((total / providers.length) * 100) / 100;
}

function numberDelta(current: number | null, previous: number | null | undefined): number | null {
  if (current == null || previous == null || !Number.isFinite(current) || !Number.isFinite(previous)) {
    return null;
  }
  return Math.round((current - previous) * 100) / 100;
}

function parseJson<T>(raw: string | null): T | null {
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function formatPct(value: number): string {
  return `${formatNumber(value, 1)}%`;
}

function normalizeWinRatePercent(value: number | null): number | null {
  if (value == null || !Number.isFinite(value)) {
    return null;
  }
  if (value >= 0 && value <= 1) {
    return value * 100;
  }
  return value;
}

function formatUsd(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: Math.abs(value) >= 1000 ? 0 : 2,
  }).format(value);
}

function formatNumber(value: number, decimals: number): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  }).format(value);
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function parseEnvInt(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : fallback;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
