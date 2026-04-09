import { getBirdeyeWalletPnlSummary } from "./birdeye.js";

const MOBULA_API_BASE_URL = process.env.MOBULA_API_BASE_URL ?? "https://api.mobula.io";
const SOL_NATIVE_PLACEHOLDER_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const SOL_MINT = "So11111111111111111111111111111111111111112";

export type MobulaWalletPortfolioSummary = {
  walletAddress: string;
  totalWalletBalanceUsd: number | null;
  nativeSolBalance: number | null;
  nativeSolBalanceUsd: number | null;
  assetCount: number | null;
};

export type MobulaTopHolder = {
  walletAddress: string;
  tokenAmount: number | null;
  tokenAmountRaw: string | null;
  tokenAmountUsd: number | null;
  percentageOfTotalSupply: number | null;
  pnlUsd: number | null;
  realizedPnlUsd: number | null;
  unrealizedPnlUsd: number | null;
  totalPnlUsd: number | null;
  buys: number | null;
  sells: number | null;
  volumeBuyToken: number | null;
  volumeSellToken: number | null;
  volumeBuyUsd: number | null;
  volumeSellUsd: number | null;
  avgBuyPriceUsd: number | null;
  avgSellPriceUsd: number | null;
  walletFundAt: string | null;
  lastActivityAt: string | null;
  firstTradeAt: string | null;
  lastTradeAt: string | null;
  labels: string[];
  walletMetadata: {
    entityName: string | null;
    entityLabels: string[];
    entityType: string | null;
    entityDescription: string | null;
    entityTwitter: string | null;
    entityWebsite: string | null;
  } | null;
  platform: {
    id: string | null;
    name: string | null;
    logo: string | null;
  } | null;
  fundingInfo: {
    from: string | null;
    date: string | null;
    formattedAmount: number | null;
    currencySymbol: string | null;
  } | null;
};

export type MobulaTopHolderAnalysisEntry = {
  walletAddress: string;
  tokenAmount: number | null;
  tokenAmountRaw: string | null;
  tokenAmountUsd: number | null;
  percentageOfTotalSupply: number | null;
  tokenPnl: {
    pnlUsd: number | null;
    realizedPnlUsd: number | null;
    unrealizedPnlUsd: number | null;
    totalPnlUsd: number | null;
  };
  activity: {
    buys: number | null;
    sells: number | null;
    volumeBuyUsd: number | null;
    volumeSellUsd: number | null;
    avgBuyPriceUsd: number | null;
    avgSellPriceUsd: number | null;
    walletFundAt: string | null;
    lastActivityAt: string | null;
    firstTradeAt: string | null;
    lastTradeAt: string | null;
  };
  labels: string[];
  walletMetadata: MobulaTopHolder["walletMetadata"];
  platform: MobulaTopHolder["platform"];
  fundingInfo: MobulaTopHolder["fundingInfo"];
  excludedFromWalletQualityAnalysis: boolean;
  exclusionReason: string | null;
  wallet: MobulaWalletPortfolioSummary | null;
  walletQualityError: string | null;
  walletQuality: {
    nativeSolThreshold: number;
    hasMoreThanThresholdSol: boolean | null;
    winRate30d: number | null;
    monthlyPnlUsd: number | null;
  };
};

export type MobulaTopHoldersAnalysis = {
  tokenAddress: string;
  requestedTopHolders: number;
  totalHolderCount: number | null;
  analyzedHolderCount: number;
  excludedHolderCount: number;
  minNativeSolThreshold: number;
  topHolderPct: number | null;
  summary: {
    holdersWithMoreThanThresholdSol: number;
    cumulativeSupplyPctAnalyzed: number | null;
    cumulativeSupplyPctHeldByMoreThanThresholdSolWallets: number | null;
  };
  holders: MobulaTopHolderAnalysisEntry[];
  holdersWithMoreThanThresholdSol: MobulaTopHolderAnalysisEntry[];
};

type MobulaHolderPositionsResponse = {
  data?: Array<Record<string, unknown>>;
  totalCount?: number;
};

type MobulaWalletPortfolioResponse = {
  data?: {
    total_wallet_balance?: number;
    assets?: Array<Record<string, unknown>>;
    balances_length?: number;
  };
};

export async function getMobulaTopHoldersAnalysis(
  tokenAddress: string,
  input: {
    limit?: number;
    minNativeSolThreshold?: number;
  } = {},
): Promise<MobulaTopHoldersAnalysis> {
  const requestedTopHolders = clampInt(input.limit ?? 20, 1, 20);
  const minNativeSolThreshold = pickNumber(input.minNativeSolThreshold) ?? 10;
  const topHoldersResponse = await getMobulaTokenHolderPositions(tokenAddress, requestedTopHolders);

  const holders = await mapWithConcurrency(
    topHoldersResponse.holders,
    4,
    async (holder): Promise<MobulaTopHolderAnalysisEntry> => {
      const exclusionReason = getHolderExclusionReason(holder);
      if (exclusionReason) {
        return {
          walletAddress: holder.walletAddress,
          tokenAmount: holder.tokenAmount,
          tokenAmountRaw: holder.tokenAmountRaw,
          tokenAmountUsd: holder.tokenAmountUsd,
          percentageOfTotalSupply: holder.percentageOfTotalSupply,
          tokenPnl: {
            pnlUsd: holder.pnlUsd,
            realizedPnlUsd: holder.realizedPnlUsd,
            unrealizedPnlUsd: holder.unrealizedPnlUsd,
            totalPnlUsd: holder.totalPnlUsd,
          },
          activity: {
            buys: holder.buys,
            sells: holder.sells,
            volumeBuyUsd: holder.volumeBuyUsd,
            volumeSellUsd: holder.volumeSellUsd,
            avgBuyPriceUsd: holder.avgBuyPriceUsd,
            avgSellPriceUsd: holder.avgSellPriceUsd,
            walletFundAt: holder.walletFundAt,
            lastActivityAt: holder.lastActivityAt,
            firstTradeAt: holder.firstTradeAt,
            lastTradeAt: holder.lastTradeAt,
          },
          labels: holder.labels,
          walletMetadata: holder.walletMetadata,
          platform: holder.platform,
          fundingInfo: holder.fundingInfo,
          excludedFromWalletQualityAnalysis: true,
          exclusionReason,
          wallet: null,
          walletQualityError: null,
          walletQuality: {
            nativeSolThreshold: minNativeSolThreshold,
            hasMoreThanThresholdSol: null,
            winRate30d: null,
            monthlyPnlUsd: null,
          },
        };
      }

      try {
        const wallet = await getMobulaWalletPortfolioSummary(holder.walletAddress);
        let walletPnlSummary: Awaited<ReturnType<typeof getBirdeyeWalletPnlSummary>> | null = null;
        let walletQualityError: string | null = null;

        try {
          walletPnlSummary = await getBirdeyeWalletPnlSummary(holder.walletAddress);
        } catch (error) {
          walletQualityError = error instanceof Error ? error.message : "wallet pnl summary lookup failed";
        }

        return {
          walletAddress: holder.walletAddress,
          tokenAmount: holder.tokenAmount,
          tokenAmountRaw: holder.tokenAmountRaw,
          tokenAmountUsd: holder.tokenAmountUsd,
          percentageOfTotalSupply: holder.percentageOfTotalSupply,
          tokenPnl: {
            pnlUsd: holder.pnlUsd,
            realizedPnlUsd: holder.realizedPnlUsd,
            unrealizedPnlUsd: holder.unrealizedPnlUsd,
            totalPnlUsd: holder.totalPnlUsd,
          },
          activity: {
            buys: holder.buys,
            sells: holder.sells,
            volumeBuyUsd: holder.volumeBuyUsd,
            volumeSellUsd: holder.volumeSellUsd,
            avgBuyPriceUsd: holder.avgBuyPriceUsd,
            avgSellPriceUsd: holder.avgSellPriceUsd,
            walletFundAt: holder.walletFundAt,
            lastActivityAt: holder.lastActivityAt,
            firstTradeAt: holder.firstTradeAt,
            lastTradeAt: holder.lastTradeAt,
          },
          labels: holder.labels,
          walletMetadata: holder.walletMetadata,
          platform: holder.platform,
          fundingInfo: holder.fundingInfo,
          excludedFromWalletQualityAnalysis: false,
          exclusionReason: null,
          wallet,
          walletQualityError,
          walletQuality: {
            nativeSolThreshold: minNativeSolThreshold,
            hasMoreThanThresholdSol:
              wallet.nativeSolBalance != null ? wallet.nativeSolBalance > minNativeSolThreshold : null,
            winRate30d: walletPnlSummary?.winRate ?? null,
            monthlyPnlUsd: walletPnlSummary?.totalPnlUsd ?? null,
          },
        };
      } catch (error) {
        return {
          walletAddress: holder.walletAddress,
          tokenAmount: holder.tokenAmount,
          tokenAmountRaw: holder.tokenAmountRaw,
          tokenAmountUsd: holder.tokenAmountUsd,
          percentageOfTotalSupply: holder.percentageOfTotalSupply,
          tokenPnl: {
            pnlUsd: holder.pnlUsd,
            realizedPnlUsd: holder.realizedPnlUsd,
            unrealizedPnlUsd: holder.unrealizedPnlUsd,
            totalPnlUsd: holder.totalPnlUsd,
          },
          activity: {
            buys: holder.buys,
            sells: holder.sells,
            volumeBuyUsd: holder.volumeBuyUsd,
            volumeSellUsd: holder.volumeSellUsd,
            avgBuyPriceUsd: holder.avgBuyPriceUsd,
            avgSellPriceUsd: holder.avgSellPriceUsd,
            walletFundAt: holder.walletFundAt,
            lastActivityAt: holder.lastActivityAt,
            firstTradeAt: holder.firstTradeAt,
            lastTradeAt: holder.lastTradeAt,
          },
          labels: holder.labels,
          walletMetadata: holder.walletMetadata,
          platform: holder.platform,
          fundingInfo: holder.fundingInfo,
          excludedFromWalletQualityAnalysis: false,
          exclusionReason: null,
          wallet: null,
          walletQualityError: error instanceof Error ? error.message : "wallet quality lookup failed",
          walletQuality: {
            nativeSolThreshold: minNativeSolThreshold,
            hasMoreThanThresholdSol: null,
            winRate30d: null,
            monthlyPnlUsd: null,
          },
        };
      }
    },
  );

  const analyzedHolders = holders.filter((holder) => !holder.excludedFromWalletQualityAnalysis);
  const holdersWithMoreThanThresholdSol = analyzedHolders.filter(
    (holder) => holder.walletQuality.hasMoreThanThresholdSol === true,
  );
  const cumulativeSupplyPctAnalyzed = sumNullableNumbers(
    analyzedHolders.map((holder) => holder.percentageOfTotalSupply),
  );
  const cumulativeSupplyPctHeldByMoreThanThresholdSolWallets = sumNullableNumbers(
    holdersWithMoreThanThresholdSol.map((holder) => holder.percentageOfTotalSupply),
  );

  return {
    tokenAddress,
    requestedTopHolders,
    totalHolderCount: topHoldersResponse.totalCount,
    analyzedHolderCount: analyzedHolders.length,
    excludedHolderCount: holders.length - analyzedHolders.length,
    minNativeSolThreshold,
    topHolderPct: holders[0]?.percentageOfTotalSupply ?? null,
    summary: {
      holdersWithMoreThanThresholdSol: holdersWithMoreThanThresholdSol.length,
      cumulativeSupplyPctAnalyzed,
      cumulativeSupplyPctHeldByMoreThanThresholdSolWallets,
    },
    holders,
    holdersWithMoreThanThresholdSol,
  };
}

async function getMobulaTokenHolderPositions(
  tokenAddress: string,
  limit: number,
): Promise<{ holders: MobulaTopHolder[]; totalCount: number | null }> {
  const apiKey = getMobulaApiKey();
  const url = new URL(`${MOBULA_API_BASE_URL}/api/2/token/holder-positions`);
  url.searchParams.set("blockchain", "solana");
  url.searchParams.set("address", tokenAddress);
  url.searchParams.set("limit", String(limit));

  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      Authorization: apiKey,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Mobula token holder positions failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as MobulaHolderPositionsResponse;
  return {
    holders: (payload.data ?? []).map(parseMobulaHolderPosition),
    totalCount: normalizeNullableNumber(payload.totalCount),
  };
}

export async function getMobulaWalletPortfolioSummary(
  walletAddress: string,
): Promise<MobulaWalletPortfolioSummary> {
  const apiKey = getMobulaApiKey();
  const url = new URL(`${MOBULA_API_BASE_URL}/api/1/wallet/portfolio`);
  url.searchParams.set("wallet", walletAddress);
  url.searchParams.set("blockchains", "solana:solana");
  url.searchParams.set("pnl", "true");

  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      Authorization: apiKey,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Mobula wallet portfolio failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as MobulaWalletPortfolioResponse;
  const data = payload.data ?? {};
  const assets = Array.isArray(data.assets) ? data.assets : [];
  const nativeSolAsset = assets.find(isNativeSolAsset);

  return {
    walletAddress,
    totalWalletBalanceUsd: pickNumber(data.total_wallet_balance),
    nativeSolBalance: nativeSolAsset ? pickNumber(nativeSolAsset.token_balance) : null,
    nativeSolBalanceUsd: nativeSolAsset ? pickNumber(nativeSolAsset.estimated_balance) : null,
    assetCount: pickNumber(data.balances_length),
  };
}

function parseMobulaHolderPosition(record: Record<string, unknown>): MobulaTopHolder {
  return {
    walletAddress: pickString(record.walletAddress) ?? "",
    tokenAmount: pickNumber(record.tokenAmount),
    tokenAmountRaw: pickString(record.tokenAmountRaw),
    tokenAmountUsd: pickNumber(record.tokenAmountUSD),
    percentageOfTotalSupply: pickNumber(record.percentageOfTotalSupply),
    pnlUsd: pickNumber(record.pnlUSD),
    realizedPnlUsd: pickNumber(record.realizedPnlUSD),
    unrealizedPnlUsd: pickNumber(record.unrealizedPnlUSD),
    totalPnlUsd: pickNumber(record.totalPnlUSD),
    buys: pickNumber(record.buys),
    sells: pickNumber(record.sells),
    volumeBuyToken: pickNumber(record.volumeBuyToken),
    volumeSellToken: pickNumber(record.volumeSellToken),
    volumeBuyUsd: pickNumber(record.volumeBuyUSD),
    volumeSellUsd: pickNumber(record.volumeSellUSD),
    avgBuyPriceUsd: pickNumber(record.avgBuyPriceUSD),
    avgSellPriceUsd: pickNumber(record.avgSellPriceUSD),
    walletFundAt: pickString(record.walletFundAt),
    lastActivityAt: pickString(record.lastActivityAt),
    firstTradeAt: pickString(record.firstTradeAt),
    lastTradeAt: pickString(record.lastTradeAt),
    labels: pickStringArray(record.labels),
    walletMetadata: pickRecord(record.walletMetadata)
      ? {
          entityName: pickString(pickRecord(record.walletMetadata)?.entityName),
          entityLabels: pickStringArray(pickRecord(record.walletMetadata)?.entityLabels),
          entityType: pickString(pickRecord(record.walletMetadata)?.entityType),
          entityDescription: pickString(pickRecord(record.walletMetadata)?.entityDescription),
          entityTwitter: pickString(pickRecord(record.walletMetadata)?.entityTwitter),
          entityWebsite: pickString(pickRecord(record.walletMetadata)?.entityWebsite),
        }
      : null,
    platform: pickRecord(record.platform)
      ? {
          id: pickString(pickRecord(record.platform)?.id),
          name: pickString(pickRecord(record.platform)?.name),
          logo: pickString(pickRecord(record.platform)?.logo),
        }
      : null,
    fundingInfo: pickRecord(record.fundingInfo)
      ? {
          from: pickString(pickRecord(record.fundingInfo)?.from),
          date: pickString(pickRecord(record.fundingInfo)?.date),
          formattedAmount: pickNumber(pickRecord(record.fundingInfo)?.formattedAmount),
          currencySymbol: pickString(pickRecord(pickRecord(record.fundingInfo)?.currency)?.symbol),
        }
      : null,
  };
}

function getHolderExclusionReason(holder: MobulaTopHolder): string | null {
  const haystacks = [
    ...holder.labels,
    holder.walletMetadata?.entityType ?? "",
    ...(holder.walletMetadata?.entityLabels ?? []),
  ]
    .map((value) => value.toLowerCase())
    .filter(Boolean);

  const patterns = [
    { pattern: "liquiditypool", reason: "liquidity pool wallet" },
    { pattern: "liquidity pool", reason: "liquidity pool wallet" },
    { pattern: "market maker", reason: "market maker wallet" },
    { pattern: "exchange", reason: "exchange wallet" },
    { pattern: "bridge", reason: "bridge wallet" },
    { pattern: "protocol", reason: "protocol-owned wallet" },
    { pattern: "contract", reason: "contract-owned wallet" },
    { pattern: "treasury", reason: "treasury wallet" },
  ];

  for (const candidate of patterns) {
    if (haystacks.some((value) => value.includes(candidate.pattern))) {
      return candidate.reason;
    }
  }

  return null;
}

function isNativeSolAsset(record: Record<string, unknown>): boolean {
  const asset = pickRecord(record.asset);
  const contracts = pickUnknownArray(asset?.contracts);
  const hasSolContract = contracts.some((contract) => {
    const value = pickString(contract);
    return value === SOL_NATIVE_PLACEHOLDER_ADDRESS || value === SOL_MINT;
  });

  if (hasSolContract) {
    return true;
  }

  return pickString(asset?.symbol) === "SOL";
}

function getMobulaApiKey(): string {
  const apiKey = process.env.MOBULA_API_KEY;
  if (!apiKey) {
    throw new Error("MOBULA_API_KEY is not configured");
  }

  return apiKey;
}

function pickRecord(value: unknown): Record<string, unknown> | null {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function pickUnknownArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function pickString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function pickStringArray(value: unknown): string[] {
  return pickUnknownArray(value)
    .map((entry) => pickString(entry))
    .filter((entry): entry is string => entry != null);
}

function pickNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function normalizeNullableNumber(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function clampInt(value: number, min: number, max: number): number {
  return Math.min(Math.max(Math.trunc(value), min), max);
}

function sumNullableNumbers(values: Array<number | null | undefined>): number | null {
  let found = false;
  let total = 0;

  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      found = true;
      total += value;
    }
  }

  return found ? total : null;
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  mapFn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let currentIndex = 0;

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (currentIndex < items.length) {
      const itemIndex = currentIndex;
      currentIndex += 1;
      results[itemIndex] = await mapFn(items[itemIndex], itemIndex);
    }
  });

  await Promise.all(workers);
  return results;
}
