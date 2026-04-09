const BIRDEYE_BASE_URL = process.env.BIRDEYE_BASE_URL ?? "https://public-api.birdeye.so";

export type BirdeyeTokenOverview = {
  tokenAddress: string;
  symbol: string | null;
  name: string | null;
  priceUsd: number | null;
  marketCapUsd: number | null;
  liquidityUsd: number | null;
  volume24hUsd: number | null;
  holderCount: number | null;
  websiteUrl: string | null;
  telegramUrl: string | null;
  twitterUrl: string | null;
};

export type BirdeyeOhlcvCandle = {
  unixTime: number;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volumeUsd: number | null;
};

export type BirdeyeOhlcvSeries = {
  tokenAddress: string;
  interval: string;
  candles: BirdeyeOhlcvCandle[];
};

export type BirdeyeWalletPnlSummary = {
  walletAddress: string;
  duration: string;
  winRate: number | null;
  totalPnlUsd: number | null;
};

export async function getBirdeyeTokenOverview(tokenAddress: string): Promise<BirdeyeTokenOverview> {
  const apiKey = process.env.BIRDEYE_API_KEY;
  if (!apiKey) {
    throw new Error("BIRDEYE_API_KEY is not configured");
  }

  const headers = {
    accept: "application/json",
    "X-API-KEY": apiKey,
    "x-chain": "solana",
  };
  const marketDataUrl = `${BIRDEYE_BASE_URL}/defi/v3/token/market-data?address=${encodeURIComponent(tokenAddress)}`;
  const marketDataResponse = await fetch(marketDataUrl, { headers });

  if (!marketDataResponse.ok) {
    const body = await marketDataResponse.text();
    throw new Error(`Birdeye request failed: status=${marketDataResponse.status} body=${body}`);
  }

  const marketPayload = (await marketDataResponse.json()) as { data?: Record<string, unknown> };
  const marketData = marketPayload.data ?? {};
  const overviewData = await getBirdeyeOverviewData(tokenAddress, headers);
  const mergedData = {
    ...overviewData,
    ...marketData,
  };

  return {
    tokenAddress,
    symbol: pickString(mergedData, ["symbol", "tokenSymbol"]),
    name: pickString(mergedData, ["name", "tokenName"]),
    priceUsd: pickNumber(mergedData, ["price", "priceUsd", "value"]),
    marketCapUsd: pickNumber(mergedData, ["marketCap", "market_cap", "market_cap_usd", "mcap"]),
    liquidityUsd: pickNumber(mergedData, ["liquidity", "liquidityUsd", "liquidity_usd"]),
    volume24hUsd: pickNumber(mergedData, ["v24hUSD", "volume24h", "volume24hUSD", "volume24hUsd"]),
    holderCount: pickNumber(mergedData, ["holder", "holders", "holderCount"]),
    websiteUrl: pickNestedString(mergedData, [
      ["extensions", "website"],
      ["website"],
      ["links", "website"],
      ["socials", "website"],
    ]),
    telegramUrl: pickNestedString(mergedData, [
      ["extensions", "telegram"],
      ["telegram"],
      ["links", "telegram"],
      ["socials", "telegram"],
    ]),
    twitterUrl: pickNestedString(mergedData, [
      ["extensions", "twitter"],
      ["twitter"],
      ["links", "twitter"],
      ["socials", "twitter"],
    ]),
  };
}

export async function getBirdeyeWalletPnlSummary(
  walletAddress: string,
  duration = "30d",
): Promise<BirdeyeWalletPnlSummary> {
  const apiKey = process.env.BIRDEYE_API_KEY;
  if (!apiKey) {
    throw new Error("BIRDEYE_API_KEY is not configured");
  }

  const url = new URL(`${BIRDEYE_BASE_URL}/wallet/v2/pnl/summary`);
  url.searchParams.set("wallet", walletAddress);
  url.searchParams.set("duration", duration);

  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      "X-API-KEY": apiKey,
      "x-chain": "solana",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Birdeye wallet pnl summary failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as {
    data?: {
      summary?: {
        counts?: Record<string, unknown>;
        pnl?: Record<string, unknown>;
      };
    };
  };
  const summary = payload.data?.summary ?? {};

  return {
    walletAddress,
    duration,
    winRate: pickNestedNumber(summary as Record<string, unknown>, [["counts", "win_rate"]]),
    totalPnlUsd: pickNestedNumber(summary as Record<string, unknown>, [["pnl", "total_usd"]]),
  };
}

export async function getBirdeyeOhlcv(
  tokenAddress: string,
  options: {
    interval?: string;
    fromUnixTime?: number;
    toUnixTime?: number;
  } = {},
): Promise<BirdeyeOhlcvSeries> {
  const apiKey = process.env.BIRDEYE_API_KEY;
  if (!apiKey) {
    throw new Error("BIRDEYE_API_KEY is not configured");
  }

  const interval = options.interval?.trim() || "1m";
  const toUnixTime = options.toUnixTime ?? Math.floor(Date.now() / 1000);
  const fromUnixTime = options.fromUnixTime ?? Math.max(0, toUnixTime - 60 * 60);

  const url = new URL(`${BIRDEYE_BASE_URL}/defi/v3/ohlcv`);
  url.searchParams.set("address", tokenAddress);
  url.searchParams.set("type", interval);
  url.searchParams.set("time_from", String(fromUnixTime));
  url.searchParams.set("time_to", String(toUnixTime));

  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      "X-API-KEY": apiKey,
      "x-chain": "solana",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Birdeye OHLCV request failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as { data?: unknown };
  const items = extractOhlcvItems(payload.data);

  return {
    tokenAddress,
    interval,
    candles: items
      .map(normalizeOhlcvCandle)
      .filter((candle): candle is BirdeyeOhlcvCandle => candle != null)
      .sort((a, b) => a.unixTime - b.unixTime),
  };
}

async function getBirdeyeOverviewData(
  tokenAddress: string,
  headers: Record<string, string>,
): Promise<Record<string, unknown>> {
  const url = `${BIRDEYE_BASE_URL}/defi/token_overview?address=${encodeURIComponent(tokenAddress)}`;

  try {
    const response = await fetch(url, { headers });
    if (!response.ok) {
      return {};
    }

    const payload = (await response.json()) as { data?: Record<string, unknown> };
    return payload.data ?? {};
  } catch {
    return {};
  }
}

function pickString(record: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return null;
}

function pickNumber(record: Record<string, unknown>, keys: string[]): number | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }

    if (typeof value === "string") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return null;
}

function pickNestedString(
  record: Record<string, unknown>,
  paths: string[][],
): string | null {
  for (const path of paths) {
    let cursor: unknown = record;
    let found = true;
    for (const segment of path) {
      if (cursor == null || typeof cursor !== "object" || Array.isArray(cursor)) {
        found = false;
        break;
      }
      cursor = (cursor as Record<string, unknown>)[segment];
    }
    if (found && typeof cursor === "string" && cursor.trim()) {
      return cursor.trim();
    }
  }

  return null;
}

function pickNestedNumber(
  record: Record<string, unknown>,
  paths: string[][],
): number | null {
  for (const path of paths) {
    let cursor: unknown = record;
    let found = true;
    for (const segment of path) {
      if (cursor == null || typeof cursor !== "object" || Array.isArray(cursor)) {
        found = false;
        break;
      }
      cursor = (cursor as Record<string, unknown>)[segment];
    }

    if (!found) {
      continue;
    }

    if (typeof cursor === "number" && Number.isFinite(cursor)) {
      return cursor;
    }

    if (typeof cursor === "string") {
      const parsed = Number(cursor);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return null;
}

function extractOhlcvItems(data: unknown): unknown[] {
  if (Array.isArray(data)) {
    return data;
  }

  if (!data || typeof data !== "object") {
    return [];
  }

  const record = data as Record<string, unknown>;
  for (const key of ["items", "candles", "list"]) {
    const value = record[key];
    if (Array.isArray(value)) {
      return value;
    }
  }

  return [];
}

function normalizeOhlcvCandle(input: unknown): BirdeyeOhlcvCandle | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }

  const record = input as Record<string, unknown>;
  const unixTime = pickNumber(record, [
    "unixTime",
    "startUnixTime",
    "start_time",
    "time",
    "timestamp",
  ]);
  const close = pickNumber(record, ["c", "close", "closeUsd", "value"]);

  if (unixTime == null || close == null) {
    return null;
  }

  return {
    unixTime: Math.floor(unixTime),
    open: pickNumber(record, ["o", "open", "openUsd"]),
    high: pickNumber(record, ["h", "high", "highUsd"]),
    low: pickNumber(record, ["l", "low", "lowUsd"]),
    close,
    volumeUsd: pickNumber(record, ["v", "vUsd", "volume", "volumeUsd", "volume_usd"]),
  };
}
