import {
  getBirdeyeTokenOverview,
  getBirdeyeOhlcv,
  type BirdeyeOhlcvSeries,
  type BirdeyeTokenOverview,
} from "../../providers/birdeye.js";
import {
  getDexScreenerTokenData,
  type DexScreenerTokenData,
} from "../../providers/dexscreener.js";
import {
  getHeliusTokenMetadata,
  type HeliusTokenMetadata,
} from "../../providers/helius.js";
import {
  getMobulaTopHoldersAnalysis,
  type MobulaTopHoldersAnalysis,
} from "../../providers/mobula.js";
import {
  getTwitterTokenSignal,
  type TwitterTokenSignal,
} from "../../providers/twitter.js";

export type TokenResearchContext = {
  trackedWalletEntryMarketCapUsd?: number | null;
  myEntryMarketCapUsd?: number | null;
  myEntrySpentSol?: number | null;
};

export type TokenResearchResult = {
  token: string;
  symbol: string | null;
  name: string | null;
  liquidityUsd: number | null;
  volume24hUsd: number | null;
  marketCapUsd: number | null;
  fdvUsd: number | null;
  holderCount: number | null;
  topHolderPct: number | null;
  boostsActive: number | null;
  trackedWalletEntryMarketCapUsd: number | null;
  myEntryMarketCapUsd: number | null;
  myEntrySpentSol: number | null;
  socials: {
    websiteUrl: string | null;
    telegramUrl: string | null;
    x: {
      linkedAccounts: Array<{
        source: "birdeye" | "dexscreener";
        url: string;
        username: string | null;
        kind: "official_or_project" | "community" | "unknown";
        note: string | null;
        profile: {
          username: string;
          name: string;
          verified: boolean;
          followers: number | null;
          following: number | null;
          tweetCount: number | null;
          listedCount: number | null;
          createdAt: string | null;
          description: string | null;
          relevanceScore: number;
        } | null;
        recentPosts: Array<{
          id: string;
          text: string;
          createdAt: string | null;
          likeCount: number;
          repostCount: number;
          replyCount: number;
          quoteCount: number;
          author: {
            username: string;
            name: string;
            verified: boolean;
            followers: number | null;
            relevanceScore: number;
          } | null;
        }>;
      }>;
      community: {
        communityId: string;
        url: string;
        tweets: Array<{
          id: string;
          text: string;
          createdAt: string | null;
          likeCount: number;
          repostCount: number;
          replyCount: number;
          quoteCount: number;
          author: {
            username: string;
            name: string;
            verified: boolean;
            followers: number | null;
            relevanceScore: number;
          } | null;
        }>;
        moderators: Array<{
          username: string;
          name: string;
          verified: boolean;
          followers: number | null;
          createdAt: string | null;
          description: string | null;
          relevanceScore: number;
        }>;
        verifiedMembers: Array<{
          username: string;
          name: string;
          verified: boolean;
          followers: number | null;
          createdAt: string | null;
          description: string | null;
          relevanceScore: number;
        }>;
        errors: {
          tweets?: string;
          moderators?: string;
          members?: string;
        };
      } | null;
      contractSearch: TwitterSearchSummary | null;
      tickerSearch: TwitterSearchSummary | null;
      summary: {
        linkedAccountCount: number;
        communityTweetCount: number;
        communityModeratorCount: number;
        communityVerifiedMemberCount: number;
        contractPostCount: number;
        tickerPostCount: number;
        filteredOutPostCount: number;
        totalRelevantPostCount: number;
      } | null;
    } | null;
  };
  chart: {
    timeframe: string;
    lookbackMinutes: number;
    candleCount: number;
    sparkline: string | null;
    latestPriceUsd: number | null;
    latestMarketCapUsd: number | null;
    athPriceUsd: number | null;
    athMarketCapUsd: number | null;
    lowPriceUsd: number | null;
    lowMarketCapUsd: number | null;
    distanceFromAthPct: number | null;
    reboundFromLowPct: number | null;
    sampledPoints: Array<{
      unixTime: number;
      closePriceUsd: number;
      marketCapUsd: number | null;
    }>;
  } | null;
  holders: MobulaTopHoldersAnalysis | null;
  riskFlags: string[];
  action: "MONITOR" | "WATCH" | "AVOID";
  providerData: {
    dexscreener: DexScreenerTokenData | null;
    birdeye: BirdeyeTokenOverview | null;
    birdeyeOhlcv: BirdeyeOhlcvSeries | null;
    helius: HeliusTokenMetadata | null;
    mobula: MobulaTopHoldersAnalysis | null;
    twitter: TwitterTokenSignal | null;
  };
  providerErrors: Partial<
    Record<"dexscreener" | "birdeye" | "birdeyeOhlcv" | "helius" | "mobula" | "twitter", string>
  >;
};

export async function researchToken(
  tokenAddress: string,
  context: TokenResearchContext = {},
): Promise<TokenResearchResult> {
  const nowUnixTime = Math.floor(Date.now() / 1000);
  const [dexscreenerResult, birdeyeResult, birdeyeOhlcvResult, heliusResult, mobulaResult] =
    await Promise.allSettled([
      getDexScreenerTokenData(tokenAddress),
      getBirdeyeTokenOverview(tokenAddress),
      getBirdeyeOhlcv(tokenAddress, {
        interval: "1m",
        fromUnixTime: nowUnixTime - 60 * 60,
        toUnixTime: nowUnixTime,
      }),
      getHeliusTokenMetadata(tokenAddress),
      getMobulaTopHoldersAnalysis(tokenAddress),
    ]);

  const dexscreener = dexscreenerResult.status === "fulfilled" ? dexscreenerResult.value : null;
  const birdeye = birdeyeResult.status === "fulfilled" ? birdeyeResult.value : null;
  const birdeyeOhlcv =
    birdeyeOhlcvResult.status === "fulfilled" ? birdeyeOhlcvResult.value : null;
  const helius = heliusResult.status === "fulfilled" ? heliusResult.value : null;
  const mobula = mobulaResult.status === "fulfilled" ? mobulaResult.value : null;
  const twitterResult = await Promise.allSettled([
    getTwitterTokenSignal({
      tokenAddress,
      symbol: dexscreener?.symbol ?? birdeye?.symbol ?? null,
      name: dexscreener?.name ?? birdeye?.name ?? null,
      linkedAccounts: collectTwitterLinks(birdeye, dexscreener),
    }),
  ]);
  const twitter = twitterResult[0].status === "fulfilled" ? twitterResult[0].value : null;

  const liquidityUsd = firstNumber(dexscreener?.liquidityUsd, birdeye?.liquidityUsd);
  const volume24hUsd = firstNumber(dexscreener?.volume24hUsd, birdeye?.volume24hUsd);
  const marketCapUsd = firstNumber(dexscreener?.marketCapUsd, birdeye?.marketCapUsd);
  const fdvUsd = firstNumber(dexscreener?.fdvUsd);
  const holderCount = firstNumber(birdeye?.holderCount);
  const boostsActive = firstNumber(dexscreener?.boostsActive);
  const trackedWalletEntryMarketCapUsd = normalizeNullableNumber(context.trackedWalletEntryMarketCapUsd);
  const myEntryMarketCapUsd = normalizeNullableNumber(context.myEntryMarketCapUsd);
  const myEntrySpentSol = normalizeNullableNumber(context.myEntrySpentSol);

  const riskFlags = computeRiskFlags({
    liquidityUsd,
    volume24hUsd,
    marketCapUsd,
    boostsActive,
    trackedWalletEntryMarketCapUsd,
    myEntryMarketCapUsd,
    twitterTotalPostCount: twitter?.summary.totalRelevantPostCount ?? 0,
    twitterLinkedAccountCount: twitter?.summary.linkedAccountCount ?? 0,
    hasFreezeAuthority: Boolean(helius?.freezeAuthority),
  });
  const chart = buildChartSummary(
    birdeyeOhlcv,
    firstNumber(dexscreener?.priceUsd, birdeye?.priceUsd),
    marketCapUsd,
  );

  return {
    token: tokenAddress,
    symbol: dexscreener?.symbol ?? birdeye?.symbol ?? null,
    name: dexscreener?.name ?? birdeye?.name ?? null,
    liquidityUsd,
    volume24hUsd,
    marketCapUsd,
    fdvUsd,
    holderCount,
    topHolderPct: mobula?.topHolderPct ?? null,
    boostsActive,
    trackedWalletEntryMarketCapUsd,
    myEntryMarketCapUsd,
    myEntrySpentSol,
    socials: {
      websiteUrl: birdeye?.websiteUrl ?? dexscreener?.websites[0]?.url ?? null,
      telegramUrl: birdeye?.telegramUrl ?? null,
      x: twitter ? mapTwitterSignal(twitter) : null,
    },
    chart,
    holders: mobula,
    riskFlags,
    action: chooseAction(riskFlags),
    providerData: {
      dexscreener,
      birdeye,
      birdeyeOhlcv,
      helius,
      mobula,
      twitter,
    },
    providerErrors: {
      dexscreener: getRejectedMessage(dexscreenerResult),
      birdeye: getRejectedMessage(birdeyeResult),
      birdeyeOhlcv: getRejectedMessage(birdeyeOhlcvResult),
      helius: getRejectedMessage(heliusResult),
      mobula: getRejectedMessage(mobulaResult),
      twitter: getRejectedMessage(twitterResult[0]),
    },
  };
}

function computeRiskFlags(input: {
  liquidityUsd: number | null;
  volume24hUsd: number | null;
  marketCapUsd: number | null;
  boostsActive: number | null;
  trackedWalletEntryMarketCapUsd: number | null;
  myEntryMarketCapUsd: number | null;
  twitterTotalPostCount: number;
  twitterLinkedAccountCount: number;
  hasFreezeAuthority: boolean;
}): string[] {
  const flags: string[] = [];

  if (input.liquidityUsd != null && input.liquidityUsd < 20_000) {
    flags.push("low liquidity");
  }

  if (input.volume24hUsd != null && input.volume24hUsd < 50_000) {
    flags.push("low 24h volume");
  }

  if (input.marketCapUsd != null && input.trackedWalletEntryMarketCapUsd != null) {
    const multiple = input.marketCapUsd / input.trackedWalletEntryMarketCapUsd;
    if (multiple >= 3) {
      flags.push("3x above tracked wallet entry mcap");
    }
  }

  if (input.marketCapUsd != null && input.myEntryMarketCapUsd != null) {
    const multiple = input.marketCapUsd / input.myEntryMarketCapUsd;
    if (multiple >= 2) {
      flags.push("2x above my entry mcap");
    }
  }

  if ((input.boostsActive ?? 0) > 0) {
    flags.push("dex paid boosts active");
  }

  if (input.twitterLinkedAccountCount === 0 && input.twitterTotalPostCount < 3) {
    flags.push("weak social presence");
  }

  if (input.hasFreezeAuthority) {
    flags.push("freeze authority present");
  }

  return flags;
}

function chooseAction(riskFlags: string[]): "MONITOR" | "WATCH" | "AVOID" {
  if (riskFlags.includes("freeze authority present")) {
    return "AVOID";
  }
  if (riskFlags.length >= 3) {
    return "AVOID";
  }
  if (riskFlags.length >= 1) {
    return "WATCH";
  }
  return "MONITOR";
}

function collectTwitterLinks(
  birdeye: BirdeyeTokenOverview | null,
  dexscreener: DexScreenerTokenData | null,
): Array<{ source: "birdeye" | "dexscreener"; url: string }> {
  const links: Array<{ source: "birdeye" | "dexscreener"; url: string }> = [];

  if (birdeye?.twitterUrl) {
    links.push({ source: "birdeye", url: birdeye.twitterUrl });
  }

  for (const social of dexscreener?.socials ?? []) {
    if ((social.type ?? "").toLowerCase() === "twitter") {
      links.push({ source: "dexscreener", url: social.url });
    }
  }

  const seen = new Set<string>();
  return links.filter((entry) => {
    const key = entry.url.trim().toLowerCase();
    if (!key || seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

type TwitterSearchSummary = {
  query: string;
  posts: Array<{
    id: string;
    text: string;
    createdAt: string | null;
    likeCount: number;
    repostCount: number;
    replyCount: number;
    quoteCount: number;
    author: {
      username: string;
      name: string;
      verified: boolean;
      followers: number | null;
      relevanceScore: number;
    } | null;
  }>;
};

function mapTwitterSignal(twitter: TwitterTokenSignal): TokenResearchResult["socials"]["x"] {
  return {
    linkedAccounts: twitter.linkedAccounts.map((account) => ({
      source: account.source,
      url: account.url,
      username: account.username,
      kind: account.kind,
      note: account.note,
      profile: account.profile,
      recentPosts: account.recentPosts,
    })),
    community: twitter.community
      ? {
          communityId: twitter.community.communityId,
          url: twitter.community.url,
          tweets: twitter.community.tweets,
          moderators: twitter.community.moderators,
          verifiedMembers: twitter.community.verifiedMembers,
          errors: twitter.community.errors,
        }
      : null,
    contractSearch: twitter.contractSearch,
    tickerSearch: twitter.tickerSearch,
    summary: twitter.summary,
  };
}

function firstNumber(...values: Array<number | null | undefined>): number | null {
  for (const value of values) {
    if (value != null && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function normalizeNullableNumber(value: number | null | undefined): number | null {
  return value != null && Number.isFinite(value) ? value : null;
}

function getRejectedMessage(result: PromiseSettledResult<unknown>): string | undefined {
  if (result.status !== "rejected") {
    return undefined;
  }
  return result.reason instanceof Error ? result.reason.message : String(result.reason);
}

function buildChartSummary(
  series: BirdeyeOhlcvSeries | null,
  currentPriceUsd: number | null,
  currentMarketCapUsd: number | null,
): TokenResearchResult["chart"] {
  const candles = (series?.candles ?? []).filter((candle) => candle.close != null);
  if (candles.length === 0) {
    return null;
  }

  const closes = candles
    .map((candle) => candle.close)
    .filter((value): value is number => value != null && Number.isFinite(value));
  if (closes.length === 0) {
    return null;
  }

  const latestPriceUsd = closes[closes.length - 1] ?? null;
  const athPriceUsd = Math.max(...closes);
  const lowPriceUsd = Math.min(...closes);
  const distanceFromAthPct =
    latestPriceUsd != null && athPriceUsd > 0
      ? ((latestPriceUsd / athPriceUsd) - 1) * 100
      : null;
  const reboundFromLowPct =
    latestPriceUsd != null && lowPriceUsd > 0
      ? ((latestPriceUsd / lowPriceUsd) - 1) * 100
      : null;

  const marketCapReferencePrice = firstNumber(currentPriceUsd, latestPriceUsd);
  const latestMarketCapUsd = scaleMarketCap(currentMarketCapUsd, latestPriceUsd, marketCapReferencePrice);
  const athMarketCapUsd = scaleMarketCap(currentMarketCapUsd, athPriceUsd, marketCapReferencePrice);
  const lowMarketCapUsd = scaleMarketCap(currentMarketCapUsd, lowPriceUsd, marketCapReferencePrice);

  return {
    timeframe: series?.interval ?? "1m",
    lookbackMinutes: 60,
    candleCount: candles.length,
    sparkline: buildSparkline(closes),
    latestPriceUsd,
    latestMarketCapUsd,
    athPriceUsd,
    athMarketCapUsd,
    lowPriceUsd,
    lowMarketCapUsd,
    distanceFromAthPct,
    reboundFromLowPct,
    sampledPoints: sampleCandles(candles, 12).map((candle) => ({
      unixTime: candle.unixTime,
      closePriceUsd: candle.close ?? 0,
      marketCapUsd: scaleMarketCap(
        currentMarketCapUsd,
        candle.close ?? null,
        marketCapReferencePrice,
      ),
    })),
  };
}

function scaleMarketCap(
  currentMarketCapUsd: number | null,
  priceUsd: number | null,
  referencePriceUsd: number | null,
): number | null {
  if (
    currentMarketCapUsd == null ||
    priceUsd == null ||
    referencePriceUsd == null ||
    !Number.isFinite(currentMarketCapUsd) ||
    !Number.isFinite(priceUsd) ||
    !Number.isFinite(referencePriceUsd) ||
    referencePriceUsd <= 0
  ) {
    return null;
  }

  return currentMarketCapUsd * (priceUsd / referencePriceUsd);
}

function buildSparkline(values: number[]): string | null {
  if (values.length === 0) {
    return null;
  }

  const blocks = "▁▂▃▄▅▆▇█";
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return null;
  }
  if (max <= min) {
    return blocks[0].repeat(Math.min(values.length, 32));
  }

  const sampled = sampleNumbers(values, 32);
  return sampled
    .map((value) => {
      const normalized = (value - min) / (max - min);
      const idx = Math.max(0, Math.min(blocks.length - 1, Math.round(normalized * (blocks.length - 1))));
      return blocks[idx];
    })
    .join("");
}

function sampleCandles(
  candles: BirdeyeOhlcvSeries["candles"],
  maxPoints: number,
): BirdeyeOhlcvSeries["candles"] {
  if (candles.length <= maxPoints) {
    return candles;
  }

  const result: BirdeyeOhlcvSeries["candles"] = [];
  const step = (candles.length - 1) / (maxPoints - 1);
  for (let i = 0; i < maxPoints; i += 1) {
    const index = Math.min(candles.length - 1, Math.round(i * step));
    result.push(candles[index]);
  }
  return result;
}

function sampleNumbers(values: number[], maxPoints: number): number[] {
  if (values.length <= maxPoints) {
    return values;
  }

  const result: number[] = [];
  const step = (values.length - 1) / (maxPoints - 1);
  for (let i = 0; i < maxPoints; i += 1) {
    const index = Math.min(values.length - 1, Math.round(i * step));
    result.push(values[index]);
  }
  return result;
}
