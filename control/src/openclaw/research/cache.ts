import type { TokenResearchContext, TokenResearchResult } from "./tokenResearch.js";

type CacheEntry = {
  expiresAt: number;
  result: TokenResearchResult;
};

const cache = new Map<string, CacheEntry>();

export function getCachedResearch(
  tokenAddress: string,
  context: TokenResearchContext,
): TokenResearchResult | null {
  const entry = cache.get(buildResearchCacheKey(tokenAddress, context));
  if (!entry) {
    return null;
  }
  if (entry.expiresAt <= Date.now()) {
    cache.delete(buildResearchCacheKey(tokenAddress, context));
    return null;
  }
  return entry.result;
}

export function setCachedResearch(
  tokenAddress: string,
  context: TokenResearchContext,
  result: TokenResearchResult,
  ttlMs: number,
): void {
  if (ttlMs <= 0) {
    return;
  }
  cache.set(buildResearchCacheKey(tokenAddress, context), {
    expiresAt: Date.now() + ttlMs,
    result,
  });
}

function buildResearchCacheKey(
  tokenAddress: string,
  context: TokenResearchContext,
): string {
  return JSON.stringify({
    tokenAddress,
    trackedWalletEntryMarketCapUsd: normalize(context.trackedWalletEntryMarketCapUsd),
    myEntryMarketCapUsd: normalize(context.myEntryMarketCapUsd),
    myEntrySpentSol: normalize(context.myEntrySpentSol),
  });
}

function normalize(value: number | null | undefined): number | null {
  return Number.isFinite(value) ? Number(value) : null;
}
