const DEXSCREENER_BASE_URL = process.env.DEXSCREENER_BASE_URL ?? "https://api.dexscreener.com";

export type DexScreenerTokenData = {
  tokenAddress: string;
  symbol: string | null;
  name: string | null;
  chainId: string | null;
  dexId: string | null;
  pairAddress: string | null;
  priceUsd: number | null;
  liquidityUsd: number | null;
  volume24hUsd: number | null;
  marketCapUsd: number | null;
  fdvUsd: number | null;
  boostsActive: number | null;
  pairCreatedAt: number | null;
  rawPairCount: number;
  websites: Array<{
    label: string | null;
    url: string;
  }>;
  socials: Array<{
    type: string | null;
    url: string;
  }>;
};

type DexScreenerPair = {
  chainId?: string;
  dexId?: string;
  pairAddress?: string;
  pairCreatedAt?: number;
  priceUsd?: string;
  fdv?: number;
  marketCap?: number;
  liquidity?: {
    usd?: number;
  };
  volume?: {
    h24?: number;
  };
  boosts?: {
    active?: number;
  };
  baseToken?: {
    address?: string;
    symbol?: string;
    name?: string;
  };
  quoteToken?: {
    address?: string;
  };
  info?: {
    websites?: Array<{
      label?: string;
      url?: string;
    }>;
    socials?: Array<{
      type?: string;
      url?: string;
    }>;
  };
};

type DexScreenerResponse = {
  pairs?: DexScreenerPair[];
};

export async function getDexScreenerTokenData(tokenAddress: string): Promise<DexScreenerTokenData> {
  const url = `${DEXSCREENER_BASE_URL}/latest/dex/tokens/${tokenAddress}`;
  const response = await fetch(url, {
    headers: {
      accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`DexScreener request failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as DexScreenerResponse;
  const pairs = payload.pairs ?? [];
  const primaryPair = choosePrimaryPair(tokenAddress, pairs);

  return {
    tokenAddress,
    symbol: primaryPair?.baseToken?.symbol ?? null,
    name: primaryPair?.baseToken?.name ?? null,
    chainId: primaryPair?.chainId ?? null,
    dexId: primaryPair?.dexId ?? null,
    pairAddress: primaryPair?.pairAddress ?? null,
    priceUsd: parseMaybeNumber(primaryPair?.priceUsd),
    liquidityUsd: primaryPair?.liquidity?.usd ?? null,
    volume24hUsd: primaryPair?.volume?.h24 ?? null,
    marketCapUsd: primaryPair?.marketCap ?? null,
    fdvUsd: primaryPair?.fdv ?? null,
    boostsActive: primaryPair?.boosts?.active ?? null,
    pairCreatedAt: primaryPair?.pairCreatedAt ?? null,
    rawPairCount: pairs.length,
    websites: (primaryPair?.info?.websites ?? [])
      .filter((entry) => Boolean(entry?.url))
      .map((entry) => ({
        label: entry.label?.trim() || null,
        url: entry.url!.trim(),
      })),
    socials: (primaryPair?.info?.socials ?? [])
      .filter((entry) => Boolean(entry?.url))
      .map((entry) => ({
        type: entry.type?.trim() || null,
        url: entry.url!.trim(),
      })),
  };
}

function choosePrimaryPair(tokenAddress: string, pairs: DexScreenerPair[]): DexScreenerPair | null {
  if (pairs.length === 0) {
    return null;
  }

  const normalizedToken = tokenAddress.toLowerCase();
  return [...pairs]
    .filter((pair) => pair.baseToken?.address?.toLowerCase() === normalizedToken)
    .sort((left, right) => scorePair(right) - scorePair(left))[0] ?? null;
}

function scorePair(pair: DexScreenerPair): number {
  return (pair.liquidity?.usd ?? 0) * 10 + (pair.volume?.h24 ?? 0) + (pair.marketCap ?? 0) / 100;
}

function parseMaybeNumber(value: string | undefined): number | null {
  if (!value) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
