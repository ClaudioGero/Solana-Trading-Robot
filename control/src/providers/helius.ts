const HELIUS_API_BASE_URL = process.env.HELIUS_API_BASE_URL ?? "https://mainnet.helius-rpc.com";

export type HeliusTokenMetadata = {
  tokenAddress: string;
  decimals: number | null;
  supply: string | null;
  mintAuthority: string | null;
  freezeAuthority: string | null;
};

export async function getHeliusTokenMetadata(tokenAddress: string): Promise<HeliusTokenMetadata> {
  const apiKey = process.env.HELIUS_API_KEY;
  if (!apiKey) {
    throw new Error("HELIUS_API_KEY is not configured");
  }

  const response = await fetch(`${HELIUS_API_BASE_URL}/?api-key=${encodeURIComponent(apiKey)}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: "token-metadata",
      method: "getAsset",
      params: {
        id: tokenAddress,
      },
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Helius request failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as {
    result?: {
      token_info?: {
        decimals?: number;
        supply?: number | string;
        mint_authority?: string;
        freeze_authority?: string;
      };
    };
    error?: {
      message?: string;
    };
  };

  if (payload.error?.message) {
    throw new Error(`Helius RPC error: ${payload.error.message}`);
  }

  const tokenInfo = payload.result?.token_info;
  return {
    tokenAddress,
    decimals: tokenInfo?.decimals ?? null,
    supply: tokenInfo?.supply != null ? String(tokenInfo.supply) : null,
    mintAuthority: tokenInfo?.mint_authority ?? null,
    freezeAuthority: tokenInfo?.freeze_authority ?? null,
  };
}
