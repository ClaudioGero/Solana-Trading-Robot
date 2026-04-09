import dotenv from "dotenv";
import { analyzeAlphaWallets } from "./walletAnalysis.js";

dotenv.config();

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const report = await analyzeAlphaWallets(args);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});

function parseArgs(argv: string[]) {
  const result: {
    includeDisabled?: boolean;
    forceRefresh?: boolean;
    maxWallets?: number;
    requestDelayMs?: number;
    retryCount?: number;
    retryDelayMs?: number;
    persist?: boolean;
  } = {};

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === "--enabled-only") {
      result.includeDisabled = false;
      continue;
    }
    if (arg === "--force-refresh") {
      result.forceRefresh = true;
      continue;
    }
    if (arg === "--no-persist") {
      result.persist = false;
      continue;
    }
    if (arg === "--max-wallets" && next) {
      result.maxWallets = Number(next);
      index += 1;
      continue;
    }
    if (arg === "--delay-ms" && next) {
      result.requestDelayMs = Number(next);
      index += 1;
      continue;
    }
    if (arg === "--retry-count" && next) {
      result.retryCount = Number(next);
      index += 1;
      continue;
    }
    if (arg === "--retry-delay-ms" && next) {
      result.retryDelayMs = Number(next);
      index += 1;
    }
  }

  return result;
}
