import dotenv from "dotenv";
import express from "express";
import { pinoHttp } from "pino-http";
import { z } from "zod";
import { loadConfig } from "./config.js";
import { logger } from "./logger.js";
import { applyOpenClawInstruction, parseOpenClawRequest } from "./openclaw/instructions.js";
import { RedisControl } from "./redisControl.js";
import { getCachedResearch, setCachedResearch } from "./openclaw/research/cache.js";
import { researchToken } from "./openclaw/research/tokenResearch.js";
import { analyzeAlphaWallets, getStoredLatestWalletAnalysis } from "./walletAnalysis.js";

dotenv.config();

const app = express();
app.use(express.json({ limit: "256kb" }));
app.use(pinoHttp({ logger }));
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redisControl = new RedisControl(redisUrl);
const openClawWebhookToken = process.env.OPENCLAW_WEBHOOK_TOKEN;
const openClawResearchToken =
  process.env.OPENCLAW_RESEARCH_TOKEN ??
  process.env.CONTROL_RESEARCH_TOKEN ??
  openClawWebhookToken;
const researchCacheTtlMs = normalizeCacheTtlMs(process.env.RESEARCH_CACHE_TTL_MS);
const TokenResearchRequestSchema = z.object({
  tokenAddress: z.string().trim().min(1),
  trackedWalletEntryMarketCapUsd: z.number().finite().optional(),
  myEntryMarketCapUsd: z.number().finite().optional(),
  myEntrySpentSol: z.number().finite().optional(),
  analysisId: z.string().trim().min(1).optional(),
  positionId: z.string().trim().min(1).optional(),
  forceRefresh: z.boolean().optional(),
});
const WalletAnalysisRequestSchema = z.object({
  includeDisabled: z.boolean().optional(),
  forceRefresh: z.boolean().optional(),
  maxWallets: z.number().int().positive().optional(),
  requestDelayMs: z.number().int().nonnegative().optional(),
  retryCount: z.number().int().nonnegative().optional(),
  retryDelayMs: z.number().int().nonnegative().optional(),
  persist: z.boolean().optional(),
});

app.use(async (_req, res, next) => {
  try {
    await redisControl.connect();
    next();
  } catch (error) {
    next(error);
  }
});

app.get("/health", (_req, res) => res.json({ ok: true }));

app.get("/config", (_req, res) => {
  const cfg = loadConfig();
  res.json({
    alphaWalletsPath: cfg.alphaWalletsPath,
    botConfigPath: cfg.botConfigPath,
    alphaWallets: {
      version: cfg.alphaWallets.version,
      total: cfg.alphaWallets.wallets.length,
      enabled: cfg.alphaWallets.wallets.filter((w) => w.enabled).length,
    },
    bot: cfg.botConfig,
  });
});

app.post("/openclaw/instructions", async (req, res, next) => {
  try {
    if (openClawWebhookToken) {
      const authHeader = req.header("authorization");
      if (authHeader !== `Bearer ${openClawWebhookToken}`) {
        res.status(401).json({ ok: false, error: "unauthorized" });
        return;
      }
    }

    const payload = parseOpenClawRequest(req.body);
    const results = [];
    for (const instruction of payload.instructions) {
      const result = await applyOpenClawInstruction(redisControl, instruction, payload.analysis_id);
      results.push(result);
    }

    logger.info(
      {
        analysisId: payload.analysis_id,
        tokenMint: payload.token?.mint,
        instructionCount: payload.instructions.length,
        results,
      },
      "processed OpenClaw instructions",
    );

    res.json({
      ok: true,
      analysis_id: payload.analysis_id ?? null,
      results,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/research/token", async (req, res, next) => {
  try {
    if (openClawResearchToken) {
      const authHeader = req.header("authorization");
      if (authHeader !== `Bearer ${openClawResearchToken}`) {
        res.status(401).json({ ok: false, error: "unauthorized" });
        return;
      }
    }

    const payload = TokenResearchRequestSchema.parse(req.body);
    const context = {
      trackedWalletEntryMarketCapUsd: payload.trackedWalletEntryMarketCapUsd,
      myEntryMarketCapUsd: payload.myEntryMarketCapUsd,
      myEntrySpentSol: payload.myEntrySpentSol,
    };
    const cachedResult = payload.forceRefresh ? null : getCachedResearch(payload.tokenAddress, context);
    const result = cachedResult ?? await researchToken(payload.tokenAddress, context);
    if (!cachedResult) {
      setCachedResearch(payload.tokenAddress, context, result, researchCacheTtlMs);
    }

    res.json({
      ok: true,
      analysis_id: payload.analysisId ?? null,
      position_id: payload.positionId ?? null,
      token_address: payload.tokenAddress,
      cached: Boolean(cachedResult),
      cache_ttl_ms: researchCacheTtlMs,
      result,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/wallets/analyze", async (req, res, next) => {
  try {
    if (openClawResearchToken) {
      const authHeader = req.header("authorization");
      if (authHeader !== `Bearer ${openClawResearchToken}`) {
        res.status(401).json({ ok: false, error: "unauthorized" });
        return;
      }
    }

    const payload = WalletAnalysisRequestSchema.parse(req.body ?? {});
    const report = await analyzeAlphaWallets(payload);
    res.json({
      ok: true,
      generated_at: report.generatedAt,
      report,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/wallets/analyze/latest", async (req, res, next) => {
  try {
    if (openClawResearchToken) {
      const authHeader = req.header("authorization");
      if (authHeader !== `Bearer ${openClawResearchToken}`) {
        res.status(401).json({ ok: false, error: "unauthorized" });
        return;
      }
    }

    const report = await getStoredLatestWalletAnalysis();
    if (!report) {
      res.status(404).json({ ok: false, error: "no stored wallet analysis" });
      return;
    }

    res.json({
      ok: true,
      generated_at: report.generatedAt,
      report,
    });
  } catch (error) {
    next(error);
  }
});

app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err: error }, "control plane request failed");
  const message = error instanceof Error ? error.message : "internal error";
  res.status(400).json({ ok: false, error: message });
});

const bind = resolveBind(process.env.CONTROL_BIND, process.env.PORT);
const onListen = () => {
  const cfg = loadConfig();
  logger.info(
    {
      port: bind.port,
      host: bind.host,
      controlBind: bind.raw,
      alphaWalletsTotal: cfg.alphaWallets.wallets.length,
      alphaWalletsEnabled: cfg.alphaWallets.wallets.filter((w) => w.enabled).length,
      dryRun: cfg.botConfig.mode.dry_run,
      simulateOnly: cfg.botConfig.mode.simulate_only,
      openClawWebhookEnabled: Boolean(openClawWebhookToken),
      openClawResearchAuthEnabled: Boolean(openClawResearchToken),
      researchCacheTtlMs,
    },
    "control plane started (scaffold)",
  );
};

if (bind.host) {
  app.listen(bind.port, bind.host, onListen);
} else {
  app.listen(bind.port, onListen);
}

function normalizeCacheTtlMs(value: string | undefined): number {
  const parsed = Number(value ?? "30000");
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 30000;
}

function resolveBind(controlBind: string | undefined, portValue: string | undefined): {
  host: string | undefined;
  port: number;
  raw: string | null;
} {
  const raw = controlBind?.trim() || null;
  if (raw) {
    const lastColon = raw.lastIndexOf(":");
    if (lastColon > 0 && lastColon < raw.length - 1) {
      const host = raw.slice(0, lastColon).trim();
      const port = Number(raw.slice(lastColon + 1).trim());
      if (host && Number.isInteger(port) && port > 0) {
        return { host, port, raw };
      }
    }
  }

  const port = Number(portValue ?? "8787");
  return {
    host: undefined,
    port: Number.isInteger(port) && port > 0 ? port : 8787,
    raw,
  };
}
