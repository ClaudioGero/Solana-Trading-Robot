import fs from "node:fs";
import path from "node:path";
import { z } from "zod";

const AlphaWalletsSchema = z.object({
  version: z.number(),
  wallets: z.array(
    z.object({
      label: z.string(),
      address: z.string(),
      enabled: z.boolean(),
      strategy: z.string().optional(),
    }),
  ),
});

const FlatExitsSchema = z.object({
  take_profit_pct: z.number(),
  stop_loss_pct: z.number(),
  sell_percent_on_take_profit: z.number(),
  sell_percent_on_stop_loss: z.number(),
});

const NestedExitsSchema = z.object({
  mode: z.string(),
  basic: FlatExitsSchema,
  two_phase: z
    .object({
      no_action_seconds: z.number().optional(),
      runner_gate_pct: z.number().optional(),
      runner_tp_pct: z.number().optional(),
      runner_tp_lock_floor_pct: z.number().optional(),
      stop_loss_pct_after_window: z.number().optional(),
      quick_breakeven_buffer_sol: z.number().optional(),
    })
    .optional(),
});

const BotConfigSchema = z.object({
  version: z.number(),
  mode: z.object({
    dry_run: z.boolean(),
    simulate_only: z.boolean(),
  }),
  copytrade: z.object({
    fixed_buy_sol: z.number(),
  }),
  exits: z.union([FlatExitsSchema, NestedExitsSchema]),
  filters: z
    .object({
      enabled: z.boolean(),
      only_buys: z.boolean(),
      allowed_venues: z.array(z.string()),
      min_notional_sol: z.number(),
    })
    .optional(),
  executor: z
    .object({
      enabled: z.boolean(),
      slippage_bps: z.number(),
      wrap_and_unwrap_sol: z.boolean(),
      user_public_key_env: z.string(),
    })
    .optional(),
  providers: z.object({
    rpc: z.object({
      primary: z.string(),
      ws: z.string(),
    }),
    jupiter: z
      .object({
        base_url: z.string(),
      })
      .optional(),
    jito: z
      .object({
        enabled: z.boolean(),
        max_tip_sol: z.number(),
        max_priority_fee_sol: z.number(),
      })
      .optional(),
  }),
});

function readJson(p: string): unknown {
  const abs = path.resolve(p);
  const raw = fs.readFileSync(abs, "utf8");
  return JSON.parse(raw) as unknown;
}

export function loadConfig() {
  const alphaWalletsPath = process.env.ALPHA_WALLETS_PATH ?? "../config/alpha_wallets.json";
  const botConfigPath = process.env.BOT_CONFIG_PATH ?? "../config/bot.json";

  const alphaWallets = AlphaWalletsSchema.parse(readJson(alphaWalletsPath));
  const botConfig = BotConfigSchema.parse(readJson(botConfigPath));

  return { alphaWalletsPath, botConfigPath, alphaWallets, botConfig };
}
