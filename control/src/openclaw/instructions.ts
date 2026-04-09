import { z } from "zod";
import {
  RedisControl,
  type ControlStatus,
  type BuyDipSetup,
  type PositionExitPlanUpdate,
  type PositionRecord,
} from "../redisControl.js";

const ExitPlanSchema = z.object({
  mode: z.string().trim().min(1).default("basic"),
  take_profit_pct: z.number().finite().optional(),
  stop_loss_pct: z.number().finite().optional(),
  sell_percent_on_take_profit: z.number().finite().optional(),
  sell_percent_on_stop_loss: z.number().finite().optional(),
  notes: z.string().trim().min(1).max(1000).optional(),
});

const InstructionSchema = z.object({
  instruction_id: z.string().trim().min(1).optional(),
  source: z.string().trim().min(1).optional(),
  action: z.enum([
    "no_change",
    "update_exit_plan",
    "arm_buy_dip",
    "discard_buy_dip",
    "pause_buys",
    "resume",
    "emergency_stop",
  ]),
  position_id: z.string().trim().min(1).optional(),
  mint: z.string().trim().min(1).optional(),
  exit_plan: ExitPlanSchema.optional(),
  buy_point_1_market_cap_usd: z.number().finite().positive().optional(),
  buy_point_2_market_cap_usd: z.number().finite().positive().optional(),
  expiry_seconds: z.number().int().positive().max(3600).optional(),
  setup_id: z.string().trim().min(1).optional(),
  alpha_wallet: z.string().trim().min(1).optional(),
  alpha_notional_sol: z.number().finite().nonnegative().optional(),
  alpha_observed_at: z.string().trim().min(1).optional(),
  alpha_buy_market_cap_usd: z.number().finite().positive().optional(),
  analysis_summary: z.string().trim().min(1).max(4000).optional(),
});

const OpenClawRequestSchema = z.object({
  analysis_id: z.string().trim().min(1).optional(),
  token: z
    .object({
      mint: z.string().trim().min(1),
      symbol: z.string().trim().min(1).optional(),
      name: z.string().trim().min(1).optional(),
    })
    .optional(),
  analysis_summary: z.string().trim().min(1).max(4000).optional(),
  instructions: z.array(InstructionSchema).min(1),
});

export type OpenClawRequest = z.infer<typeof OpenClawRequestSchema>;

export function parseOpenClawRequest(payload: unknown): OpenClawRequest {
  return OpenClawRequestSchema.parse(payload);
}

export type OpenClawInstructionResult =
  | {
      action: "no_change";
      summary: string;
    }
  | {
      action: "control";
      summary: string;
      status: ControlStatus;
    }
  | {
      action: "update_exit_plan";
      summary: string;
      updatedPositionIds: string[];
    }
  | {
      action: "buy_dip";
      summary: string;
      setupId: string;
    };

export async function applyOpenClawInstruction(
  redisControl: RedisControl,
  instruction: OpenClawRequest["instructions"][number],
  requestAnalysisId?: string,
): Promise<OpenClawInstructionResult> {
  const source = instruction.source ?? "openclaw";
  const sourceTag = instruction.instruction_id
    ? `${source}:${instruction.instruction_id}`
    : requestAnalysisId
      ? `${source}:${requestAnalysisId}`
      : source;

  if (instruction.action === "no_change") {
    return {
      action: "no_change",
      summary:
        instruction.analysis_summary ??
        `OpenClaw kept the baseline strategy${instruction.mint ? ` for ${instruction.mint}` : ""}.`,
    };
  }

  if (instruction.action === "pause_buys") {
    const status = await redisControl.pauseBuysOnly();
    return {
      action: "control",
      status,
      summary: "OpenClaw set pause-buys.",
    };
  }

  if (instruction.action === "resume") {
    const status = await redisControl.resumeBot();
    return {
      action: "control",
      status,
      summary: "OpenClaw resumed the bot.",
    };
  }

  if (instruction.action === "emergency_stop") {
    const status = await redisControl.setBotOff();
    return {
      action: "control",
      status,
      summary: "OpenClaw triggered emergency stop.",
    };
  }

  if (instruction.action === "discard_buy_dip") {
    const setup = buildBuyDipSetup(instruction, requestAnalysisId, true);
    await redisControl.enqueueBuyDipSetupUpdate({ setup });
    return {
      action: "buy_dip",
      setupId: setup.id,
      summary:
        instruction.analysis_summary ??
        `OpenClaw discarded buy_dip for ${instruction.mint ?? "unknown mint"}.`,
    };
  }

  if (instruction.action === "arm_buy_dip") {
    const setup = buildBuyDipSetup(instruction, requestAnalysisId, false);
    if (!setup.buy_point_1_market_cap_usd && !setup.buy_point_2_market_cap_usd) {
      throw new Error("arm_buy_dip requires at least one buy point");
    }
    await redisControl.enqueueBuyDipSetupUpdate({ setup });
    return {
      action: "buy_dip",
      setupId: setup.id,
      summary:
        instruction.analysis_summary ??
        `OpenClaw armed buy_dip for ${instruction.mint ?? "unknown mint"}.`,
    };
  }

  if (!instruction.exit_plan) {
    throw new Error("update_exit_plan requires exit_plan");
  }

  const targets = await resolveTargets(redisControl, instruction.position_id, instruction.mint);
  if (targets.length === 0) {
    throw new Error("no open positions matched the OpenClaw target");
  }

  const now = new Date().toISOString();
  for (const position of targets) {
    const plan: PositionExitPlanUpdate = {
      position_id: position.id,
      mode: instruction.exit_plan.mode,
      source: sourceTag,
      take_profit_pct: instruction.exit_plan.take_profit_pct,
      stop_loss_pct: instruction.exit_plan.stop_loss_pct,
      sell_percent_on_take_profit: instruction.exit_plan.sell_percent_on_take_profit,
      sell_percent_on_stop_loss: instruction.exit_plan.sell_percent_on_stop_loss,
      notes: instruction.exit_plan.notes ?? instruction.analysis_summary,
      updated_at: now,
    };
    await redisControl.enqueuePositionExitPlanUpdate(plan);
  }

  return {
    action: "update_exit_plan",
    updatedPositionIds: targets.map((position) => position.id),
    summary: `OpenClaw updated exit plan for ${targets.length} position(s).`,
  };
}

function buildBuyDipSetup(
  instruction: OpenClawRequest["instructions"][number],
  requestAnalysisId: string | undefined,
  cancelled: boolean,
): BuyDipSetup {
  const mint = instruction.mint?.trim();
  const wallet = instruction.alpha_wallet?.trim();
  const analysisId = requestAnalysisId?.trim();
  if (!mint) {
    throw new Error(`${instruction.action} requires mint`);
  }
  if (!wallet) {
    throw new Error(`${instruction.action} requires alpha_wallet`);
  }
  if (!analysisId) {
    throw new Error(`${instruction.action} requires analysis_id`);
  }
  if (!instruction.alpha_observed_at) {
    throw new Error(`${instruction.action} requires alpha_observed_at`);
  }
  if (instruction.alpha_notional_sol == null) {
    throw new Error(`${instruction.action} requires alpha_notional_sol`);
  }

  const now = new Date();
  const expiresAt = new Date(now.getTime() + (instruction.expiry_seconds ?? 900) * 1000);
  return {
    id: instruction.setup_id?.trim() || `${wallet}:${mint}`,
    wallet,
    mint,
    analysis_id: analysisId,
    analysis_summary: instruction.analysis_summary ?? null,
    alpha_buy_market_cap_usd: instruction.alpha_buy_market_cap_usd ?? null,
    alpha_notional_sol: instruction.alpha_notional_sol,
    alpha_observed_at: new Date(instruction.alpha_observed_at).toISOString(),
    expires_at: expiresAt.toISOString(),
    buy_point_1_market_cap_usd: instruction.buy_point_1_market_cap_usd ?? null,
    buy_point_2_market_cap_usd: instruction.buy_point_2_market_cap_usd ?? null,
    total_budget_sol: 1.0,
    entry1_budget_sol: 0.5,
    entry2_budget_sol: 0.5,
    entry1_filled: false,
    entry2_filled: false,
    entry1_armed: !cancelled,
    entry2_armed: !cancelled,
    cancelled,
    cancel_reason: cancelled ? instruction.analysis_summary ?? "discarded_by_openclaw" : null,
    created_at: now.toISOString(),
    updated_at: now.toISOString(),
  };
}

async function resolveTargets(
  redisControl: RedisControl,
  positionId?: string,
  mint?: string,
): Promise<PositionRecord[]> {
  if (positionId) {
    const position = await redisControl.getPositionById(positionId);
    return position ? [position] : [];
  }

  if (mint) {
    return redisControl.getOpenPositionsByMint(mint);
  }

  throw new Error("instruction requires position_id or mint");
}
