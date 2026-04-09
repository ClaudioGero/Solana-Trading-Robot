import { createClient, type RedisClientType } from "redis";

const CTRL_PAUSE_BUYS_KEY = "sb:ctrl:pause_buys";
const CTRL_EMERGENCY_STOP_KEY = "sb:ctrl:emergency_stop";
const CTRL_AUTO_OFF_AT_KEY = "sb:ctrl:auto_off_at";
const OPEN_POSITIONS_KEY = "sb:pos:open";
const POSITION_EXIT_PLAN_UPDATES_KEY = "sb:q:position_exit_plan_updates";
const BUY_DIP_SETUP_UPDATES_KEY = "sb:q:buy_dip_setup_updates";

export type PositionRecord = {
  id: string;
  wallet: string;
  mint: string;
  openedAt: string;
  buySig: string;
  sizeSol: number;
  strategyId: string | null;
  tokenAmount: string;
};

export type PositionExitPlanUpdate = {
  position_id: string;
  mode: string;
  source: string;
  take_profit_pct?: number;
  stop_loss_pct?: number;
  sell_percent_on_take_profit?: number;
  sell_percent_on_stop_loss?: number;
  notes?: string;
  updated_at: string;
};

export type BuyDipSetup = {
  id: string;
  wallet: string;
  mint: string;
  analysis_id: string;
  analysis_summary?: string | null;
  alpha_buy_market_cap_usd?: number | null;
  alpha_notional_sol: number;
  alpha_observed_at: string;
  expires_at: string;
  buy_point_1_market_cap_usd?: number | null;
  buy_point_2_market_cap_usd?: number | null;
  total_budget_sol: number;
  entry1_budget_sol: number;
  entry2_budget_sol: number;
  entry1_filled?: boolean;
  entry2_filled?: boolean;
  entry1_armed?: boolean;
  entry2_armed?: boolean;
  cancelled?: boolean;
  cancel_reason?: string | null;
  created_at: string;
  updated_at: string;
};

export type BuyDipSetupUpdate = {
  setup: BuyDipSetup;
};

export type ControlStatus = {
  pauseBuys: boolean;
  emergencyStop: boolean;
  autoOffAt: string | null;
  checkedAt: string;
};

export class RedisControl {
  private readonly client: RedisClientType;

  constructor(redisUrl: string) {
    this.client = createClient({ url: redisUrl });
  }

  async connect(): Promise<void> {
    if (!this.client.isOpen) {
      await this.client.connect();
    }
  }

  async disconnect(): Promise<void> {
    if (this.client.isOpen) {
      await this.client.quit();
    }
  }

  async getStatus(): Promise<ControlStatus> {
    const [pauseBuys, emergencyStop, autoOffAt] = await this.client.mGet([
      CTRL_PAUSE_BUYS_KEY,
      CTRL_EMERGENCY_STOP_KEY,
      CTRL_AUTO_OFF_AT_KEY,
    ]);

    return {
      pauseBuys: pauseBuys === "1",
      emergencyStop: emergencyStop === "1",
      autoOffAt,
      checkedAt: new Date().toISOString(),
    };
  }

  async setBotOn(autoOffAt?: string): Promise<ControlStatus> {
    const multi = this.client.multi();
    multi.del(CTRL_PAUSE_BUYS_KEY);
    multi.del(CTRL_EMERGENCY_STOP_KEY);

    if (autoOffAt) {
      multi.set(CTRL_AUTO_OFF_AT_KEY, autoOffAt);
    } else {
      multi.del(CTRL_AUTO_OFF_AT_KEY);
    }

    await multi.exec();
    return this.getStatus();
  }

  async setBotOff(): Promise<ControlStatus> {
    const multi = this.client.multi();
    multi.set(CTRL_PAUSE_BUYS_KEY, "1");
    multi.set(CTRL_EMERGENCY_STOP_KEY, "1");
    multi.del(CTRL_AUTO_OFF_AT_KEY);
    await multi.exec();
    return this.getStatus();
  }

  async pauseBuysOnly(): Promise<ControlStatus> {
    const multi = this.client.multi();
    multi.set(CTRL_PAUSE_BUYS_KEY, "1");
    multi.del(CTRL_EMERGENCY_STOP_KEY);
    await multi.exec();
    return this.getStatus();
  }

  async resumeBot(): Promise<ControlStatus> {
    const multi = this.client.multi();
    multi.del(CTRL_PAUSE_BUYS_KEY);
    multi.del(CTRL_EMERGENCY_STOP_KEY);
    multi.del(CTRL_AUTO_OFF_AT_KEY);
    await multi.exec();
    return this.getStatus();
  }

  async enqueuePositionExitPlanUpdate(plan: PositionExitPlanUpdate): Promise<void> {
    await this.client.lPush(POSITION_EXIT_PLAN_UPDATES_KEY, JSON.stringify(plan));
  }

  async enqueueBuyDipSetupUpdate(update: BuyDipSetupUpdate): Promise<void> {
    await this.client.lPush(BUY_DIP_SETUP_UPDATES_KEY, JSON.stringify(update));
  }

  async getPositionById(id: string): Promise<PositionRecord | null> {
    const raw = await this.client.hGetAll(`sb:pos:${id}`);
    return this.mapPosition(id, raw);
  }

  async getOpenPositionsByMint(mint: string): Promise<PositionRecord[]> {
    const ids = await this.client.sMembers(OPEN_POSITIONS_KEY);
    const positions = await Promise.all(ids.map(async (id) => this.getPositionById(id)));
    return positions.filter((position): position is PositionRecord => position?.mint === mint);
  }

  private mapPosition(id: string, raw: Record<string, string>): PositionRecord | null {
    if (Object.keys(raw).length === 0) {
      return null;
    }

    return {
      id: raw.id ?? id,
      wallet: raw.wallet ?? "",
      mint: raw.mint ?? "",
      openedAt: raw.opened_at ?? "",
      buySig: raw.buy_sig ?? "",
      sizeSol: Number(raw.size_sol ?? "0"),
      strategyId: raw.strategy_id || null,
      tokenAmount: raw.token_amount ?? "0",
    };
  }
}
