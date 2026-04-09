/// Redis keyspace layout (v1).
///
/// Conventions:
/// - Prefix everything with `sb:` (solana-bot) to avoid collisions
/// - Use `:` delimiters
/// - Never store secrets
pub struct Keys;

impl Keys {
    /// Dedupe key for a detected on-chain event (by signature).
    /// Value: "1"
    pub fn dedupe_sig(sig: &str) -> String {
        format!("sb:dedupe:sig:{sig}")
    }

    /// Queue of raw wallet events (JSON-encoded) waiting for classification.
    /// Type: Redis List
    pub const Q_WALLET_EVENTS: &'static str = "sb:q:wallet_events";

    /// Queue of trade intents (JSON-encoded) waiting for execution.
    /// Type: Redis List
    pub const Q_TRADE_INTENTS: &'static str = "sb:q:trade_intents";

    /// Queue of approved execution orders (JSON-encoded) waiting for executor (Block 6).
    /// Type: Redis List
    pub const Q_EXEC_ORDERS: &'static str = "sb:q:exec_orders";

    /// Queue of alert events (JSON-encoded) waiting for Telegram sender (Block 9).
    /// Type: Redis List
    pub const Q_ALERTS: &'static str = "sb:q:alerts";

    /// Queue of alpha-wallet sell signals (TradeIntent JSON) to let the exit engine react immediately.
    /// Type: Redis List
    pub const Q_ALPHA_SELL_SIGNALS: &'static str = "sb:q:alpha_sell_signals";

    /// Queue of post-buy analysis exit-plan overrides (PositionExitPlan JSON).
    /// Type: Redis List
    pub const Q_POSITION_EXIT_PLAN_UPDATES: &'static str = "sb:q:position_exit_plan_updates";

    /// Queue of buy_dip setup updates (BuyDipSetupUpdate JSON) sent from OpenClaw/control.
    /// Type: Redis List
    pub const Q_BUY_DIP_SETUP_UPDATES: &'static str = "sb:q:buy_dip_setup_updates";

    /// Queue of alpha_playbook add setup updates sent from OpenClaw/control.
    pub const Q_ALPHA_PLAYBOOK_ADD_UPDATES: &'static str = "sb:q:alpha_playbook_add_updates";

    /// Last mints traded by the bot (most recent first). Ring buffer trimmed to last N (v1=10).
    /// Type: Redis List
    pub const RECENT_MINTS: &'static str = "sb:recent:mints";

    /// Per-mint short lock to prevent duplicate buys while one is in-flight.
    pub fn inflight_mint(mint: &str) -> String {
        format!("sb:inflight:mint:{mint}")
    }

    /// Per-position sell lock to prevent multiple overlapping sell attempts.
    pub fn inflight_sell(pos_id: &str) -> String {
        format!("sb:inflight:sell:{pos_id}")
    }

    /// Control flag: pause buys (sells still allowed).
    pub const CTRL_PAUSE_BUYS: &'static str = "sb:ctrl:pause_buys";

    /// Control flag: emergency stop (block buys + sells).
    pub const CTRL_EMERGENCY_STOP: &'static str = "sb:ctrl:emergency_stop";

    /// Total lamports spent buying a mint (best-effort).
    pub fn spent_mint_lamports(mint: &str) -> String {
        format!("sb:spent:mint:{mint}")
    }

    /// Cached token info (e.g., Birdeye-derived name/symbol/marketcap).
    pub fn token_info(mint: &str) -> String {
        format!("sb:tokeninfo:{mint}")
    }

    /// Alpha playbook token runtime state.
    pub fn token_state(mint: &str) -> String {
        format!("sb:token:state:{mint}")
    }

    /// Alpha playbook watch state (e.g. dormant-spike retrace watch).
    pub fn token_watch(mint: &str) -> String {
        format!("sb:token:watch:{mint}")
    }

    /// Set of mints with active alpha_playbook watch state.
    pub const ALPHA_PLAYBOOK_WATCH_MINTS_ACTIVE: &'static str = "sb:token:watch:active";

    /// Alpha playbook cooldown state after a bad fast flip.
    pub fn token_cooldown(mint: &str) -> String {
        format!("sb:token:cooldown:{mint}")
    }

    /// Alpha playbook confirmation wallet tracking.
    pub fn token_confirmation(mint: &str) -> String {
        format!("sb:token:confirm:{mint}")
    }

    /// Alpha-wallet first BUY marker (per wallet+mint).
    /// Used to enrich alerts only once per short window.
    pub fn alpha_first_buy(wallet: &str, mint: &str) -> String {
        format!("sb:alpha:first_buy:{wallet}:{mint}")
    }

    /// Rate-limit key for noisy alpha "unclassified" alerts per wallet.
    pub fn alpha_unclassified_rl(wallet: &str) -> String {
        format!("sb:alpha:unclassified:rl:{wallet}")
    }

    /// Open position hash.
    /// Type: Redis Hash
    /// Fields:
    /// - wallet
    /// - mint
    /// - opened_at (rfc3339)
    /// - buy_sig
    /// - size_sol
    /// - token_amount (string)
    pub fn position(id: &str) -> String {
        format!("sb:pos:{id}")
    }

    /// Set of open position IDs.
    /// Type: Redis Set
    pub const OPEN_POSITIONS: &'static str = "sb:pos:open";

    /// Exit strategy state for a position (mode + flags).
    /// Type: Redis Hash
    /// Fields (v1):
    /// - mode: "unset" | "runner" | "quick"
    /// - armed: "0" | "1"  (decision made after the no-action window)
    /// - tp_locked: "0" | "1" (runner mode: once +25% reached, enable trailing-stop behavior)
    pub fn exit_state(pos_id: &str) -> String {
        format!("sb:exit:pos:{pos_id}")
    }

    /// Stored exit plan for a position.
    pub fn position_exit_plan(pos_id: &str) -> String {
        format!("sb:exitplan:pos:{pos_id}")
    }

    /// Stored buy_dip setup JSON.
    pub fn buy_dip_setup(setup_id: &str) -> String {
        format!("sb:buydip:setup:{setup_id}")
    }

    /// Set of active buy_dip setup ids.
    /// Type: Redis Set
    pub const BUY_DIP_SETUPS_ACTIVE: &'static str = "sb:buydip:setups:active";

    /// Stored alpha_playbook add setup JSON.
    pub fn alpha_playbook_add_setup(setup_id: &str) -> String {
        format!("sb:alphapb:add:{setup_id}")
    }

    /// Set of active alpha_playbook add setup ids.
    pub const ALPHA_PLAYBOOK_ADD_SETUPS_ACTIVE: &'static str = "sb:alphapb:add:active";
}
