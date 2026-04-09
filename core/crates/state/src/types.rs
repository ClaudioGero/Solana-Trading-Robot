use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Minimal event payload from the wallet streamer (Block 3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletEvent {
    pub signature: String,
    pub slot: u64,
    pub wallet: String,
    #[serde(default)]
    pub wallet_label: Option<String>,
    pub observed_at: DateTime<Utc>,
}

/// Output of the classifier (Block 4) and input to filters/executor (Blocks 5–7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeIntent {
    pub signature: String,
    pub slot: u64,
    pub wallet: String,
    pub side: TradeSide,
    pub mint: String,
    pub notional_sol: f64,
    #[serde(default)]
    pub venue: Option<String>,
    /// Timestamp when the streamer observed the triggering tx (best-effort local time).
    #[serde(default)]
    pub observed_at: Option<DateTime<Utc>>,
    /// Timestamp when classification completed (best-effort local time).
    #[serde(default)]
    pub classified_at: Option<DateTime<Utc>>,
    /// Optional amount to use as swap input amount (base units string).
    /// - For sells: token amount in base units (required).
    /// - For buys: can be omitted (executor uses fixed SOL config).
    #[serde(default)]
    pub amount_in_base_units: Option<String>,
    /// Optional token amount delta in base units for the traded mint.
    /// - For buys: token received (base units).
    /// - For sells: token spent (base units).
    /// Used for best-effort analytics/alerts (e.g., price/mcap at alpha buy).
    #[serde(default)]
    pub token_delta_base_units: Option<String>,
    /// Optional explicit BUY size override in SOL. Used by staged strategies such as buy_dip.
    #[serde(default)]
    pub requested_buy_sol: Option<f64>,
    /// For tracked-wallet SELLs, whether the source wallet fully exited the token in this tx.
    #[serde(default)]
    pub source_wallet_exit_full: bool,
    #[serde(default)]
    pub source_wallet_sold_pct: Option<f64>,
    pub created_at: DateTime<Utc>,
}

/// Output of filters (Block 5) and input to executor (Blocks 6–7).
///
/// IMPORTANT: Keep JSON shape stable; other components enqueue/dequeue this payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecOrder {
    pub intent: TradeIntent,
    /// For audit/debug. Executor can log this.
    pub filter_reason: String,
    /// When this order was enqueued for execution (best-effort local time).
    #[serde(default)]
    pub exec_enqueued_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: String,
    pub wallet: String,
    pub mint: String,
    pub opened_at: DateTime<Utc>,
    pub buy_sig: String,
    pub size_sol: f64,
    /// Legacy combined strategy id kept for backward-compat with older stored positions.
    #[serde(default)]
    pub strategy_id: Option<String>,
    /// Buy strategy used for this position.
    #[serde(default)]
    pub buy_strategy_id: Option<String>,
    /// Sell strategy used for this position.
    #[serde(default)]
    pub sell_strategy_id: Option<String>,
    /// String to avoid float rounding issues for token amounts.
    pub token_amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitPlanTemplate {
    pub mode: String,
    #[serde(default)]
    pub take_profit_pct: Option<f64>,
    #[serde(default)]
    pub stop_loss_pct: Option<f64>,
    #[serde(default)]
    pub sell_percent_on_take_profit: Option<f64>,
    #[serde(default)]
    pub sell_percent_on_stop_loss: Option<f64>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub tp1_upside_pct: Option<f64>,
    #[serde(default)]
    pub moonbag_target_multiple: Option<f64>,
    #[serde(default)]
    pub stale_exit_after_seconds: Option<i64>,
    #[serde(default)]
    pub stale_exit_requires_non_loss: Option<bool>,
}

impl ExitPlanTemplate {
    pub fn to_position_plan(&self, position_id: &str, source: &str) -> PositionExitPlan {
        PositionExitPlan {
            position_id: position_id.to_string(),
            mode: self.mode.clone(),
            source: source.to_string(),
            take_profit_pct: self.take_profit_pct,
            stop_loss_pct: self.stop_loss_pct,
            sell_percent_on_take_profit: self.sell_percent_on_take_profit,
            sell_percent_on_stop_loss: self.sell_percent_on_stop_loss,
            notes: self.notes.clone(),
            tp1_upside_pct: self.tp1_upside_pct,
            moonbag_target_multiple: self.moonbag_target_multiple,
            stale_exit_after_seconds: self.stale_exit_after_seconds,
            stale_exit_requires_non_loss: self.stale_exit_requires_non_loss,
            updated_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionExitPlan {
    pub position_id: String,
    pub mode: String,
    pub source: String,
    #[serde(default)]
    pub take_profit_pct: Option<f64>,
    #[serde(default)]
    pub stop_loss_pct: Option<f64>,
    #[serde(default)]
    pub sell_percent_on_take_profit: Option<f64>,
    #[serde(default)]
    pub sell_percent_on_stop_loss: Option<f64>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub tp1_upside_pct: Option<f64>,
    #[serde(default)]
    pub moonbag_target_multiple: Option<f64>,
    #[serde(default)]
    pub stale_exit_after_seconds: Option<i64>,
    #[serde(default)]
    pub stale_exit_requires_non_loss: Option<bool>,
    pub updated_at: DateTime<Utc>,
}

/// Simple alert message event (Block 9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub ts: DateTime<Utc>,
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuyDipSetup {
    pub id: String,
    pub wallet: String,
    pub mint: String,
    pub analysis_id: String,
    #[serde(default)]
    pub analysis_summary: Option<String>,
    #[serde(default)]
    pub alpha_buy_market_cap_usd: Option<f64>,
    pub alpha_notional_sol: f64,
    pub alpha_observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub buy_point_1_market_cap_usd: Option<f64>,
    #[serde(default)]
    pub buy_point_2_market_cap_usd: Option<f64>,
    pub total_budget_sol: f64,
    pub entry1_budget_sol: f64,
    pub entry2_budget_sol: f64,
    #[serde(default)]
    pub entry1_filled: bool,
    #[serde(default)]
    pub entry2_filled: bool,
    #[serde(default)]
    pub entry1_armed: bool,
    #[serde(default)]
    pub entry2_armed: bool,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub cancel_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuyDipSetupUpdate {
    pub setup: BuyDipSetup,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlphaPlaybookScenario {
    NewHot,
    OldDormantSpiked,
    MidTrend,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlphaPlaybookWatchKind {
    OldDormantRetrace,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlphaPlaybookRiskTier {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaPlaybookTokenState {
    pub mint: String,
    #[serde(default)]
    pub scenario: Option<AlphaPlaybookScenario>,
    #[serde(default)]
    pub chosen_playbook: Option<String>,
    #[serde(default)]
    pub chosen_size_sol: Option<f64>,
    #[serde(default)]
    pub first_wallet: Option<String>,
    #[serde(default)]
    pub first_buy_signature: Option<String>,
    #[serde(default)]
    pub first_buy_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub latest_wallet: Option<String>,
    #[serde(default)]
    pub latest_event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub confirmation_wallets: Vec<String>,
    #[serde(default)]
    pub confirmation_triggered: bool,
    #[serde(default)]
    pub entry_executed: bool,
    #[serde(default)]
    pub entry_executed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub confirmation_entry_executed: bool,
    #[serde(default)]
    pub confirmation_entry_executed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cooldown_active: bool,
    #[serde(default)]
    pub cooldown_expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub watch_active: bool,
    #[serde(default)]
    pub token_age_seconds: Option<i64>,
    #[serde(default)]
    pub token_age_bucket: Option<String>,
    #[serde(default)]
    pub market_cap_usd: Option<f64>,
    #[serde(default)]
    pub peak_market_cap_usd: Option<f64>,
    #[serde(default)]
    pub volume_5m_usd: Option<f64>,
    #[serde(default)]
    pub volume_30m_usd: Option<f64>,
    #[serde(default)]
    pub recent_activity_bucket: Option<String>,
    #[serde(default)]
    pub risk_tier: Option<AlphaPlaybookRiskTier>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaPlaybookWatchState {
    pub mint: String,
    pub kind: AlphaPlaybookWatchKind,
    #[serde(default)]
    pub first_wallet: Option<String>,
    #[serde(default)]
    pub first_buy_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub spike_market_cap_usd: Option<f64>,
    #[serde(default)]
    pub spike_price_usd: Option<f64>,
    #[serde(default)]
    pub retrace_15_level_usd: Option<f64>,
    #[serde(default)]
    pub retrace_50_level_usd: Option<f64>,
    #[serde(default)]
    pub last_low_observed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub lowest_market_cap_usd: Option<f64>,
    #[serde(default)]
    pub stabilization_deadline_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub cancel_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaPlaybookCooldownState {
    pub mint: String,
    pub active: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub triggered_by_wallet: Option<String>,
    #[serde(default)]
    pub triggered_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaPlaybookConfirmationState {
    pub mint: String,
    #[serde(default)]
    pub first_wallet: Option<String>,
    #[serde(default)]
    pub first_buy_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub wallets: Vec<String>,
    #[serde(default)]
    pub last_confirmed_wallet: Option<String>,
    #[serde(default)]
    pub confirmation_triggered: bool,
    #[serde(default)]
    pub confirmation_triggered_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaPlaybookAddSetup {
    pub id: String,
    pub wallet: String,
    pub mint: String,
    pub analysis_id: String,
    #[serde(default)]
    pub scenario: Option<AlphaPlaybookScenario>,
    #[serde(default)]
    pub analysis_summary: Option<String>,
    #[serde(default)]
    pub alpha_buy_market_cap_usd: Option<f64>,
    pub alpha_notional_sol: f64,
    pub alpha_observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub target_market_cap_usd: f64,
    pub budget_sol: f64,
    #[serde(default)]
    pub armed: bool,
    #[serde(default)]
    pub filled: bool,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub cancel_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaPlaybookAddSetupUpdate {
    pub setup: AlphaPlaybookAddSetup,
}
