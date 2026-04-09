use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{info, warn};
use url::Url;

#[derive(Debug, Deserialize)]
struct AlphaWalletsFile {
    version: u64,
    wallets: Vec<AlphaWalletEntry>,
}

#[derive(Debug, Deserialize)]
struct AlphaWalletEntry {
    label: String,
    address: String,
    enabled: bool,
    #[serde(default)]
    strategy: Option<String>,
    #[serde(default)]
    buy_strategy: Option<String>,
    #[serde(default)]
    sell_strategy: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BotConfigFile {
    version: u64,
    mode: BotMode,
    copytrade: CopytradeCfg,
    #[serde(default)]
    exits: Option<ExitCfg>,
    #[serde(default)]
    strategies: HashMap<String, StrategyCfg>,
    #[serde(default)]
    buy_strategies: HashMap<String, BuyStrategyCfg>,
    #[serde(default)]
    sell_strategies: HashMap<String, SellStrategyCfg>,
    #[serde(default)]
    filters: Option<FiltersCfg>,
    #[serde(default)]
    executor: Option<ExecutorCfg>,
    providers: ProvidersCfg,
}

#[derive(Debug, Deserialize)]
struct BotMode {
    dry_run: bool,
    simulate_only: bool,
}

#[derive(Debug, Deserialize)]
struct CopytradeCfg {
    fixed_buy_sol: f64,
}

#[derive(Debug, Deserialize)]
struct ExitBasicCfg {
    take_profit_pct: f64,
    stop_loss_pct: f64,
    sell_percent_on_take_profit: f64,
    sell_percent_on_stop_loss: f64,
}

#[derive(Debug, Deserialize, Clone)]
struct InAndOutCfg {
    #[serde(default)]
    tp1_upside_pct: Option<f64>,
    #[serde(default)]
    moonbag_target_multiple: Option<f64>,
    #[serde(default)]
    stale_exit_after_seconds: Option<i64>,
    #[serde(default)]
    stale_exit_requires_non_loss: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ExitTwoPhaseCfg {
    #[serde(default)]
    no_action_seconds: Option<i64>,
    #[serde(default)]
    runner_gate_pct: Option<f64>,
    #[serde(default)]
    runner_tp_pct: Option<f64>,
    #[serde(default)]
    runner_tp_lock_floor_pct: Option<f64>,
    #[serde(default)]
    stop_loss_pct_after_window: Option<f64>,
    #[serde(default)]
    quick_breakeven_buffer_sol: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ExitCfg {
    /// Exit profile selector: "basic" or "two_phase"
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    basic: Option<ExitBasicCfg>,
    #[serde(default)]
    two_phase: Option<ExitTwoPhaseCfg>,

    // Backward compat (older bot.json) – if present, treated as "basic"/"two_phase" respectively.
    #[serde(default)]
    take_profit_pct: Option<f64>,
    #[serde(default)]
    stop_loss_pct: Option<f64>,
    #[serde(default)]
    sell_percent_on_take_profit: Option<f64>,
    #[serde(default)]
    sell_percent_on_stop_loss: Option<f64>,
    #[serde(default)]
    no_action_seconds: Option<i64>,
    #[serde(default)]
    runner_gate_pct: Option<f64>,
    #[serde(default)]
    runner_tp_pct: Option<f64>,
    #[serde(default)]
    runner_tp_lock_floor_pct: Option<f64>,
    #[serde(default)]
    stop_loss_pct_after_window: Option<f64>,
    #[serde(default)]
    quick_breakeven_buffer_sol: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StrategyCfg {
    #[serde(default)]
    buy_sol: Option<f64>,
    #[serde(default)]
    exit: Option<StrategyExitCfg>,
}

#[derive(Debug, Deserialize)]
struct BuyStrategyCfg {
    mode: String,
    #[serde(default)]
    buy_sol: Option<f64>,
    #[serde(default)]
    max_fill_delay_ms: Option<i64>,
    #[serde(default)]
    max_price_above_alpha_pct: Option<f64>,
    #[serde(default)]
    min_market_cap_usd: Option<f64>,
    #[serde(default)]
    slippage_bps: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SellStrategyCfg {
    mode: String,
    #[serde(default)]
    basic: Option<ExitBasicCfg>,
    #[serde(default)]
    in_and_out: Option<InAndOutCfg>,
    #[serde(default)]
    take_profit_pct: Option<f64>,
    #[serde(default)]
    stop_loss_pct: Option<f64>,
    #[serde(default)]
    sell_percent_on_take_profit: Option<f64>,
    #[serde(default)]
    sell_percent_on_stop_loss: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StrategyExitCfg {
    mode: String,
    #[serde(default)]
    basic: Option<ExitBasicCfg>,
    #[serde(default)]
    in_and_out: Option<InAndOutCfg>,
    #[serde(default)]
    take_profit_pct: Option<f64>,
    #[serde(default)]
    stop_loss_pct: Option<f64>,
    #[serde(default)]
    sell_percent_on_take_profit: Option<f64>,
    #[serde(default)]
    sell_percent_on_stop_loss: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ProvidersCfg {
    rpc: RpcProvidersCfg,
    #[serde(default)]
    jupiter: Option<JupiterProvidersCfg>,
    #[serde(default)]
    jito: Option<JitoProvidersCfg>,
}

#[derive(Debug, Deserialize)]
struct RpcProvidersCfg {
    primary: String,
    ws: String,
}

#[derive(Debug, Deserialize)]
struct JupiterProvidersCfg {
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct JitoProvidersCfg {
    enabled: bool,
    max_tip_sol: f64,
    max_priority_fee_sol: f64,
}

#[derive(Debug, Deserialize)]
struct FiltersCfg {
    enabled: bool,
    only_buys: bool,
    allowed_venues: Vec<String>,
    min_notional_sol: f64,
    #[serde(default)]
    min_market_cap_usd: Option<f64>,
    #[serde(default)]
    max_market_cap_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ExecutorCfg {
    enabled: bool,
    slippage_bps: u64,
    wrap_and_unwrap_sol: bool,
    user_public_key_env: String,
    #[serde(default)]
    min_sell_token_ui: Option<f64>,
    #[serde(default)]
    sell_dust_alert_ttl_seconds: Option<u64>,
}

fn init_tracing() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}

fn exit_template_from_strategy_cfg(
    strategy: &StrategyCfg,
) -> Option<state::types::ExitPlanTemplate> {
    let exit = strategy.exit.as_ref()?;
    let mode = exit.mode.trim();
    if mode.is_empty() {
        return None;
    }

    match mode {
        "in_and_out" => Some(state::types::ExitPlanTemplate {
            mode: "in_and_out".into(),
            take_profit_pct: None,
            stop_loss_pct: None,
            sell_percent_on_take_profit: None,
            sell_percent_on_stop_loss: None,
            notes: Some("Strategy-configured in_and_out plan.".into()),
            tp1_upside_pct: exit
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.tp1_upside_pct)
                .or(Some(12.0)),
            moonbag_target_multiple: exit
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.moonbag_target_multiple)
                .or(Some(2.0)),
            stale_exit_after_seconds: exit
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.stale_exit_after_seconds),
            stale_exit_requires_non_loss: exit
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.stale_exit_requires_non_loss),
        }),
        "basic" => {
            let basic = exit.basic.as_ref();
            Some(state::types::ExitPlanTemplate {
                mode: "basic".into(),
                take_profit_pct: basic
                    .map(|cfg| cfg.take_profit_pct)
                    .or(exit.take_profit_pct),
                stop_loss_pct: basic.map(|cfg| cfg.stop_loss_pct).or(exit.stop_loss_pct),
                sell_percent_on_take_profit: basic
                    .map(|cfg| cfg.sell_percent_on_take_profit)
                    .or(exit.sell_percent_on_take_profit),
                sell_percent_on_stop_loss: basic
                    .map(|cfg| cfg.sell_percent_on_stop_loss)
                    .or(exit.sell_percent_on_stop_loss),
                notes: Some("Strategy-configured basic exit plan.".into()),
                tp1_upside_pct: None,
                moonbag_target_multiple: None,
                stale_exit_after_seconds: None,
                stale_exit_requires_non_loss: None,
            })
        }
        _ => None,
    }
}

fn exit_template_from_sell_strategy_cfg(
    strategy: &SellStrategyCfg,
) -> Option<state::types::ExitPlanTemplate> {
    let mode = strategy.mode.trim();
    if mode.is_empty() {
        return None;
    }

    match mode {
        "in_and_out" => Some(state::types::ExitPlanTemplate {
            mode: "in_and_out".into(),
            take_profit_pct: None,
            stop_loss_pct: None,
            sell_percent_on_take_profit: None,
            sell_percent_on_stop_loss: None,
            notes: Some("Sell-strategy configured in_and_out plan.".into()),
            tp1_upside_pct: strategy
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.tp1_upside_pct)
                .or(Some(12.0)),
            moonbag_target_multiple: strategy
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.moonbag_target_multiple)
                .or(Some(2.0)),
            stale_exit_after_seconds: strategy
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.stale_exit_after_seconds),
            stale_exit_requires_non_loss: strategy
                .in_and_out
                .as_ref()
                .and_then(|cfg| cfg.stale_exit_requires_non_loss),
        }),
        "basic" => {
            let basic = strategy.basic.as_ref();
            Some(state::types::ExitPlanTemplate {
                mode: "basic".into(),
                take_profit_pct: basic
                    .map(|cfg| cfg.take_profit_pct)
                    .or(strategy.take_profit_pct),
                stop_loss_pct: basic
                    .map(|cfg| cfg.stop_loss_pct)
                    .or(strategy.stop_loss_pct),
                sell_percent_on_take_profit: basic
                    .map(|cfg| cfg.sell_percent_on_take_profit)
                    .or(strategy.sell_percent_on_take_profit),
                sell_percent_on_stop_loss: basic
                    .map(|cfg| cfg.sell_percent_on_stop_loss)
                    .or(strategy.sell_percent_on_stop_loss),
                notes: Some("Sell-strategy configured basic exit plan.".into()),
                tp1_upside_pct: None,
                moonbag_target_multiple: None,
                stale_exit_after_seconds: None,
                stale_exit_requires_non_loss: None,
            })
        }
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load ./env if present (developer convenience). We can't commit .env to repo.
    let _ = dotenvy::dotenv();
    init_tracing();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let db_enabled = std::env::var("DB_ENABLED")
        .unwrap_or_else(|_| "false".into())
        .to_lowercase()
        == "true";
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://bot:bot@127.0.0.1:5432/bot".into());

    let wallets_path = std::env::var("ALPHA_WALLETS_PATH")
        .unwrap_or_else(|_| "../config/alpha_wallets.json".into());
    let bot_cfg_path =
        std::env::var("BOT_CONFIG_PATH").unwrap_or_else(|_| "../config/bot.json".into());

    let wallets: AlphaWalletsFile = common::read_json_file(&wallets_path)
        .with_context(|| format!("ALPHA_WALLETS_PATH={wallets_path}"))?;
    let bot_cfg: BotConfigFile = common::read_json_file(&bot_cfg_path)
        .with_context(|| format!("BOT_CONFIG_PATH={bot_cfg_path}"))?;

    info!(
        version = wallets.version,
        total = wallets.wallets.len(),
        enabled = wallets.wallets.iter().filter(|w| w.enabled).count(),
        "loaded alpha wallets"
    );

    // Per-wallet strategy ids (computed before we move `wallets.wallets` into `enabled_wallets`).
    let wallet_buy_strategy_id: HashMap<String, String> = wallets
        .wallets
        .iter()
        .map(|w| {
            let strat = w
                .buy_strategy
                .clone()
                .filter(|s| !s.trim().is_empty())
                .or_else(|| w.strategy.clone().filter(|s| !s.trim().is_empty()))
                .unwrap_or_else(|| "mirror_immediate".into());
            (w.address.clone(), strat)
        })
        .collect();
    let wallet_sell_strategy_id: HashMap<String, String> = wallets
        .wallets
        .iter()
        .map(|w| {
            let strat = w
                .sell_strategy
                .clone()
                .filter(|s| !s.trim().is_empty())
                .or_else(|| w.strategy.clone().filter(|s| !s.trim().is_empty()))
                .unwrap_or_else(|| "in_and_out".into());
            (w.address.clone(), strat)
        })
        .collect();

    info!(
        version = bot_cfg.version,
        dry_run = bot_cfg.mode.dry_run,
        simulate_only = bot_cfg.mode.simulate_only,
        fixed_buy_sol = bot_cfg.copytrade.fixed_buy_sol,
        exit_mode = %bot_cfg
            .exits
            .as_ref()
            .and_then(|cfg| cfg.mode.clone())
            .unwrap_or_else(|| "none".into()),
        "loaded bot config"
    );

    // Block 2 init: state backends (Redis always, Postgres optional).
    let redis_state = state::RedisState::new(&redis_url)?;
    info!(redis_url = %redis_url, "redis configured");

    // Block 9: Telegram alerts (optional; enabled if TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID are set).
    let telegram = match (
        std::env::var("TELEGRAM_BOT_TOKEN").ok(),
        std::env::var("TELEGRAM_CHAT_ID").ok(),
    ) {
        (Some(t), Some(c)) if !t.is_empty() && !c.is_empty() => Some(alerts::TelegramConfig {
            bot_token: t,
            chat_id: c,
        }),
        _ => None,
    };
    let alerts_cfg = alerts::AlertWorkerConfig {
        enabled: true,
        idle_sleep_ms: 100,
        telegram,
    };
    let redis_for_alerts = redis_state.clone();
    tokio::spawn(async move {
        if let Err(e) = alerts::run_alert_worker(alerts_cfg, redis_for_alerts).await {
            warn!(error = %e, "alert worker task exited");
        }
    });

    if db_enabled {
        let _db = db::Db::connect(&database_url, 5).await?;
        info!("postgres configured (DB_ENABLED=true)");
    } else {
        info!("postgres disabled (DB_ENABLED=false)");
    }

    if bot_cfg.mode.simulate_only && !bot_cfg.mode.dry_run {
        warn!("SIMULATE_ONLY=true but DRY_RUN=false; this will be treated as dry-run in v1 scaffolding");
    }

    // Block 3: wallet streamer (WS logsSubscribe mentions filter).
    let enabled_wallets: Vec<stream::TrackedWallet> = wallets
        .wallets
        .into_iter()
        .filter(|w| w.enabled)
        .map(|w| stream::TrackedWallet {
            address: w.address,
            label: w.label,
        })
        .collect::<Vec<_>>();

    let ws_url = std::env::var("RPC_WS_URL").unwrap_or_else(|_| bot_cfg.providers.rpc.ws.clone());
    let ws_url = Url::parse(&ws_url).context("RPC_WS_URL/providers.rpc.ws must be a valid URL")?;

    let streamer_cfg = stream::StreamerConfig {
        ws_url,
        commitment: "processed".to_string(),
        dedupe_ttl_seconds: 300,
        include_failed: false,
    };

    let redis_for_stream = redis_state.clone();
    tokio::spawn(async move {
        if let Err(e) =
            stream::run_wallet_streamer(streamer_cfg, redis_for_stream, enabled_wallets).await
        {
            warn!(error = %e, "wallet streamer task exited");
        }
    });

    // Block 4: classifier worker (wallet_event -> getTransaction -> TradeIntent).
    let rpc_http_url =
        std::env::var("RPC_HTTP_URL").unwrap_or_else(|_| bot_cfg.providers.rpc.primary.clone());
    let jupiter_api_key = std::env::var("JUPITER_API_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    let birdeye_api_key = std::env::var("BIRDEYE_API_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    // Use the same threshold as filters (if configured) to reduce alpha alert spam for low-mcap tokens.
    let min_alert_market_cap_usd = bot_cfg
        .filters
        .as_ref()
        .and_then(|f| f.min_market_cap_usd)
        .unwrap_or(10_000.0);
    let max_alert_market_cap_usd = bot_cfg
        .filters
        .as_ref()
        .and_then(|f| f.max_market_cap_usd)
        .unwrap_or(0.0);
    let classifier_cfg = classify::ClassifierWorkerConfig {
        rpc_http_url,
        birdeye_api_key: birdeye_api_key.clone(),
        idle_sleep_ms: 100,
        max_retries: 2,
        min_alert_market_cap_usd,
        max_alert_market_cap_usd,
    };
    let redis_for_classifier = redis_state.clone();
    tokio::spawn(async move {
        if let Err(e) = classify::run_classifier_worker(classifier_cfg, redis_for_classifier).await
        {
            warn!(error = %e, "classifier worker task exited");
        }
    });

    // Block 5: filter worker (TradeIntent -> ExecOrder).
    let f = bot_cfg.filters.unwrap_or(FiltersCfg {
        enabled: true,
        only_buys: true,
        allowed_venues: vec!["pumpfun".into(), "jupiter".into(), "unknown".into()],
        min_notional_sol: 0.0,
        min_market_cap_usd: Some(10_000.0),
        max_market_cap_usd: None,
    });
    // Block 6: executor worker (ExecOrder -> Jupiter swap -> simulate).
    let exec_cfg = bot_cfg.executor.unwrap_or(ExecutorCfg {
        enabled: true,
        slippage_bps: 1000,
        wrap_and_unwrap_sol: true,
        user_public_key_env: "USER_PUBLIC_KEY".into(),
        min_sell_token_ui: None,
        sell_dust_alert_ttl_seconds: None,
    });

    let user_public_key = std::env::var(&exec_cfg.user_public_key_env).unwrap_or_default();
    if user_public_key.is_empty() {
        warn!(
            env = %exec_cfg.user_public_key_env,
            "executor enabled but USER_PUBLIC_KEY is missing; set it to your bot wallet pubkey to build Jupiter swaps"
        );
    }

    let rpc_http_url =
        std::env::var("RPC_HTTP_URL").unwrap_or_else(|_| bot_cfg.providers.rpc.primary.clone());
    let jup_base = bot_cfg
        .providers
        .jupiter
        .as_ref()
        .map(|j| j.base_url.clone())
        .unwrap_or_else(|| "https://api.jup.ag".into());

    // Optional: Jupiter API key (some deployments require it; returns 401 without).
    // (also used by classifier for SOL/USD conversion in alpha-buy enrichment)

    // Block 7 always-on Jito:
    // - Endpoint is provided via env (so you can switch providers without changing config).
    // - Tip is bounded by config/providers.jito.max_tip_sol.
    let jito_bundle_endpoint = std::env::var("JITO_BUNDLE_ENDPOINT").ok();
    let keypair_path = std::env::var("KEYPAIR_PATH").ok();
    let jito_tip_sol = bot_cfg
        .providers
        .jito
        .as_ref()
        .filter(|j| j.enabled)
        .map(|j| j.max_tip_sol)
        .unwrap_or(0.0);
    let max_priority_fee_lamports: u64 = bot_cfg
        .providers
        .jito
        .as_ref()
        .filter(|j| j.enabled)
        .map(|j| (j.max_priority_fee_sol * 1_000_000_000.0).round() as u64)
        .unwrap_or(0);

    let min_sell_token_ui = exec_cfg.min_sell_token_ui.unwrap_or(0.00001);
    let sell_dust_alert_ttl_seconds = exec_cfg.sell_dust_alert_ttl_seconds.unwrap_or(600);
    let openclaw_event_url = std::env::var("OPENCLAW_EVENT_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("OPENCLAW_ANALYSIS_URL")
                .ok()
                .filter(|s| !s.trim().is_empty())
        });
    let openclaw_api_key = std::env::var("OPENCLAW_EVENT_TOKEN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("OPENCLAW_EVENT_API_KEY")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .or_else(|| {
            std::env::var("OPENCLAW_API_KEY")
                .ok()
                .filter(|s| !s.trim().is_empty())
        });
    let helius_api_key = std::env::var("HELIUS_API_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let helius_api_base_url = std::env::var("HELIUS_API_BASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "https://mainnet.helius-rpc.com".to_string());

    let default_basic_take_profit_pct = bot_cfg
        .exits
        .as_ref()
        .and_then(|cfg| cfg.basic.as_ref().map(|v| v.take_profit_pct))
        .or_else(|| bot_cfg.exits.as_ref().and_then(|cfg| cfg.take_profit_pct))
        .unwrap_or(20.0);
    let default_basic_stop_loss_pct = bot_cfg
        .exits
        .as_ref()
        .and_then(|cfg| cfg.basic.as_ref().map(|v| v.stop_loss_pct))
        .or_else(|| bot_cfg.exits.as_ref().and_then(|cfg| cfg.stop_loss_pct))
        .unwrap_or(25.0);
    let default_basic_sell_percent_on_take_profit = bot_cfg
        .exits
        .as_ref()
        .and_then(|cfg| cfg.basic.as_ref().map(|v| v.sell_percent_on_take_profit))
        .or_else(|| {
            bot_cfg
                .exits
                .as_ref()
                .and_then(|cfg| cfg.sell_percent_on_take_profit)
        })
        .unwrap_or(100.0);
    let default_basic_sell_percent_on_stop_loss = bot_cfg
        .exits
        .as_ref()
        .and_then(|cfg| cfg.basic.as_ref().map(|v| v.sell_percent_on_stop_loss))
        .or_else(|| {
            bot_cfg
                .exits
                .as_ref()
                .and_then(|cfg| cfg.sell_percent_on_stop_loss)
        })
        .unwrap_or(100.0);
    let default_exit_plan_template = state::types::ExitPlanTemplate {
        mode: "basic".into(),
        take_profit_pct: Some(default_basic_take_profit_pct),
        stop_loss_pct: Some(default_basic_stop_loss_pct),
        sell_percent_on_take_profit: Some(default_basic_sell_percent_on_take_profit),
        sell_percent_on_stop_loss: Some(default_basic_sell_percent_on_stop_loss),
        notes: Some("Default config exit plan. OpenClaw may override per position.".into()),
        tp1_upside_pct: None,
        moonbag_target_multiple: None,
        stale_exit_after_seconds: None,
        stale_exit_requires_non_loss: None,
    };

    // Strategy catalog.
    let default_wallet_buy_strategy_id = "mirror_immediate".to_string();
    let telegram_buy_strategy_id = "mirror_immediate".to_string();
    let default_wallet_sell_strategy_id = "in_and_out".to_string();
    let telegram_sell_strategy_id = "tg_calls".to_string();
    let mut buy_strategy_sol: HashMap<String, f64> = HashMap::new();
    let mut buy_strategy_mode: HashMap<String, String> = HashMap::new();
    let mut buy_strategy_max_fill_delay_ms: HashMap<String, i64> = HashMap::new();
    let mut buy_strategy_max_price_above_alpha_pct: HashMap<String, f64> = HashMap::new();
    let mut buy_strategy_min_market_cap_usd: HashMap<String, f64> = HashMap::new();
    let mut buy_strategy_slippage_bps: HashMap<String, u64> = HashMap::new();
    let mut sell_strategy_templates: HashMap<String, state::types::ExitPlanTemplate> =
        HashMap::new();

    if bot_cfg.buy_strategies.is_empty()
        && bot_cfg.sell_strategies.is_empty()
        && bot_cfg.strategies.is_empty()
    {
        buy_strategy_sol.insert("mirror_immediate".into(), 0.8);
        buy_strategy_mode.insert("mirror_immediate".into(), "mirror_immediate".into());
        buy_strategy_max_fill_delay_ms.insert("mirror_immediate".into(), 3_000);
        buy_strategy_max_price_above_alpha_pct.insert("mirror_immediate".into(), 100.0);
        buy_strategy_min_market_cap_usd.insert("mirror_immediate".into(), 10_000.0);
        buy_strategy_slippage_bps.insert("mirror_immediate".into(), 1_000);
        buy_strategy_sol.insert("copytrade_fast".into(), 0.8);
        buy_strategy_mode.insert("copytrade_fast".into(), "copytrade_fast".into());
        buy_strategy_max_fill_delay_ms.insert("copytrade_fast".into(), 3_000);
        buy_strategy_max_price_above_alpha_pct.insert("copytrade_fast".into(), 10.0);
        buy_strategy_min_market_cap_usd.insert("copytrade_fast".into(), 10_000.0);
        buy_strategy_slippage_bps.insert("copytrade_fast".into(), 1_000);
        buy_strategy_sol.insert("buy_dip".into(), 0.8);
        buy_strategy_mode.insert("buy_dip".into(), "buy_dip".into());
        buy_strategy_min_market_cap_usd.insert("buy_dip".into(), 10_000.0);
        buy_strategy_slippage_bps.insert("buy_dip".into(), 1_000);
        sell_strategy_templates.insert(
            "in_and_out".into(),
            state::types::ExitPlanTemplate {
                mode: "in_and_out".into(),
                take_profit_pct: None,
                stop_loss_pct: None,
                sell_percent_on_take_profit: None,
                sell_percent_on_stop_loss: None,
                notes: Some("Default in_and_out strategy plan.".into()),
                tp1_upside_pct: Some(12.0),
                moonbag_target_multiple: Some(2.0),
                stale_exit_after_seconds: None,
                stale_exit_requires_non_loss: None,
            },
        );
        sell_strategy_templates.insert(
            "tg_calls".into(),
            state::types::ExitPlanTemplate {
                mode: "basic".into(),
                take_profit_pct: Some(25.0),
                stop_loss_pct: Some(40.0),
                sell_percent_on_take_profit: Some(100.0),
                sell_percent_on_stop_loss: Some(100.0),
                notes: Some("Default tg_calls strategy plan.".into()),
                tp1_upside_pct: None,
                moonbag_target_multiple: None,
                stale_exit_after_seconds: None,
                stale_exit_requires_non_loss: None,
            },
        );
    } else {
        for (strategy_id, strategy_cfg) in &bot_cfg.buy_strategies {
            buy_strategy_sol.insert(
                strategy_id.clone(),
                strategy_cfg
                    .buy_sol
                    .filter(|value| value.is_finite() && *value > 0.0)
                    .unwrap_or(bot_cfg.copytrade.fixed_buy_sol),
            );
            buy_strategy_mode.insert(strategy_id.clone(), strategy_cfg.mode.clone());
            buy_strategy_max_fill_delay_ms.insert(
                strategy_id.clone(),
                strategy_cfg.max_fill_delay_ms.unwrap_or(3_000),
            );
            buy_strategy_max_price_above_alpha_pct.insert(
                strategy_id.clone(),
                strategy_cfg.max_price_above_alpha_pct.unwrap_or(10.0),
            );
            buy_strategy_min_market_cap_usd.insert(
                strategy_id.clone(),
                strategy_cfg.min_market_cap_usd.unwrap_or(10_000.0),
            );
            buy_strategy_slippage_bps.insert(
                strategy_id.clone(),
                strategy_cfg.slippage_bps.unwrap_or(exec_cfg.slippage_bps),
            );
        }
        for (strategy_id, strategy_cfg) in &bot_cfg.sell_strategies {
            if let Some(template) = exit_template_from_sell_strategy_cfg(strategy_cfg) {
                sell_strategy_templates.insert(strategy_id.clone(), template);
            }
        }
        for (strategy_id, strategy_cfg) in &bot_cfg.strategies {
            let buy_sol = strategy_cfg
                .buy_sol
                .filter(|value| value.is_finite() && *value > 0.0)
                .unwrap_or(bot_cfg.copytrade.fixed_buy_sol);
            buy_strategy_sol.insert(strategy_id.clone(), buy_sol);
            buy_strategy_mode.insert(strategy_id.clone(), "mirror_immediate".into());
            buy_strategy_min_market_cap_usd.insert(
                strategy_id.clone(),
                f.min_market_cap_usd.unwrap_or(10_000.0),
            );
            buy_strategy_slippage_bps.insert(strategy_id.clone(), exec_cfg.slippage_bps);
            if let Some(template) = exit_template_from_strategy_cfg(strategy_cfg) {
                sell_strategy_templates.insert(strategy_id.clone(), template);
            }
        }
    }

    let filter_cfg = filters::FilterWorkerConfig {
        idle_sleep_ms: 100,
        birdeye_api_key: birdeye_api_key.clone(),
        filters: filters::FiltersConfig {
            enabled: f.enabled,
            only_buys: f.only_buys,
            allowed_venues: f.allowed_venues,
            min_notional_sol: f.min_notional_sol,
            min_market_cap_usd: f.min_market_cap_usd.unwrap_or(10_000.0),
            max_market_cap_usd: f.max_market_cap_usd.unwrap_or(0.0),
        },
        wallet_buy_strategy_id: wallet_buy_strategy_id.clone(),
        buy_strategy_min_market_cap_usd: buy_strategy_min_market_cap_usd.clone(),
        unknown_mcap_retry_attempts: 4,
        unknown_mcap_retry_delay_ms: 350,
    };
    let redis_for_filters = redis_state.clone();
    tokio::spawn(async move {
        if let Err(e) = filters::run_filter_worker(filter_cfg, redis_for_filters).await {
            warn!(error = %e, "filter worker task exited");
        }
    });

    let executor_worker_cfg = executor::ExecutorConfig {
        enabled: exec_cfg.enabled,
        jupiter_base_url: jup_base,
        jupiter_api_key: jupiter_api_key.clone(),
        birdeye_api_key: birdeye_api_key.clone(),
        slippage_bps: exec_cfg.slippage_bps,
        wrap_and_unwrap_sol: exec_cfg.wrap_and_unwrap_sol,
        min_sell_token_ui,
        sell_dust_alert_ttl_seconds,
        fixed_buy_sol: bot_cfg.copytrade.fixed_buy_sol,
        default_wallet_buy_strategy_id,
        telegram_buy_strategy_id,
        wallet_buy_strategy_id: wallet_buy_strategy_id.clone(),
        buy_strategy_sol,
        buy_strategy_mode,
        buy_strategy_max_fill_delay_ms,
        buy_strategy_max_price_above_alpha_pct,
        buy_strategy_slippage_bps,
        default_wallet_sell_strategy_id,
        telegram_sell_strategy_id,
        wallet_sell_strategy_id: wallet_sell_strategy_id.clone(),
        sell_strategy_templates: sell_strategy_templates.clone(),
        default_exit_plan_template: default_exit_plan_template.clone(),
        openclaw_event_url,
        openclaw_api_key,
        helius_api_key,
        helius_api_base_url,
        dry_run: bot_cfg.mode.dry_run,
        simulate_only: bot_cfg.mode.simulate_only,
        rpc_http_url,
        user_public_key,
        keypair_path,
        jito_bundle_endpoint,
        jito_tip_sol,
        max_priority_fee_lamports,
        idle_sleep_ms: 100,
    };

    let redis_for_exec = redis_state.clone();
    tokio::spawn(async move {
        if let Err(e) = executor::run_executor_worker(executor_worker_cfg, redis_for_exec).await {
            warn!(error = %e, "executor worker task exited");
        }
    });

    // Block 8: exit engine (poll Jupiter sell quotes and enqueue sells).
    let jup_base_for_exits = bot_cfg
        .providers
        .jupiter
        .as_ref()
        .map(|j| j.base_url.clone())
        .unwrap_or_else(|| "https://api.jup.ag".into());

    // Exit strategies are driven by per-wallet sell strategy ids.
    let default_wallet_strategy_id = "in_and_out".to_string();
    let telegram_strategy_id = "tg_calls".to_string();

    let exits_cfg = exits::ExitEngineConfig {
        enabled: true,
        jupiter_base_url: jup_base_for_exits,
        jupiter_api_key,
        slippage_bps: exec_cfg.slippage_bps,
        cadence_ms: 250,
        default_wallet_strategy_id,
        telegram_strategy_id,
        wallet_strategy_id: wallet_sell_strategy_id.clone(),
        strategy_exit_templates: sell_strategy_templates,
        default_exit_plan_template,
    };
    let redis_for_exits = redis_state.clone();
    tokio::spawn(async move {
        if let Err(e) = exits::run_exit_engine(exits_cfg, redis_for_exits).await {
            warn!(error = %e, "exit engine task exited");
        }
    });

    info!("core-app is running (streamer + classifier + filters + executor + exit-engine).");

    // Demonstrate dedupe primitive (no-op if Redis not running; will error on first use).
    // This is intentionally minimal for Block 2; Block 3 will start producing real events.
    let _ = redis_state.dedupe_signature("scaffold", 60).await;

    // Placeholder: keep process alive until ctrl-c (later we’ll run async tasks).
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    Ok(())
}
