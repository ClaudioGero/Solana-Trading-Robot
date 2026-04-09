use anyhow::{Context, Result};
use chrono::Utc;
use common::token_info::BirdeyeClient;
use serde::{Deserialize, Serialize};
use state::{
    token_cache::token_info_cached,
    types::AlertEvent,
    types::{ExecOrder, TradeIntent, TradeSide},
    RedisState,
};
use std::collections::HashMap;
use tokio::time::Duration;
use tracing::{info, warn};

/// Filter config (v1). Keep this minimal and fast.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FiltersConfig {
    /// Enable/disable filter stage (useful for debugging).
    pub enabled: bool,
    /// If true, only forward BUY intents (copytrading buys).
    pub only_buys: bool,
    /// Allowed venues (based on classifier tagging).
    /// Common: ["pumpfun", "jupiter", "unknown"]
    pub allowed_venues: Vec<String>,
    /// Minimum notional SOL (best-effort estimate from SOL delta).
    pub min_notional_sol: f64,
    /// Minimum market cap in USD required to allow BUYs.
    /// If market cap is unknown/unavailable, BUYs are rejected to enforce the rule strictly.
    pub min_market_cap_usd: f64,
    /// Maximum market cap in USD allowed to allow BUYs.
    /// If 0.0, this check is disabled.
    #[serde(default)]
    pub max_market_cap_usd: f64,
}

impl Default for FiltersConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            only_buys: true,
            allowed_venues: vec!["pumpfun".into(), "jupiter".into(), "unknown".into()],
            min_notional_sol: 0.0,
            min_market_cap_usd: 10_000.0,
            max_market_cap_usd: 0.0,
        }
    }
}

pub fn apply_filters(intent: &TradeIntent, cfg: &FiltersConfig) -> Result<Option<String>> {
    if !cfg.enabled {
        return Ok(Some("filters_disabled".into()));
    }

    if cfg.only_buys && intent.side != TradeSide::Buy {
        return Ok(None);
    }

    // Notional filter: applies to tracked alpha-wallet buys.
    // Telegram calls currently come with `notional_sol = 0.0`, so we exempt them.
    if intent.side == TradeSide::Buy
        && !intent.wallet.starts_with("telegram:")
        && cfg.min_notional_sol > 0.0
        && intent.notional_sol < cfg.min_notional_sol
    {
        return Ok(None);
    }

    let venue = intent.venue.clone().unwrap_or_else(|| "unknown".into());
    if !cfg.allowed_venues.iter().any(|v| v == &venue) {
        return Ok(None);
    }

    Ok(Some("passed".into()))
}

fn market_cap_in_bounds(mc: Option<f64>, min_market_cap_usd: f64, max_market_cap_usd: f64) -> bool {
    if min_market_cap_usd > 0.0 {
        let Some(value) = mc else {
            return false;
        };
        if value < min_market_cap_usd {
            return false;
        }
    }

    if max_market_cap_usd > 0.0 && mc.is_some_and(|value| value > max_market_cap_usd) {
        return false;
    }

    true
}

#[derive(Clone, Debug)]
pub struct FilterWorkerConfig {
    pub idle_sleep_ms: u64,
    pub birdeye_api_key: Option<String>,
    pub filters: FiltersConfig,
    pub wallet_buy_strategy_id: HashMap<String, String>,
    pub buy_strategy_min_market_cap_usd: HashMap<String, f64>,
    pub unknown_mcap_retry_attempts: usize,
    pub unknown_mcap_retry_delay_ms: u64,
}

async fn resolve_market_cap_with_retry(
    redis: &RedisState,
    birdeye: Option<&BirdeyeClient>,
    mint: &str,
    ttl_seconds: usize,
    retry_attempts: usize,
    retry_delay_ms: u64,
) -> Option<f64> {
    let mut mc = token_info_cached(redis, birdeye, mint, ttl_seconds)
        .await
        .and_then(|t| t.market_cap_usd);
    if mc.is_some() || birdeye.is_none() {
        return mc;
    }

    for _ in 0..retry_attempts {
        tokio::time::sleep(Duration::from_millis(retry_delay_ms)).await;
        mc = token_info_cached(redis, birdeye, mint, ttl_seconds)
            .await
            .and_then(|t| t.market_cap_usd);
        if mc.is_some() {
            return mc;
        }
    }

    None
}

/// Worker: pops TradeIntent from Redis, filters, pushes ExecOrder to `sb:q:exec_orders`.
pub async fn run_filter_worker(cfg: FilterWorkerConfig, redis: RedisState) -> Result<()> {
    info!(
        filters_enabled = cfg.filters.enabled,
        "filter worker started"
    );
    let birdeye = cfg
        .birdeye_api_key
        .clone()
        .and_then(|k| BirdeyeClient::new(k).ok());
    if cfg.filters.min_market_cap_usd > 0.0 && birdeye.is_none() {
        // Emit a single actionable alert because Birdeye enrichments are unavailable.
        // Market-cap resolution can still fall back to other providers.
        if redis
            .dedupe_signature("filters:missing_birdeye_api_key", 3600)
            .await
            .unwrap_or(false)
        {
            if !redis.is_bot_off().await.unwrap_or(false) {
                let alert = AlertEvent {
                    ts: Utc::now(),
                    kind: "bot_filters_misconfig".into(),
                    message: format!(
                        "CLAWDIO BOT filters warning: min mcap is enabled (>= ${:.0}) but BIRDEYE_API_KEY is missing; market-cap resolution will rely on fallback providers",
                        cfg.filters.min_market_cap_usd
                    ),
                };
                let _ = redis
                    .enqueue_alert(&serde_json::to_string(&alert).unwrap_or_default())
                    .await;
            }
        }
        warn!(
            min_market_cap_usd = cfg.filters.min_market_cap_usd,
            "min_market_cap_usd is enabled but BIRDEYE_API_KEY is missing; relying on fallback market-cap providers"
        );
    }

    loop {
        let popped = match redis.pop_trade_intent().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "redis unavailable (filters); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let Some(raw) = popped else {
            tokio::time::sleep(Duration::from_millis(cfg.idle_sleep_ms)).await;
            continue;
        };

        let intent: TradeIntent = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    raw = %raw.chars().take(240).collect::<String>(),
                    "dropping invalid TradeIntent payload"
                );
                continue;
            }
        };

        // Market cap bounds.
        //
        // Enforce MIN/MAX strictly.
        // If market cap is unknown while min mcap is enabled, reject the BUY.
        if intent.side == TradeSide::Buy {
            let buy_strategy_id = cfg
                .wallet_buy_strategy_id
                .get(&intent.wallet)
                .map(String::as_str)
                .unwrap_or("mirror_immediate");
            let min_market_cap_usd = cfg
                .buy_strategy_min_market_cap_usd
                .get(buy_strategy_id)
                .copied()
                .unwrap_or(cfg.filters.min_market_cap_usd);
            if min_market_cap_usd <= 0.0 {
                match apply_filters(&intent, &cfg.filters)? {
                    Some(reason) => {
                        let order = ExecOrder {
                            intent: intent.clone(),
                            filter_reason: reason,
                            exec_enqueued_at: Some(Utc::now()),
                        };
                        let payload = serde_json::to_string(&order)
                            .context("failed serializing ExecOrder")?;
                        redis.enqueue_exec_order(&payload).await?;
                    }
                    None => {}
                }
                continue;
            }
            let retry_attempts = cfg.unknown_mcap_retry_attempts;
            let retry_delay_ms = cfg.unknown_mcap_retry_delay_ms;
            let mc = resolve_market_cap_with_retry(
                &redis,
                birdeye.as_ref(),
                &intent.mint,
                300,
                retry_attempts,
                retry_delay_ms,
            )
            .await;
            if !market_cap_in_bounds(mc, min_market_cap_usd, cfg.filters.max_market_cap_usd) {
                if mc.is_none() {
                    let reason_key = format!("buy_mcap_unknown_rejected:{}", intent.mint);
                    if redis
                        .dedupe_signature(&reason_key, 300)
                        .await
                        .unwrap_or(false)
                    {
                        if !redis.is_bot_off().await.unwrap_or(false) {
                            let alert = AlertEvent {
                                ts: Utc::now(),
                                kind: "bot_buy_mcap_unknown_rejected".into(),
                                message: format!(
                                    "CLAWDIO BOT BUY rejected: mcap unknown for token={} while min_market_cap_usd is enabled (strategy={} retried {}x over {} ms)",
                                    intent.mint,
                                    buy_strategy_id,
                                    retry_attempts,
                                    retry_attempts as u64 * retry_delay_ms
                                ),
                            };
                            let _ = redis
                                .enqueue_alert(&serde_json::to_string(&alert).unwrap_or_default())
                                .await;
                        }
                    }
                }
                continue;
            }
        }
        match apply_filters(&intent, &cfg.filters)? {
            Some(reason) => {
                let order = ExecOrder {
                    intent: intent.clone(),
                    filter_reason: reason.clone(),
                    exec_enqueued_at: Some(Utc::now()),
                };
                let payload =
                    serde_json::to_string(&order).context("failed serializing ExecOrder")?;
                redis.enqueue_exec_order(&payload).await?;
                let obs_age_ms = intent
                    .observed_at
                    .map(|t| (Utc::now() - t).num_milliseconds())
                    .unwrap_or(-1);
                let classified_to_exec_ms = match (intent.classified_at, order.exec_enqueued_at) {
                    (Some(c), Some(e)) => (e - c).num_milliseconds(),
                    _ => -1,
                };
                info!(
                    signature = %intent.signature,
                    wallet = %intent.wallet,
                    mint = %intent.mint,
                    side = ?intent.side,
                    venue = %intent.venue.clone().unwrap_or_else(|| "none".into()),
                    notional_sol = intent.notional_sol,
                    reason = %reason,
                    obs_age_ms,
                    classified_to_exec_ms,
                    "approved exec order"
                );
            }
            None => {
                // Filtered out (intentionally quiet to avoid log spam).
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::market_cap_in_bounds;

    #[test]
    fn rejects_unknown_market_cap_when_min_enabled() {
        assert!(!market_cap_in_bounds(None, 12_000.0, 200_000.0));
    }

    #[test]
    fn rejects_below_min_market_cap() {
        assert!(!market_cap_in_bounds(Some(4_000.0), 12_000.0, 200_000.0));
    }

    #[test]
    fn rejects_above_max_market_cap() {
        assert!(!market_cap_in_bounds(Some(250_000.0), 12_000.0, 200_000.0));
    }

    #[test]
    fn accepts_in_range_market_cap() {
        assert!(market_cap_in_bounds(Some(25_000.0), 12_000.0, 200_000.0));
    }
}
