pub mod classifier;
pub mod rpc;

use anyhow::{Context, Result};
use chrono::Utc;
use common::{format::format_compact, token_info::BirdeyeClient};
use state::{
    token_cache::token_info_cached,
    types::{
        AlphaPlaybookConfirmationState, AlphaPlaybookRiskTier, AlphaPlaybookScenario,
        AlphaPlaybookTokenState, TradeIntent, TradeSide, WalletEvent,
    },
    RedisState,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio::time::Duration;
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct ClassifierWorkerConfig {
    pub rpc_http_url: String,
    pub birdeye_api_key: Option<String>,
    pub idle_sleep_ms: u64,
    pub max_retries: usize,
    /// Suppress alpha buy/sell alerts when Birdeye mcap is known and below this threshold.
    /// (We still process intents/signals; this is only to reduce alert spam.)
    pub min_alert_market_cap_usd: f64,
    /// Suppress alpha buy/sell alerts when Birdeye mcap is known and above this threshold.
    /// If 0.0, this check is disabled.
    pub max_alert_market_cap_usd: f64,
}

pub async fn run_classifier_worker(cfg: ClassifierWorkerConfig, redis: RedisState) -> Result<()> {
    let rpc = rpc::RpcClient::new(cfg.rpc_http_url.clone())?;
    // Optional "fast" RPC for quicker reaction time:
    // - We try getTransaction at `processed` first (many non-Helius RPCs support it).
    // - If it returns null / errors, we fall back to the existing confirmed call (Helius-friendly).
    //
    // Set via env: CLASSIFIER_FAST_RPC_HTTP_URL
    let rpc_fast = std::env::var("CLASSIFIER_FAST_RPC_HTTP_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .and_then(|u| rpc::RpcClient::new(u).ok());
    let birdeye = cfg
        .birdeye_api_key
        .clone()
        .and_then(|k| BirdeyeClient::new(k).ok());
    info!(
        rpc_http_url = %cfg.rpc_http_url,
        fast_rpc_enabled = rpc_fast.is_some(),
        "classifier worker started"
    );

    // Network-bound stage; allow bounded parallelism to avoid queue backlogs.
    let classifier_concurrency: usize = std::env::var("CLASSIFIER_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(6);
    let sem = Arc::new(Semaphore::new(classifier_concurrency));
    let max_retries = cfg.max_retries;
    let min_alert_market_cap_usd = cfg.min_alert_market_cap_usd;
    let max_alert_market_cap_usd = cfg.max_alert_market_cap_usd;
    let trace = std::env::var("CLASSIFIER_TRACE")
        .ok()
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);

    loop {
        let popped = match redis.pop_wallet_event().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (classifier); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let Some(raw) = popped else {
            tokio::time::sleep(Duration::from_millis(cfg.idle_sleep_ms)).await;
            continue;
        };

        let evt: WalletEvent =
            serde_json::from_str(&raw).context("failed parsing WalletEvent from redis")?;

        // Process in background with bounded parallelism.
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
        let rpc = rpc.clone();
        let rpc_fast = rpc_fast.clone();
        let redis = redis.clone();
        let birdeye = birdeye.clone();
        let trace = trace;
        let min_alert_market_cap_usd = min_alert_market_cap_usd;
        let max_alert_market_cap_usd = max_alert_market_cap_usd;

        tokio::spawn(async move {
            let _permit = permit;
            let stage_start = Instant::now();
            let mut last_err: Option<anyhow::Error> = None;
            let mut resp: Option<serde_json::Value> = None;

            if trace {
                info!(
                    signature = %evt.signature,
                    wallet = %evt.wallet,
                    label = %evt.wallet_label.clone().unwrap_or_else(|| "none".into()),
                    age_ms = (Utc::now() - evt.observed_at).num_milliseconds(),
                    "classifier popped wallet event"
                );
            }

            for attempt in 0..=max_retries {
                // First try the fast path (processed commitment) if configured.
                if let Some(rpc_fast) = rpc_fast.as_ref() {
                    match rpc_fast
                        .get_transaction_with_commitment(&evt.signature, "processed")
                        .await
                    {
                        Ok(v) if v.get("result").is_some_and(|r| !r.is_null()) => {
                            resp = Some(v);
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => last_err = Some(e),
                    }
                }

                // If fast RPC is configured, wait briefly and retry processed before falling back to confirmed.
                if rpc_fast.is_some() && attempt < max_retries {
                    sleep(Duration::from_millis(
                        120u64.saturating_mul((attempt as u64) + 1),
                    ))
                    .await;
                    continue;
                }

                match rpc.get_transaction(&evt.signature).await {
                    Ok(v) => {
                        if v.get("result").is_some_and(|r| r.is_null()) {
                            if trace {
                                info!(
                                    signature = %evt.signature,
                                    attempt,
                                    "getTransaction returned null result (confirmed)"
                                );
                            }
                            sleep(Duration::from_millis(
                                200u64.saturating_mul((attempt as u64) + 1),
                            ))
                            .await;
                            continue;
                        }
                        resp = Some(v);
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        sleep(Duration::from_millis(
                            200u64.saturating_mul((attempt as u64) + 1),
                        ))
                        .await;
                    }
                }
            }

            let Some(txn_resp) = resp else {
                warn!(
                    signature = %evt.signature,
                    error = %last_err.map(|e| e.to_string()).unwrap_or_else(|| "unknown".into()),
                    "getTransaction failed/null; dropping event (v1)"
                );
                return;
            };

            match classifier::classify_from_get_transaction(&evt, &txn_resp) {
                Ok(Some(intent)) => {
                    let payload = match serde_json::to_string(&intent) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(signature = %evt.signature, error = %e, "failed serializing TradeIntent");
                            return;
                        }
                    };
                    if let Err(e) = redis.enqueue_trade_intent(&payload).await {
                        warn!(signature = %evt.signature, error = %e, "failed enqueuing TradeIntent");
                        return;
                    }

                    if let Err(e) =
                        update_alpha_playbook_tracking(&redis, birdeye.as_ref(), &intent).await
                    {
                        warn!(
                            signature = %intent.signature,
                            mint = %intent.mint,
                            error = %e,
                            "failed updating alpha_playbook tracking"
                        );
                    }

                    // Push alpha SELL signals to a dedicated queue so the exit engine can react fast.
                    if intent.side == state::types::TradeSide::Sell {
                        let _ = redis.enqueue_alpha_sell_signal(&payload).await;
                    }

                    let now = Utc::now();
                    let obs_age_ms = (now - evt.observed_at).num_milliseconds();
                    let classify_loop_ms = stage_start.elapsed().as_millis() as i64;

                    info!(
                        signature = %intent.signature,
                        wallet = %intent.wallet,
                        mint = %intent.mint,
                        side = ?intent.side,
                        venue = %intent.venue.clone().unwrap_or_else(|| "none".into()),
                        notional_sol = intent.notional_sol,
                        obs_age_ms,
                        classify_loop_ms,
                        "enqueued trade intent"
                    );

                    // Enqueue Telegram alert enrichment off the hot path (Birdeye can be slow/rate-limited).
                    let redis_for_alert = redis.clone();
                    let who = evt
                        .wallet_label
                        .clone()
                        .unwrap_or_else(|| evt.wallet.clone());
                    let intent_side = intent.side;
                    let intent_mint = intent.mint.clone();
                    let intent_notional = intent.notional_sol;
                    tokio::spawn(async move {
                        let side_word = match intent_side {
                            state::types::TradeSide::Buy => "bought",
                            state::types::TradeSide::Sell => "sold",
                        };
                        let kind = match intent_side {
                            state::types::TradeSide::Buy => "alpha_buy",
                            state::types::TradeSide::Sell => "alpha_sell",
                        };
                        let ti = token_info_cached(
                            &redis_for_alert,
                            birdeye.as_ref(),
                            &intent_mint,
                            300,
                        )
                        .await;
                        let mc = ti.as_ref().and_then(|t| t.market_cap_usd);
                        // Reduce spam: if we can see mcap is below threshold, don't alert at all.
                        if mc.is_some_and(|v| v < min_alert_market_cap_usd) {
                            return;
                        }
                        // Reduce spam: if we can see mcap is above max threshold, don't alert at all.
                        if max_alert_market_cap_usd > 0.0
                            && mc.is_some_and(|v| v > max_alert_market_cap_usd)
                        {
                            return;
                        }
                        let mcap_line = match mc {
                            Some(v) => format!(" at ${} mcap", format_compact(v)),
                            None => "".into(),
                        };
                        let msg = format!(
                            "{} {} {:.4} SOL of {}{}",
                            who, side_word, intent_notional, intent_mint, mcap_line
                        );
                        let alert = state::types::AlertEvent {
                            ts: Utc::now(),
                            kind: kind.into(),
                            message: msg,
                        };
                        let _ = enqueue_alert_if_bot_active(&redis_for_alert, &alert).await;
                    });
                }
                Ok(None) => {
                    // Important visibility: if we keep getting here, the bot looks "dead" even though
                    // it is consuming wallet events. Emit a trace log showing whether the tx had token deltas.
                    let has_token_delta = txn_has_token_delta_for_wallet(&txn_resp, &evt.wallet);
                    if trace {
                        info!(
                            signature = %evt.signature,
                            wallet = %evt.wallet,
                            has_token_delta,
                            "classifier produced no TradeIntent"
                        );
                    }
                    if !has_token_delta {
                        return;
                    }
                    if !redis
                        .try_rate_limit_alpha_unclassified(&evt.wallet, 60)
                        .await
                        .unwrap_or(false)
                    {
                        return;
                    }

                    let who = evt
                        .wallet_label
                        .clone()
                        .unwrap_or_else(|| evt.wallet.clone());
                    let alert = state::types::AlertEvent {
                        ts: Utc::now(),
                        kind: "alpha_unclassified".into(),
                        message: format!(
                            "{} did an unclassified token tx\nslot: {}",
                            who, evt.slot
                        ),
                    };
                    let _ = enqueue_alert_if_bot_active(&redis, &alert).await;
                }
                Err(e) => {
                    warn!(signature = %evt.signature, error = %e, "classification failed");
                }
            }
        });
    }
}

async fn enqueue_alert_if_bot_active(
    redis: &RedisState,
    alert: &state::types::AlertEvent,
) -> Result<()> {
    if redis.is_bot_off().await.unwrap_or(false) {
        return Ok(());
    }
    redis.enqueue_alert(&serde_json::to_string(alert)?).await
}

async fn update_alpha_playbook_tracking(
    redis: &RedisState,
    birdeye: Option<&BirdeyeClient>,
    intent: &TradeIntent,
) -> Result<()> {
    if intent.wallet.starts_with("telegram:") {
        return Ok(());
    }

    let event_at = intent
        .observed_at
        .or(intent.classified_at)
        .unwrap_or(intent.created_at);

    let mut token_state = redis
        .get_alpha_playbook_token_state(&intent.mint)
        .await?
        .unwrap_or(AlphaPlaybookTokenState {
            mint: intent.mint.clone(),
            scenario: None,
            chosen_playbook: None,
            chosen_size_sol: None,
            first_wallet: None,
            first_buy_signature: None,
            first_buy_at: None,
            latest_wallet: None,
            latest_event_at: None,
            confirmation_wallets: Vec::new(),
            confirmation_triggered: false,
            entry_executed: false,
            entry_executed_at: None,
            confirmation_entry_executed: false,
            confirmation_entry_executed_at: None,
            cooldown_active: false,
            cooldown_expires_at: None,
            watch_active: false,
            token_age_seconds: None,
            token_age_bucket: None,
            market_cap_usd: None,
            peak_market_cap_usd: None,
            volume_5m_usd: None,
            volume_30m_usd: None,
            recent_activity_bucket: None,
            risk_tier: None,
            updated_at: event_at,
            created_at: event_at,
        });

    let mut confirmation = redis
        .get_alpha_playbook_confirmation_state(&intent.mint)
        .await?
        .unwrap_or(AlphaPlaybookConfirmationState {
            mint: intent.mint.clone(),
            first_wallet: None,
            first_buy_at: None,
            wallets: Vec::new(),
            last_confirmed_wallet: None,
            confirmation_triggered: false,
            confirmation_triggered_at: None,
            updated_at: event_at,
            created_at: event_at,
        });

    token_state.latest_wallet = Some(intent.wallet.clone());
    token_state.latest_event_at = Some(event_at);
    token_state.updated_at = event_at;
    confirmation.updated_at = event_at;

    if intent.side == TradeSide::Buy {
        if token_state.first_wallet.is_none() {
            token_state.first_wallet = Some(intent.wallet.clone());
            token_state.first_buy_signature = Some(intent.signature.clone());
            token_state.first_buy_at = Some(event_at);
        }

        if confirmation.first_wallet.is_none() {
            confirmation.first_wallet = Some(intent.wallet.clone());
            confirmation.first_buy_at = Some(event_at);
        }

        if !confirmation.wallets.iter().any(|wallet| wallet == &intent.wallet) {
            confirmation.wallets.push(intent.wallet.clone());
        }

        let is_distinct_confirm_wallet = confirmation
            .first_wallet
            .as_ref()
            .is_some_and(|wallet| wallet != &intent.wallet);
        let meets_confirmation_delay = confirmation
            .first_buy_at
            .map(|first_buy_at| (event_at - first_buy_at).num_seconds() >= 10)
            .unwrap_or(false);

        if is_distinct_confirm_wallet && meets_confirmation_delay {
            confirmation.last_confirmed_wallet = Some(intent.wallet.clone());
            confirmation.confirmation_triggered = true;
            confirmation.confirmation_triggered_at = Some(event_at);
            token_state.confirmation_triggered = true;
        }

        token_state.confirmation_wallets = confirmation.wallets.clone();

        if let Some(token_info) = token_info_cached(redis, birdeye, &intent.mint, 300).await {
            let age_seconds = token_info
                .pair_created_at
                .map(|created_at| (event_at - created_at).num_seconds())
                .filter(|seconds| *seconds >= 0);
            let inferred_volume_30m_usd = token_info.volume_1h_usd.map(|volume| volume / 2.0);
            token_state.market_cap_usd = token_info.market_cap_usd;
            token_state.peak_market_cap_usd = match (
                token_state.peak_market_cap_usd,
                token_info.market_cap_usd,
            ) {
                (Some(existing), Some(current)) => Some(existing.max(current)),
                (None, Some(current)) => Some(current),
                (existing, None) => existing,
            };
            token_state.volume_5m_usd = token_info.volume_5m_usd;
            token_state.volume_30m_usd = inferred_volume_30m_usd;
            token_state.token_age_seconds = age_seconds;
            token_state.token_age_bucket = age_seconds.map(token_age_bucket);
            token_state.recent_activity_bucket = Some(activity_bucket(
                token_info.volume_5m_usd,
                inferred_volume_30m_usd,
                token_info.liquidity_usd,
            ));

            if let Some((scenario, risk_tier)) = classify_alpha_playbook_scenario(
                age_seconds,
                token_info.market_cap_usd,
                token_info.volume_5m_usd,
                inferred_volume_30m_usd,
                token_info.liquidity_usd,
            ) {
                token_state.scenario = Some(scenario);
                token_state.risk_tier = Some(risk_tier);
            }
        }
    }

    if intent.side == TradeSide::Sell && token_state.first_wallet.is_none() {
        token_state.first_wallet = Some(intent.wallet.clone());
    }

    redis.set_alpha_playbook_token_state(&token_state).await?;
    redis
        .set_alpha_playbook_confirmation_state(&confirmation)
        .await?;
    Ok(())
}

fn classify_alpha_playbook_scenario(
    age_seconds: Option<i64>,
    market_cap_usd: Option<f64>,
    volume_5m_usd: Option<f64>,
    volume_30m_usd: Option<f64>,
    liquidity_usd: Option<f64>,
) -> Option<(AlphaPlaybookScenario, AlphaPlaybookRiskTier)> {
    let age_seconds = age_seconds?;
    let market_cap_usd = market_cap_usd?;
    let volume_5m_usd = volume_5m_usd.unwrap_or(0.0);
    let volume_30m_usd = volume_30m_usd.unwrap_or(0.0);
    let liquidity_usd = liquidity_usd.unwrap_or(0.0);

    if age_seconds <= 600 && market_cap_usd < 30_000.0 && volume_5m_usd >= 20_000.0 {
        return Some((AlphaPlaybookScenario::NewHot, AlphaPlaybookRiskTier::High));
    }

    if age_seconds > 7_200
        && market_cap_usd < 13_000.0
        && volume_30m_usd <= 20_000.0
        && volume_5m_usd <= 5_000.0
    {
        return Some((
            AlphaPlaybookScenario::OldDormantSpiked,
            AlphaPlaybookRiskTier::High,
        ));
    }

    let active_enough = volume_5m_usd >= 5_000.0 || volume_30m_usd >= 20_000.0 || liquidity_usd >= 10_000.0;
    if age_seconds > 600
        && market_cap_usd > 12_000.0
        && market_cap_usd <= 150_000.0
        && active_enough
    {
        return Some((AlphaPlaybookScenario::MidTrend, AlphaPlaybookRiskTier::Medium));
    }

    None
}

fn token_age_bucket(age_seconds: i64) -> String {
    if age_seconds <= 600 {
        "lte_10m".into()
    } else if age_seconds <= 7_200 {
        "10m_to_2h".into()
    } else {
        "gt_2h".into()
    }
}

fn activity_bucket(
    volume_5m_usd: Option<f64>,
    volume_30m_usd: Option<f64>,
    liquidity_usd: Option<f64>,
) -> String {
    let volume_5m_usd = volume_5m_usd.unwrap_or(0.0);
    let volume_30m_usd = volume_30m_usd.unwrap_or(0.0);
    let liquidity_usd = liquidity_usd.unwrap_or(0.0);

    if volume_5m_usd >= 20_000.0 || volume_30m_usd >= 40_000.0 {
        "hot".into()
    } else if volume_5m_usd >= 5_000.0 || volume_30m_usd >= 20_000.0 || liquidity_usd >= 10_000.0
    {
        "active".into()
    } else {
        "dormant".into()
    }
}

fn txn_has_token_delta_for_wallet(txn_resp: &serde_json::Value, wallet: &str) -> bool {
    let result = match txn_resp.get("result") {
        Some(r) if !r.is_null() => r,
        _ => return false,
    };
    let meta = match result.get("meta") {
        Some(m) if !m.is_null() => m,
        _ => return false,
    };
    let pre = meta
        .get("preTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let post = meta
        .get("postTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // If there are any token balance entries owned by wallet, we consider it token-related.
    pre.iter()
        .chain(post.iter())
        .any(|e| e.get("owner").and_then(|o| o.as_str()) == Some(wallet))
}

#[cfg(test)]
mod tests {
    use super::{
        activity_bucket, classify_alpha_playbook_scenario, token_age_bucket,
        txn_has_token_delta_for_wallet,
    };
    use serde_json::json;
    use state::types::{AlphaPlaybookRiskTier, AlphaPlaybookScenario};

    #[test]
    fn detects_token_delta_for_wallet() {
        let txn = json!({
            "result": {
                "meta": {
                    "preTokenBalances": [
                        { "owner": "wallet-a" }
                    ],
                    "postTokenBalances": []
                }
            }
        });
        assert!(txn_has_token_delta_for_wallet(&txn, "wallet-a"));
        assert!(!txn_has_token_delta_for_wallet(&txn, "wallet-b"));
    }

    #[test]
    fn classifies_new_hot_tokens() {
        let scenario =
            classify_alpha_playbook_scenario(Some(300), Some(25_000.0), Some(25_000.0), Some(30_000.0), Some(8_000.0));
        assert_eq!(
            scenario,
            Some((AlphaPlaybookScenario::NewHot, AlphaPlaybookRiskTier::High))
        );
    }

    #[test]
    fn classifies_old_dormant_spiked_tokens() {
        let scenario =
            classify_alpha_playbook_scenario(Some(9_000), Some(12_500.0), Some(3_000.0), Some(12_000.0), Some(4_000.0));
        assert_eq!(
            scenario,
            Some((
                AlphaPlaybookScenario::OldDormantSpiked,
                AlphaPlaybookRiskTier::High
            ))
        );
    }

    #[test]
    fn classifies_mid_trend_tokens() {
        let scenario =
            classify_alpha_playbook_scenario(Some(1_200), Some(45_000.0), Some(6_000.0), Some(24_000.0), Some(15_000.0));
        assert_eq!(
            scenario,
            Some((AlphaPlaybookScenario::MidTrend, AlphaPlaybookRiskTier::Medium))
        );
    }

    #[test]
    fn computes_buckets() {
        assert_eq!(token_age_bucket(120), "lte_10m");
        assert_eq!(token_age_bucket(3600), "10m_to_2h");
        assert_eq!(token_age_bucket(9000), "gt_2h");
        assert_eq!(activity_bucket(Some(25_000.0), Some(30_000.0), Some(5_000.0)), "hot");
        assert_eq!(activity_bucket(Some(6_000.0), Some(12_000.0), Some(12_000.0)), "active");
        assert_eq!(activity_bucket(Some(500.0), Some(2_000.0), Some(1_000.0)), "dormant");
    }
}
