use anyhow::Result;
use chrono::Utc;
use common::jupiter::JupiterClient;
use state::{
    token_cache::token_info_cached,
    types::{ExitPlanTemplate, Position, PositionExitPlan, TradeIntent, TradeSide},
    RedisState,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const DEFAULT_IN_AND_OUT_TP1_UPSIDE_PCT: f64 = 12.0;
const DEFAULT_IN_AND_OUT_MOONBAG_TARGET_MULTIPLE: f64 = 2.0;
const ALPHA_PLAYBOOK_FAST_FLIP_WINDOW_SECONDS: i64 = 20;
const ALPHA_PLAYBOOK_COOLDOWN_MINUTES: i64 = 15;

#[derive(Debug, Clone)]
pub struct ExitEngineConfig {
    pub enabled: bool,
    pub jupiter_base_url: String,
    pub jupiter_api_key: Option<String>,
    pub slippage_bps: u64,
    pub cadence_ms: u64, // e.g. 500ms => 2/sec

    /// Default strategy used for alpha-wallet copytrades when no per-wallet strategy is set.
    pub default_wallet_strategy_id: String,
    /// Strategy used for Telegram calls (wallets like "telegram:<channel>").
    pub telegram_strategy_id: String,
    /// Per-wallet strategy ids keyed by alpha wallet address.
    pub wallet_strategy_id: HashMap<String, String>,

    /// Strategy exit templates keyed by strategy id.
    pub strategy_exit_templates: HashMap<String, ExitPlanTemplate>,
    pub default_exit_plan_template: ExitPlanTemplate,
}

// Task 5: per-position quote throttling + failure backoff.
#[derive(Default, Clone)]
struct PosQuoteState {
    failures: u32,
    next_allowed_at: chrono::DateTime<chrono::Utc>,
    last_alert_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn run_exit_engine(cfg: ExitEngineConfig, redis: RedisState) -> Result<()> {
    if !cfg.enabled {
        info!("exit engine disabled; idling");
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }

    let jup = JupiterClient::new(
        cfg.jupiter_base_url.clone(),
        cfg.jupiter_api_key.clone(),
        Duration::from_secs(10),
    )?;
    info!(
        cadence_ms = cfg.cadence_ms,
        "exit engine started (strategy exits)"
    );

    let mut pos_state: std::collections::HashMap<String, PosQuoteState> =
        std::collections::HashMap::new();

    loop {
        // React to tracked-wallet full exits only for strategies that opt into it.
        for _ in 0..20 {
            let raw = match redis.pop_alpha_sell_signal().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "redis unavailable (alpha sell signals); retrying");
                    sleep(Duration::from_millis(500)).await;
                    break;
                }
            };
            let Some(raw) = raw else { break };
            let sig_intent: TradeIntent = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if sig_intent.side != TradeSide::Sell {
                continue;
            }
            if !sig_intent.source_wallet_exit_full {
                let sold_pct = sig_intent.source_wallet_sold_pct.unwrap_or(0.0);
                let token_state = redis
                    .get_alpha_playbook_token_state(&sig_intent.mint)
                    .await
                    .ok()
                    .flatten();
                let is_fast_flip = token_state.as_ref().is_some_and(|state| {
                    state.first_wallet.as_deref() == Some(sig_intent.wallet.as_str())
                        && state
                            .first_buy_at
                            .is_some_and(|first_buy_at| {
                                (Utc::now() - first_buy_at).num_seconds()
                                    <= ALPHA_PLAYBOOK_FAST_FLIP_WINDOW_SECONDS
                            })
                        && sold_pct > 50.0
                });
                if is_fast_flip {
                    if let Some(mut state) = token_state {
                        state.cooldown_active = true;
                        state.cooldown_expires_at =
                            Some(Utc::now() + chrono::Duration::minutes(ALPHA_PLAYBOOK_COOLDOWN_MINUTES));
                        state.watch_active = false;
                        state.updated_at = Utc::now();
                        let _ = redis.set_alpha_playbook_token_state(&state).await;
                        let cooldown = state::types::AlphaPlaybookCooldownState {
                            mint: state.mint.clone(),
                            active: true,
                            reason: Some("fast_flip_gt_50pct_under_20s".into()),
                            triggered_by_wallet: Some(sig_intent.wallet.clone()),
                            triggered_at: Some(Utc::now()),
                            expires_at: state.cooldown_expires_at,
                            updated_at: Utc::now(),
                            created_at: Utc::now(),
                        };
                        let _ = redis.set_alpha_playbook_cooldown_state(&cooldown).await;
                    }
                    let _ = redis.clear_alpha_playbook_watch_state(&sig_intent.mint).await;
                    if let Ok(setup_ids) = redis.list_active_buy_dip_setup_ids().await {
                        for setup_id in setup_ids {
                            let Some(mut setup) = redis.get_buy_dip_setup(&setup_id).await.ok().flatten() else {
                                continue;
                            };
                            if setup.mint != sig_intent.mint {
                                continue;
                            }
                            setup.cancelled = true;
                            setup.cancel_reason = Some("fast_flip_cooldown".into());
                            setup.updated_at = Utc::now();
                            let _ = redis.upsert_buy_dip_setup(&setup).await;
                        }
                    }
                    if let Ok(setup_ids) = redis.list_active_alpha_playbook_add_setup_ids().await {
                        for setup_id in setup_ids {
                            let Some(mut setup) = redis.get_alpha_playbook_add_setup(&setup_id).await.ok().flatten() else {
                                continue;
                            };
                            if setup.mint != sig_intent.mint {
                                continue;
                            }
                            setup.cancelled = true;
                            setup.cancel_reason = Some("fast_flip_cooldown".into());
                            setup.updated_at = Utc::now();
                            let _ = redis.upsert_alpha_playbook_add_setup(&setup).await;
                        }
                    }

                    let pos_ids = match redis.list_open_position_ids().await {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    for pos_id in pos_ids {
                        let pos = match redis.get_position(&pos_id).await {
                            Ok(Some(p)) => p,
                            _ => continue,
                        };
                        if pos.mint != sig_intent.mint {
                            continue;
                        }
                        let locked = redis
                            .get_flag(&state::keys::Keys::inflight_sell(&pos.id))
                            .await
                            .unwrap_or(false);
                        if locked {
                            continue;
                        }
                        let intent = TradeIntent {
                            signature: format!("fast_flip:{}:{}", pos.id, Uuid::new_v4()),
                            slot: 0,
                            wallet: pos.wallet.clone(),
                            side: TradeSide::Sell,
                            mint: pos.mint.clone(),
                            notional_sol: sig_intent.notional_sol,
                            venue: Some("alpha_fast_flip_exit".into()),
                            observed_at: Some(Utc::now()),
                            classified_at: Some(Utc::now()),
                            amount_in_base_units: Some(pos.token_amount.clone()),
                            token_delta_base_units: None,
                            requested_buy_sol: None,
                            source_wallet_exit_full: true,
                            source_wallet_sold_pct: sig_intent.source_wallet_sold_pct,
                            created_at: Utc::now(),
                        };
                        let order = serde_json::json!({
                            "intent": intent,
                            "filter_reason": "alpha_fast_flip_exit",
                        });
                        let _ = redis.enqueue_exec_order(&order.to_string()).await;
                    }
                    continue;
                }
                continue;
            }

            if let Ok(setup_ids) = redis.list_active_buy_dip_setup_ids().await {
                for setup_id in setup_ids {
                    let Some(mut setup) = redis.get_buy_dip_setup(&setup_id).await.ok().flatten()
                    else {
                        continue;
                    };
                    if setup.wallet != sig_intent.wallet || setup.mint != sig_intent.mint {
                        continue;
                    }
                    setup.cancelled = true;
                    setup.cancel_reason = Some("tracked_wallet_full_exit".into());
                    setup.updated_at = Utc::now();
                    let _ = redis.upsert_buy_dip_setup(&setup).await;
                    let alert = state::types::AlertEvent {
                        ts: Utc::now(),
                        kind: "buy_dip_cancelled_alpha_exit".into(),
                        message: format!(
                            "CLAWDIO BOT BUY DIP cancelled\nToken: {}\nReason: tracked wallet fully exited",
                            setup.mint
                        ),
                    };
                    let _ = redis
                        .enqueue_alert(&serde_json::to_string(&alert).unwrap_or_default())
                        .await;
                }
            }

            let pos_ids = match redis.list_open_position_ids().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "redis unavailable (alpha sell reaction); retrying");
                    break;
                }
            };
            for pos_id in pos_ids {
                let pos = match redis.get_position(&pos_id).await {
                    Ok(Some(p)) => p,
                    Ok(None) => continue,
                    Err(e) => {
                        warn!(error = %e, pos_id = %pos_id, "failed loading position for alpha sell reaction");
                        continue;
                    }
                };
                if pos.wallet != sig_intent.wallet || pos.mint != sig_intent.mint {
                    continue;
                }

                let strategy_id = pos
                    .sell_strategy_id
                    .clone()
                    .or_else(|| pos.strategy_id.clone())
                    .clone()
                    .or_else(|| cfg.wallet_strategy_id.get(&pos.wallet).cloned())
                    .unwrap_or_else(|| cfg.default_wallet_strategy_id.clone());
                let reacts_to_alpha_exit = cfg
                    .strategy_exit_templates
                    .get(&strategy_id)
                    .map(|template| template.mode.as_str() == "in_and_out")
                    .unwrap_or(strategy_id == "in_and_out")
                    || strategy_id == "sell_dip_a";
                if !reacts_to_alpha_exit {
                    continue;
                }

                let locked = redis
                    .get_flag(&state::keys::Keys::inflight_sell(&pos.id))
                    .await
                    .unwrap_or(false);
                if locked {
                    continue;
                }

                let intent = TradeIntent {
                    signature: format!("alpha_exit:{}:{}", pos.id, Uuid::new_v4()),
                    slot: 0,
                    wallet: pos.wallet.clone(),
                    side: TradeSide::Sell,
                    mint: pos.mint.clone(),
                    notional_sol: sig_intent.notional_sol,
                    venue: Some("alpha_full_exit".into()),
                    observed_at: Some(Utc::now()),
                    classified_at: Some(Utc::now()),
                    amount_in_base_units: Some(pos.token_amount.clone()),
                    token_delta_base_units: None,
                    requested_buy_sol: None,
                    source_wallet_exit_full: true,
                    source_wallet_sold_pct: sig_intent.source_wallet_sold_pct,
                    created_at: Utc::now(),
                };

                let order = serde_json::json!({
                    "intent": intent,
                    "filter_reason": "alpha_full_exit",
                });
                redis.enqueue_exec_order(&order.to_string()).await?;

                info!(
                    pos_id = %pos.id,
                    wallet = %pos.wallet,
                    mint = %pos.mint,
                    strategy_id = %strategy_id,
                    "tracked wallet fully exited token; enqueued full sell"
                );
            }
        }

        for _ in 0..20 {
            let raw = match redis.pop_position_exit_plan_update().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "redis unavailable (position exit plan updates); retrying");
                    sleep(Duration::from_millis(500)).await;
                    break;
                }
            };
            let Some(raw) = raw else { break };
            let plan: PositionExitPlan = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, payload = %raw, "invalid position exit plan update");
                    continue;
                }
            };
            if let Err(e) = redis.set_position_exit_plan(&plan).await {
                warn!(error = %e, pos_id = %plan.position_id, "failed storing position exit plan update");
                continue;
            }
            let alert = state::types::AlertEvent {
                ts: Utc::now(),
                kind: "exit_plan_updated".into(),
                message: format!(
                    "EXIT plan updated\npos_id: {}\nmode: {}\nsource: {}",
                    plan.position_id, plan.mode, plan.source
                ),
            };
            let _ = redis
                .enqueue_alert(&serde_json::to_string(&alert).unwrap_or_default())
                .await;
        }

        let ids = match redis.list_open_position_ids().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (exits); retrying");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        for id in ids {
            // Task 5: per-position cadence/backoff.
            let now = Utc::now();
            let st = pos_state
                .entry(id.clone())
                .or_insert_with(|| PosQuoteState {
                    failures: 0,
                    next_allowed_at: now,
                    last_alert_at: None,
                });
            if now < st.next_allowed_at {
                continue;
            }

            let pos = match redis.get_position(&id).await {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, "redis unavailable (exits get_position); retrying");
                    sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            if pos.token_amount.is_empty() {
                continue;
            }

            let plan = match resolve_exit_plan(&redis, &cfg, &pos).await {
                Ok(Some(plan)) => plan,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, pos_id = %pos.id, "failed resolving exit plan");
                    continue;
                }
            };

            let quote_ctx = match build_quote_context(&redis, &pos, &plan).await {
                Ok(Some(ctx)) => ctx,
                Ok(None) => continue,
                Err(e) => {
                    warn!(error = %e, pos_id = %pos.id, "failed building exit quote context");
                    continue;
                }
            };

            // Quote selling token amount back to SOL (wSOL).
            let quote = match jup
                .quote(
                    &pos.mint,
                    WSOL_MINT,
                    &quote_ctx.quote_amount_in_base_units,
                    cfg.slippage_bps,
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    st.failures = st.failures.saturating_add(1);
                    let backoff_ms = backoff_ms(st.failures, cfg.cadence_ms);
                    st.next_allowed_at = now + chrono::Duration::milliseconds(backoff_ms);
                    warn!(pos_id = %pos.id, mint = %pos.mint, failures = st.failures, backoff_ms, error = %e, "sell quote failed");
                    maybe_alert_quote_fail(&redis, &pos.id, &pos.mint, st).await;
                    continue;
                }
            };

            let out_amount = quote
                .get("outAmount")
                .and_then(|v| v.as_str())
                .unwrap_or("0");
            let out_lamports = out_amount.parse::<u64>().unwrap_or(0);
            let expected_out_sol = (out_lamports as f64) / 1_000_000_000.0;

            // Task 5: treat outAmount=0 as failure (often "no route" or malformed).
            if out_lamports == 0 {
                st.failures = st.failures.saturating_add(1);
                let backoff_ms = backoff_ms(st.failures, cfg.cadence_ms);
                st.next_allowed_at = now + chrono::Duration::milliseconds(backoff_ms);
                warn!(pos_id = %pos.id, mint = %pos.mint, failures = st.failures, backoff_ms, "sell quote outAmount=0");
                maybe_alert_quote_fail(&redis, &pos.id, &pos.mint, st).await;
                continue;
            }

            // Quote succeeded; reset failures and enforce normal cadence.
            st.failures = 0;
            st.next_allowed_at = now + chrono::Duration::milliseconds(cfg.cadence_ms as i64);

            let mut trigger: Option<String> = if plan.mode == "in_and_out" {
                // Recover principal once the partial quote gets us back to entry,
                // then let the moonbag exit only when that remainder reaches a 2x-equivalent target.
                let stage = quote_ctx.stage.clone().unwrap_or_else(|| "new".into());
                if stage == "runner" {
                    let exit_state = redis.get_exit_state(&pos.id).await.unwrap_or_default();
                    let runner_target_sol = exit_state
                        .get("runner_target_sol")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if runner_target_sol > 0.0 && expected_out_sol >= runner_target_sol {
                        Some("runner_tp".into())
                    } else {
                        None
                    }
                } else {
                    // stage "new": quote only the amount needed to recover principal at +12%.
                    if expected_out_sol >= pos.size_sol {
                        Some("tp1".into())
                    } else {
                        None
                    }
                }
            } else if plan.mode == "alpha_playbook_new_hot" {
                let stage = quote_ctx.stage.clone().unwrap_or_else(|| "new".into());
                if stage == "runner" {
                    let exit_state = redis.get_exit_state(&pos.id).await.unwrap_or_default();
                    let clip_count = exit_state
                        .get("clip_count")
                        .and_then(|value| value.parse::<u32>().ok())
                        .unwrap_or(0);
                    let clip_anchor_out_sol = exit_state
                        .get("clip_anchor_out_sol")
                        .and_then(|value| value.parse::<f64>().ok())
                        .unwrap_or(expected_out_sol);
                    let next_clip_at = exit_state
                        .get("next_clip_at")
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                        .map(|value| value.with_timezone(&Utc));
                    if clip_count < 4
                        && (next_clip_at.is_some_and(|deadline| Utc::now() >= deadline)
                            || expected_out_sol >= (clip_anchor_out_sol * 1.2))
                    {
                        Some("clip".into())
                    } else if plan.stop_loss_pct.is_some_and(|sl| {
                        (Utc::now() - pos.opened_at).num_seconds() >= 300
                            && expected_out_sol <= pos.size_sol * (1.0 - sl / 100.0)
                    }) {
                        Some("stop_loss".into())
                    } else {
                        None
                    }
                } else if expected_out_sol >= pos.size_sol * 2.0 {
                    Some("tp1".into())
                } else if plan.stop_loss_pct.is_some_and(|sl| {
                    (Utc::now() - pos.opened_at).num_seconds() >= 300
                        && expected_out_sol <= pos.size_sol * (1.0 - sl / 100.0)
                }) {
                    Some("stop_loss".into())
                } else {
                    None
                }
            } else if plan.mode == "alpha_playbook_old_dormant" {
                let stage = quote_ctx.stage.clone().unwrap_or_else(|| "new".into());
                if expected_out_sol <= pos.size_sol * 0.67 {
                    Some("stop_loss".into())
                } else if stage != "runner" && expected_out_sol >= pos.size_sol * 1.5 {
                    Some("tp1".into())
                } else {
                    None
                }
            } else {
                let tp_pct = plan.take_profit_pct.unwrap_or(20.0);
                let sl_pct = plan.stop_loss_pct;

                let tp = pos.size_sol * (1.0 + (tp_pct / 100.0));
                let sl = sl_pct.map(|p| pos.size_sol * (1.0 - (p / 100.0)));

                if expected_out_sol >= tp {
                    Some("take_profit".into())
                } else if let Some(sl) = sl {
                    if expected_out_sol <= sl {
                        Some("stop_loss".into())
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            if trigger.is_none() {
                trigger = maybe_trigger_stale_non_loss_exit(
                    &jup,
                    &redis,
                    &cfg,
                    &pos,
                    &plan,
                    quote_ctx.stage.as_deref(),
                    expected_out_sol,
                )
                .await;
            }

            if let Some(trigger) = trigger {
                // Prevent spam: if executor already has a sell in-flight for this position, don't enqueue more.
                // (Do NOT acquire the lock here; executor owns the lock.)
                let locked = redis
                    .get_flag(&state::keys::Keys::inflight_sell(&pos.id))
                    .await
                    .unwrap_or(false);
                if locked {
                    continue;
                }
                // Build a synthetic Sell intent and enqueue directly to executor queue.
                // We bypass filters because filters default to only_buys.
                let (sig, amount_in_base_units) = if plan.mode == "in_and_out" && trigger == "tp1" {
                    let total: u128 = pos.token_amount.parse::<u128>().unwrap_or(0);
                    let tp1_upside_pct = plan
                        .tp1_upside_pct
                        .unwrap_or(DEFAULT_IN_AND_OUT_TP1_UPSIDE_PCT);
                    let moonbag_target_multiple = plan
                        .moonbag_target_multiple
                        .unwrap_or(DEFAULT_IN_AND_OUT_MOONBAG_TARGET_MULTIPLE);
                    let tp1_sell_amount = principal_recovery_sell_amount(total, tp1_upside_pct);
                    let runner_token: u128 = total.saturating_sub(tp1_sell_amount);
                    let runner_entry_value_sol = if total > 0 {
                        pos.size_sol * ((runner_token as f64) / (total as f64))
                    } else {
                        0.0
                    };
                    let runner_target_sol = runner_entry_value_sol * moonbag_target_multiple;
                    let _ = redis
                        .set_exit_state_fields(
                            &pos.id,
                            &[
                                ("stage", "tp1_enqueued"),
                                ("orig_size_sol", &pos.size_sol.to_string()),
                                ("orig_token_amount", pos.token_amount.as_str()),
                                (
                                    "runner_entry_value_sol",
                                    &runner_entry_value_sol.to_string(),
                                ),
                                ("runner_token_amount", &runner_token.to_string()),
                                ("runner_target_sol", &runner_target_sol.to_string()),
                            ],
                        )
                        .await;
                    (
                        format!("exit:{}:tp1:{}", pos.id, Uuid::new_v4()),
                        tp1_sell_amount.to_string(),
                    )
                } else if plan.mode == "in_and_out" && trigger == "runner_tp" {
                    let _ = redis
                        .set_exit_state_fields(&pos.id, &[("stage", "final_enqueued")])
                        .await;
                    (
                        format!("exit:{}:final:{}", pos.id, Uuid::new_v4()),
                        pos.token_amount.clone(),
                    )
                } else if plan.mode == "alpha_playbook_new_hot" && trigger == "tp1" {
                    let total: u128 = pos.token_amount.parse::<u128>().unwrap_or(0);
                    let sell_amount = percentage_amount(total, 50.0);
                    let runner_amount = total.saturating_sub(sell_amount);
                    let now = Utc::now();
                    let _ = redis
                        .set_exit_state_fields(
                            &pos.id,
                            &[
                                ("stage", "tp1_enqueued"),
                                ("runner_entry_value_sol", &(pos.size_sol * 0.5).to_string()),
                                ("runner_token_amount", &runner_amount.to_string()),
                                ("clip_count", "0"),
                                ("clip_anchor_out_sol", &pos.size_sol.to_string()),
                                ("next_clip_at", &(now + chrono::Duration::minutes(3)).to_rfc3339()),
                            ],
                        )
                        .await;
                    (
                        format!("exit:{}:tp1:{}", pos.id, Uuid::new_v4()),
                        sell_amount.to_string(),
                    )
                } else if plan.mode == "alpha_playbook_new_hot" && trigger == "clip" {
                    let exit_state = redis.get_exit_state(&pos.id).await.unwrap_or_default();
                    let clip_count = exit_state
                        .get("clip_count")
                        .and_then(|value| value.parse::<u32>().ok())
                        .unwrap_or(0);
                    let runner_token_amount = exit_state
                        .get("runner_token_amount")
                        .and_then(|value| value.parse::<u128>().ok())
                        .unwrap_or_else(|| pos.token_amount.parse::<u128>().unwrap_or(0));
                    let current_amount = pos.token_amount.parse::<u128>().unwrap_or(0);
                    let amount = if clip_count >= 3 {
                        current_amount
                    } else {
                        (runner_token_amount / 4).max(1).min(current_amount)
                    };
                    let _ = redis
                        .set_exit_state_fields(
                            &pos.id,
                            &[
                                ("stage", "runner"),
                                ("clip_count", &(clip_count.saturating_add(1)).to_string()),
                                ("clip_anchor_out_sol", &expected_out_sol.to_string()),
                                ("next_clip_at", &(Utc::now() + chrono::Duration::minutes(3)).to_rfc3339()),
                            ],
                        )
                        .await;
                    (
                        format!("exit:{}:clip:{}", pos.id, Uuid::new_v4()),
                        amount.to_string(),
                    )
                } else if plan.mode == "alpha_playbook_old_dormant" && trigger == "tp1" {
                    let total: u128 = pos.token_amount.parse::<u128>().unwrap_or(0);
                    let sell_amount = percentage_amount(total, 50.0);
                    let _ = redis
                        .set_exit_state_fields(&pos.id, &[("stage", "tp1_enqueued"), ("runner_entry_value_sol", &(pos.size_sol * 0.5).to_string())])
                        .await;
                    (
                        format!("exit:{}:tp1:{}", pos.id, Uuid::new_v4()),
                        sell_amount.to_string(),
                    )
                } else if plan.mode == "basic" || trigger == "stop_loss" {
                    let sell_pct = if trigger == "stop_loss" {
                        plan.sell_percent_on_stop_loss.unwrap_or(100.0)
                    } else {
                        plan.sell_percent_on_take_profit.unwrap_or(100.0)
                    };
                    let total: u128 = pos.token_amount.parse::<u128>().unwrap_or(0);
                    let bounded_pct = sell_pct.clamp(0.0, 100.0);
                    let amount = percentage_amount(total, bounded_pct);
                    if amount == 0 {
                        continue;
                    }
                    (
                        format!("exit:{}:basic:{}", pos.id, Uuid::new_v4()),
                        amount.to_string(),
                    )
                } else {
                    (
                        format!("exit:{}:final:{}", pos.id, Uuid::new_v4()),
                        pos.token_amount.clone(),
                    )
                };

                let intent = TradeIntent {
                    signature: sig,
                    slot: 0,
                    wallet: pos.wallet.clone(),
                    side: TradeSide::Sell,
                    mint: pos.mint.clone(),
                    notional_sol: expected_out_sol,
                    venue: Some(format!("exit_{}:{}", trigger, plan.mode)),
                    observed_at: Some(Utc::now()),
                    classified_at: Some(Utc::now()),
                    amount_in_base_units: Some(amount_in_base_units),
                    token_delta_base_units: None,
                    requested_buy_sol: None,
                    source_wallet_exit_full: false,
                    source_wallet_sold_pct: None,
                    created_at: Utc::now(),
                };

                let order = serde_json::json!({
                    "intent": intent,
                    "filter_reason": trigger
                });
                redis.enqueue_exec_order(&order.to_string()).await?;

                info!(
                    pos_id = %pos.id,
                    mint = %pos.mint,
                    expected_out_sol,
                    in_sol = pos.size_sol,
                    trigger = %trigger,
                    mode = %plan.mode,
                    source = %plan.source,
                    "exit triggered; enqueued sell"
                );
            }
        }

        sleep(Duration::from_millis(cfg.cadence_ms)).await;
    }
}

#[derive(Debug, Clone)]
struct QuoteContext {
    quote_amount_in_base_units: String,
    stage: Option<String>,
}

async fn resolve_exit_plan(
    redis: &RedisState,
    cfg: &ExitEngineConfig,
    pos: &Position,
) -> Result<Option<PositionExitPlan>> {
    if let Some(plan) = redis.get_position_exit_plan(&pos.id).await? {
        return Ok(Some(plan));
    }

    let fallback = if let Some(strategy_id) = pos
        .sell_strategy_id
        .clone()
        .or_else(|| pos.strategy_id.clone())
        .or_else(|| {
            if pos.wallet.starts_with("telegram:") {
                Some(cfg.telegram_strategy_id.clone())
            } else {
                cfg.wallet_strategy_id.get(&pos.wallet).cloned()
            }
        }) {
        plan_from_strategy(cfg, &pos.id, &strategy_id)
    } else {
        cfg.default_exit_plan_template
            .to_position_plan(&pos.id, "default_config")
    };

    redis.set_position_exit_plan(&fallback).await?;
    Ok(Some(fallback))
}

fn plan_from_strategy(cfg: &ExitEngineConfig, pos_id: &str, strategy_id: &str) -> PositionExitPlan {
    if let Some(template) = cfg.strategy_exit_templates.get(strategy_id) {
        return template.to_position_plan(pos_id, &format!("strategy:{strategy_id}"));
    }

    cfg.default_exit_plan_template
        .to_position_plan(pos_id, "default_config")
}

async fn maybe_trigger_stale_non_loss_exit(
    jup: &JupiterClient,
    redis: &RedisState,
    cfg: &ExitEngineConfig,
    pos: &Position,
    plan: &PositionExitPlan,
    current_stage: Option<&str>,
    current_expected_out_sol: f64,
) -> Option<String> {
    let stale_after_seconds = plan.stale_exit_after_seconds.filter(|value| *value > 0)?;
    let token_info = token_info_cached(redis, None, &pos.mint, 300).await?;
    let pair_created_at = token_info.pair_created_at?;
    if (Utc::now() - pair_created_at).num_seconds() < stale_after_seconds {
        return None;
    }

    if !plan.stale_exit_requires_non_loss.unwrap_or(false) {
        return Some("stale_non_loss_exit".into());
    }

    let non_loss_threshold_sol = if current_stage == Some("runner") {
        0.0
    } else {
        pos.size_sol
    };
    let expected_out_sol = if current_stage == Some("runner") {
        current_expected_out_sol
    } else {
        let full_quote = jup
            .quote(&pos.mint, WSOL_MINT, &pos.token_amount, cfg.slippage_bps)
            .await
            .ok()?;
        let out_amount = full_quote
            .get("outAmount")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        out_amount as f64 / 1_000_000_000.0
    };
    if expected_out_sol >= non_loss_threshold_sol {
        Some("stale_non_loss_exit".into())
    } else {
        None
    }
}

async fn build_quote_context(
    redis: &RedisState,
    pos: &Position,
    plan: &PositionExitPlan,
) -> Result<Option<QuoteContext>> {
    if plan.mode != "in_and_out" {
        return Ok(Some(QuoteContext {
            quote_amount_in_base_units: pos.token_amount.clone(),
            stage: None,
        }));
    }

    let es = redis.get_exit_state(&pos.id).await.unwrap_or_default();
    let stage = es.get("stage").cloned();
    if matches!(
        stage.as_deref(),
        Some("tp1_enqueued") | Some("final_enqueued")
    ) {
        return Ok(None);
    }
    if matches!(stage.as_deref(), Some("runner")) {
        return Ok(Some(QuoteContext {
            quote_amount_in_base_units: pos.token_amount.clone(),
            stage,
        }));
    }

    let total: u128 = pos.token_amount.parse::<u128>().unwrap_or(0);
    if total == 0 {
        return Ok(None);
    }
    let tp1_upside_pct = plan
        .tp1_upside_pct
        .unwrap_or(DEFAULT_IN_AND_OUT_TP1_UPSIDE_PCT);
    let recovery_amount = principal_recovery_sell_amount(total, tp1_upside_pct);
    if recovery_amount == 0 {
        return Ok(None);
    }
    Ok(Some(QuoteContext {
        quote_amount_in_base_units: recovery_amount.to_string(),
        stage,
    }))
}

fn principal_recovery_sell_amount(total: u128, upside_pct: f64) -> u128 {
    if total <= 1 {
        return total;
    }

    let bounded_upside_pct = upside_pct.max(0.0);
    let recovery_fraction = 1.0 / (1.0 + (bounded_upside_pct / 100.0));
    let raw_amount = ((total as f64) * recovery_fraction).ceil() as u128;
    raw_amount.clamp(1, total.saturating_sub(1))
}

fn percentage_amount(total: u128, pct: f64) -> u128 {
    if total == 0 {
        return 0;
    }
    let bounded = pct.clamp(0.0, 100.0);
    if bounded <= 0.0 {
        return 0;
    }
    if (bounded - 100.0).abs() < f64::EPSILON {
        return total;
    }
    ((total as f64) * (bounded / 100.0)).floor() as u128
}

fn backoff_ms(failures: u32, base_ms: u64) -> i64 {
    // Start exponential backoff after 3 failures; cap at 10s.
    if failures < 3 {
        return base_ms as i64;
    }
    let exp = (failures - 3).min(6); // 2^6 = 64
    let ms = (base_ms as i64).saturating_mul(1i64 << exp);
    ms.min(10_000)
}

#[cfg(test)]
mod tests {
    use super::{percentage_amount, principal_recovery_sell_amount};

    #[test]
    fn principal_recovery_amount_recovers_cost_and_keeps_moonbag() {
        assert_eq!(principal_recovery_sell_amount(1000, 12.0), 893);
        assert_eq!(1000 - principal_recovery_sell_amount(1000, 12.0), 107);
    }

    #[test]
    fn principal_recovery_amount_never_sells_entire_position_when_possible() {
        assert_eq!(principal_recovery_sell_amount(2, 12.0), 1);
        assert_eq!(principal_recovery_sell_amount(10, 12.0), 9);
    }

    #[test]
    fn percentage_amount_floors_consistently() {
        assert_eq!(percentage_amount(1000, 80.0), 800);
        assert_eq!(percentage_amount(999, 12.5), 124);
    }
}

async fn maybe_alert_quote_fail(
    redis: &RedisState,
    pos_id: &str,
    mint: &str,
    st: &mut PosQuoteState,
) {
    // Alert after 10 failures, then at most once every 30s while failing.
    if st.failures < 10 {
        return;
    }
    let now = Utc::now();
    if let Some(last) = st.last_alert_at {
        if (now - last).num_seconds() < 30 {
            return;
        }
    }
    st.last_alert_at = Some(now);
    let alert = state::types::AlertEvent {
        ts: now,
        kind: "exit_quote_failing".into(),
        message: format!(
            "EXIT quote failing\npos_id: {}\nmint: {}\nfailures: {}",
            pos_id, mint, st.failures
        ),
    };
    let _ = redis
        .enqueue_alert(&serde_json::to_string(&alert).unwrap_or_default())
        .await;
}
