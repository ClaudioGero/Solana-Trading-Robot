use anyhow::{Context, Result};
use base64::Engine;
use chrono::Utc;
use common::{
    format::format_compact,
    jupiter::JupiterClient,
    token_info::{enrich_with_helius_metadata, BirdeyeClient, TokenInfo},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::{
    hash::Hash,
    message::VersionedMessage,
    signature::{Keypair, Signature, Signer},
    system_transaction,
    transaction::VersionedTransaction,
};
use state::{
    token_cache::token_info_cached,
    types::{AlphaPlaybookScenario, AlphaPlaybookTokenState, AlphaPlaybookWatchKind, AlphaPlaybookWatchState, ExecOrder, TradeIntent, TradeSide},
    RedisState,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

// Jupiter uses wSOL mint for SOL routes.
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
// USDC mint (mainnet).
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutorConfig {
    pub enabled: bool,
    pub jupiter_base_url: String, // e.g. https://quote-api.jup.ag
    #[serde(default)]
    pub jupiter_api_key: Option<String>,
    #[serde(default)]
    pub birdeye_api_key: Option<String>,
    pub slippage_bps: u64,
    pub wrap_and_unwrap_sol: bool,
    /// Treat balances at-or-below this UI amount as dust (effectively zero) and stop selling.
    /// This prevents repeated sell retries for tiny leftover ATA dust (e.g. 0.000001 tokens).
    ///
    /// Set to 0.0 to disable dust handling (not recommended).
    pub min_sell_token_ui: f64,
    /// TTL used to dedupe "dust stop" alerts, so Telegram doesn't spam if multiple stale
    /// sell orders are queued for the same mint.
    pub sell_dust_alert_ttl_seconds: u64,

    /// From bot.json copytrade.fixed_buy_sol.
    pub fixed_buy_sol: f64,

    /// Buy strategy routing.
    #[serde(default)]
    pub default_wallet_buy_strategy_id: String,
    #[serde(default)]
    pub telegram_buy_strategy_id: String,
    #[serde(default)]
    pub wallet_buy_strategy_id: HashMap<String, String>,
    /// Buy strategy modes keyed by strategy id (e.g. mirror_immediate, buy_dip).
    #[serde(default)]
    pub buy_strategy_mode: HashMap<String, String>,
    /// Buy strategy sizes (SOL). Missing => fall back to `fixed_buy_sol`.
    #[serde(default)]
    pub buy_strategy_sol: HashMap<String, f64>,
    #[serde(default)]
    pub buy_strategy_max_fill_delay_ms: HashMap<String, i64>,
    #[serde(default)]
    pub buy_strategy_max_price_above_alpha_pct: HashMap<String, f64>,
    #[serde(default)]
    pub buy_strategy_slippage_bps: HashMap<String, u64>,
    /// Sell strategy routing.
    #[serde(default)]
    pub default_wallet_sell_strategy_id: String,
    #[serde(default)]
    pub telegram_sell_strategy_id: String,
    #[serde(default)]
    pub wallet_sell_strategy_id: HashMap<String, String>,
    /// Sell strategy exit templates keyed by strategy id.
    #[serde(default)]
    pub sell_strategy_templates: HashMap<String, state::types::ExitPlanTemplate>,

    pub default_exit_plan_template: state::types::ExitPlanTemplate,
    #[serde(default)]
    pub openclaw_event_url: Option<String>,
    #[serde(default)]
    pub openclaw_api_key: Option<String>,
    #[serde(default)]
    pub helius_api_key: Option<String>,
    #[serde(default)]
    pub helius_api_base_url: String,

    /// From bot.json mode.
    pub dry_run: bool,
    pub simulate_only: bool,

    /// RPC HTTP endpoint for simulateTransaction (Helius recommended).
    pub rpc_http_url: String,

    /// Public key of the trader (your bot wallet) used for Jupiter's swap building.
    /// In Block 7 we will load + sign with KEYPAIR_PATH; for Block 6 we only need pubkey.
    pub user_public_key: String,

    /// Path to Solana keypair json (64-byte array). Required for Block 7 sending.
    pub keypair_path: Option<String>,

    /// Jito bundle endpoint (JSON-RPC). Required for Block 7 sending (always-on).
    pub jito_bundle_endpoint: Option<String>,

    /// Optional Jito tip in SOL (0 disables tip tx).
    pub jito_tip_sol: f64,
    /// Optional cap on priority fee for swap transactions (lamports).
    /// This is applied via Jupiter `/swap` as `prioritizationFeeLamports`.
    pub max_priority_fee_lamports: u64,

    pub idle_sleep_ms: u64,
}

#[derive(Debug, Serialize)]
struct OpenClawPositionOpenedEvent<'a> {
    event_type: &'static str,
    analysis_id: String,
    chain: &'static str,
    position_id: &'a str,
    wallet: &'a str,
    mint: &'a str,
    buy_signature: &'a str,
    strategy_id: &'a str,
    opened_at: chrono::DateTime<chrono::Utc>,
    spent_sol: f64,
    token_amount_base_units: &'a str,
    baseline_exit_plan: &'a state::types::PositionExitPlan,
    token: OpenClawTokenInfo<'a>,
}

#[derive(Debug, Serialize)]
struct OpenClawBuyDipRequestedEvent<'a> {
    event_type: &'static str,
    analysis_id: String,
    chain: &'static str,
    setup_id: String,
    wallet: &'a str,
    mint: &'a str,
    strategy_id: &'a str,
    alpha_observed_at: chrono::DateTime<chrono::Utc>,
    alpha_notional_sol: f64,
    alpha_market_cap_usd: f64,
    valid_for_seconds: i64,
    token: OpenClawTokenInfo<'a>,
}

#[derive(Debug, Serialize)]
struct OpenClawTokenInfo<'a> {
    symbol: Option<&'a str>,
    name: Option<&'a str>,
    market_cap_usd: Option<f64>,
    description: Option<&'a str>,
    description_source: Option<&'a str>,
    description_source_url: Option<&'a str>,
    pumpfun_url: String,
    dexscreener_url: String,
}

enum AlphaPlaybookDecision {
    Execute {
        buy_sol: f64,
        execution_mode: &'static str,
    },
    ArmWatch,
    Skip(&'static str),
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

async fn count_open_alpha_playbook_positions(redis: &RedisState) -> Result<usize> {
    let ids = redis.list_open_position_ids().await?;
    let mut count = 0usize;
    for id in ids {
        if let Some(pos) = redis.get_position(&id).await? {
            if pos.buy_strategy_id.as_deref() == Some("alpha_playbook") {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn decide_alpha_playbook_action(
    token_state: &AlphaPlaybookTokenState,
    wallet: &str,
    alpha_playbook_open_positions: usize,
) -> AlphaPlaybookDecision {
    if token_state.cooldown_active
        && token_state
            .cooldown_expires_at
            .is_none_or(|expiry| expiry > Utc::now())
    {
        return AlphaPlaybookDecision::Skip("cooldown_active");
    }

    if token_state
        .market_cap_usd
        .is_some_and(|market_cap| market_cap > 150_000.0)
    {
        return AlphaPlaybookDecision::Skip("market_cap_above_playbook_max");
    }

    let Some(scenario) = token_state.scenario else {
        return AlphaPlaybookDecision::Skip("scenario_unclassified");
    };

    let is_confirmation_wallet = token_state.confirmation_triggered
        && token_state
            .confirmation_wallets
            .iter()
            .any(|tracked_wallet| tracked_wallet == wallet)
        && token_state.first_wallet.as_deref() != Some(wallet);

    if !token_state.entry_executed && alpha_playbook_open_positions >= 3 {
        return AlphaPlaybookDecision::Skip("alpha_playbook_position_cap");
    }

    match scenario {
        AlphaPlaybookScenario::NewHot => {
            if token_state.entry_executed {
                AlphaPlaybookDecision::Skip("new_hot_already_entered")
            } else {
                AlphaPlaybookDecision::Execute {
                    buy_sol: 0.8,
                    execution_mode: "mirror_immediate",
                }
            }
        }
        AlphaPlaybookScenario::OldDormantSpiked => {
            if is_confirmation_wallet && !token_state.confirmation_entry_executed {
                AlphaPlaybookDecision::Execute {
                    buy_sol: 0.3,
                    execution_mode: "mirror_immediate",
                }
            } else if !token_state.watch_active {
                AlphaPlaybookDecision::ArmWatch
            } else {
                AlphaPlaybookDecision::Skip("old_dormant_waiting_for_retrace")
            }
        }
        AlphaPlaybookScenario::MidTrend => {
            if !token_state.entry_executed {
                AlphaPlaybookDecision::Execute {
                    buy_sol: 0.5,
                    execution_mode: "mirror_immediate",
                }
            } else if is_confirmation_wallet && !token_state.confirmation_entry_executed {
                AlphaPlaybookDecision::Execute {
                    buy_sol: 0.5,
                    execution_mode: "mirror_immediate",
                }
            } else {
                AlphaPlaybookDecision::Skip("mid_trend_already_sized")
            }
        }
    }
}

async fn arm_old_dormant_watch(
    redis: &RedisState,
    intent: &TradeIntent,
    token_state: &mut AlphaPlaybookTokenState,
) -> Result<()> {
    let now = Utc::now();
    token_state.watch_active = true;
    token_state.updated_at = now;
    redis.set_alpha_playbook_token_state(token_state).await?;

    let watch_state = AlphaPlaybookWatchState {
        mint: intent.mint.clone(),
        kind: AlphaPlaybookWatchKind::OldDormantRetrace,
        first_wallet: token_state.first_wallet.clone(),
        first_buy_at: token_state.first_buy_at,
        spike_market_cap_usd: token_state.market_cap_usd,
        spike_price_usd: None,
        retrace_15_level_usd: token_state.market_cap_usd.map(|value| value * 0.85),
        retrace_50_level_usd: token_state.market_cap_usd.map(|value| value * 0.5),
        last_low_observed_at: None,
        lowest_market_cap_usd: None,
        stabilization_deadline_at: None,
        expires_at: Some(now + chrono::Duration::minutes(5)),
        cancelled: false,
        cancel_reason: None,
        updated_at: now,
        created_at: now,
    };
    redis.set_alpha_playbook_watch_state(&watch_state).await?;
    Ok(())
}

async fn mark_alpha_playbook_fill(
    redis: &RedisState,
    mint: &str,
    wallet: &str,
) -> Result<()> {
    let Some(mut token_state) = redis.get_alpha_playbook_token_state(mint).await? else {
        return Ok(());
    };
    let now = Utc::now();
    let is_confirmation_wallet = token_state.confirmation_triggered
        && token_state.first_wallet.as_deref() != Some(wallet)
        && token_state
            .confirmation_wallets
            .iter()
            .any(|tracked_wallet| tracked_wallet == wallet);

    if !token_state.entry_executed {
        token_state.entry_executed = true;
        token_state.entry_executed_at = Some(now);
    } else if is_confirmation_wallet && !token_state.confirmation_entry_executed {
        token_state.confirmation_entry_executed = true;
        token_state.confirmation_entry_executed_at = Some(now);
    }

    token_state.chosen_playbook = token_state.scenario.map(|scenario| match scenario {
        AlphaPlaybookScenario::NewHot => "new_hot".to_string(),
        AlphaPlaybookScenario::OldDormantSpiked => "old_dormant_spiked".to_string(),
        AlphaPlaybookScenario::MidTrend => "mid_trend".to_string(),
    });
    token_state.updated_at = now;
    redis.set_alpha_playbook_token_state(&token_state).await?;
    Ok(())
}

async fn enqueue_alpha_playbook_exec_order(
    redis: &RedisState,
    wallet: &str,
    mint: &str,
    requested_buy_sol: f64,
    venue: &str,
    filter_reason: &str,
) -> Result<()> {
    let now = Utc::now();
    let order = ExecOrder {
        intent: TradeIntent {
            signature: format!("{filter_reason}:{mint}:{}", now.timestamp_millis()),
            slot: 0,
            wallet: wallet.to_string(),
            side: TradeSide::Buy,
            mint: mint.to_string(),
            notional_sol: requested_buy_sol,
            venue: Some(venue.to_string()),
            observed_at: Some(now),
            classified_at: Some(now),
            amount_in_base_units: None,
            token_delta_base_units: None,
            requested_buy_sol: Some(requested_buy_sol),
            source_wallet_exit_full: false,
            source_wallet_sold_pct: None,
            created_at: now,
        },
        filter_reason: filter_reason.to_string(),
        exec_enqueued_at: Some(now),
    };
    redis.enqueue_exec_order(&serde_json::to_string(&order)?).await?;
    Ok(())
}

#[derive(Clone)]
struct RpcClient {
    http_url: String,
    client: reqwest::Client,
}

impl RpcClient {
    fn new(http_url: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(12))
            .build()
            .context("failed building reqwest client")?;
        Ok(Self { http_url, client })
    }

    async fn simulate_transaction_base64(&self, tx_base64: &str) -> Result<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "simulateTransaction",
            "params": [
                tx_base64,
                {
                    "encoding": "base64",
                    "sigVerify": false,
                    "replaceRecentBlockhash": true,
                    "commitment": "processed"
                }
            ]
        });

        let resp = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .context("rpc simulateTransaction post failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("simulateTransaction not json")?;
        if !status.is_success() {
            anyhow::bail!("simulateTransaction http status {} body={}", status, v);
        }
        Ok(v)
    }

    async fn send_transaction_base64(&self, tx_base64: &str) -> Result<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });

        let resp = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .context("rpc sendTransaction post failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("sendTransaction not json")?;
        if !status.is_success() {
            anyhow::bail!("sendTransaction http status {} body={}", status, v);
        }
        Ok(v)
    }

    async fn get_latest_blockhash(&self) -> Result<Hash> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [
                { "commitment": "processed" }
            ]
        });

        let resp = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .context("rpc getLatestBlockhash post failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("getLatestBlockhash not json")?;
        if !status.is_success() {
            anyhow::bail!("getLatestBlockhash http status {} body={}", status, v);
        }

        let bh = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.get("blockhash"))
            .and_then(|b| b.as_str())
            .context("getLatestBlockhash missing result.value.blockhash")?;

        Ok(bh.parse::<Hash>().context("invalid blockhash")?)
    }

    async fn get_signature_status(&self, sig: &Signature) -> Result<Option<serde_json::Value>> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [
                [sig.to_string()],
                { "searchTransactionHistory": true }
            ]
        });

        let resp = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .context("rpc getSignatureStatuses post failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("getSignatureStatuses not json")?;
        if !status.is_success() {
            anyhow::bail!("getSignatureStatuses http status {} body={}", status, v);
        }

        let status0 = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|arr| arr.as_array())
            .and_then(|arr| arr.get(0))
            .cloned();
        Ok(status0)
    }

    async fn get_balance_lamports(&self, pubkey: &str) -> Result<u64> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [
                pubkey,
                { "commitment": "processed" }
            ]
        });
        let resp = self.client.post(&self.http_url).json(&body).send().await?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("getBalance not json")?;
        if !status.is_success() {
            anyhow::bail!("getBalance http status {} body={}", status, v);
        }
        let lamports = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_u64())
            .context("getBalance missing result.value")?;
        Ok(lamports)
    }

    async fn get_token_balance_base_units(&self, owner: &str, mint: &str) -> Result<u64> {
        // jsonParsed gives tokenAmount.amount (string integer base units)
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner,
                { "mint": mint },
                { "encoding": "jsonParsed", "commitment": "processed" }
            ]
        });
        let resp = self.client.post(&self.http_url).json(&body).send().await?;
        let status = resp.status();
        let v: serde_json::Value = resp
            .json()
            .await
            .context("getTokenAccountsByOwner not json")?;
        if !status.is_success() {
            anyhow::bail!("getTokenAccountsByOwner http status {} body={}", status, v);
        }
        let arr = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
            .context("getTokenAccountsByOwner missing result.value")?;

        let mut total: u64 = 0;
        for it in arr {
            let amt_str = it
                .get("account")
                .and_then(|a| a.get("data"))
                .and_then(|d| d.get("parsed"))
                .and_then(|p| p.get("info"))
                .and_then(|i| i.get("tokenAmount"))
                .and_then(|ta| ta.get("amount"))
                .and_then(|s| s.as_str())
                .unwrap_or("0");
            total = total.saturating_add(amt_str.parse::<u64>().unwrap_or(0));
        }
        Ok(total)
    }

    async fn get_token_balance_base_units_and_decimals(
        &self,
        owner: &str,
        mint: &str,
    ) -> Result<(u64, u8)> {
        // jsonParsed gives tokenAmount.amount (string integer base units) and tokenAmount.decimals (u8)
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner,
                { "mint": mint },
                { "encoding": "jsonParsed", "commitment": "processed" }
            ]
        });
        let resp = self.client.post(&self.http_url).json(&body).send().await?;
        let status = resp.status();
        let v: serde_json::Value = resp
            .json()
            .await
            .context("getTokenAccountsByOwner not json")?;
        if !status.is_success() {
            anyhow::bail!("getTokenAccountsByOwner http status {} body={}", status, v);
        }
        let arr = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
            .context("getTokenAccountsByOwner missing result.value")?;

        let mut total: u64 = 0;
        let mut decimals: u8 = 0;
        for it in arr {
            let token_amount = it
                .get("account")
                .and_then(|a| a.get("data"))
                .and_then(|d| d.get("parsed"))
                .and_then(|p| p.get("info"))
                .and_then(|i| i.get("tokenAmount"));

            let amt_str = token_amount
                .and_then(|ta| ta.get("amount"))
                .and_then(|s| s.as_str())
                .unwrap_or("0");
            total = total.saturating_add(amt_str.parse::<u64>().unwrap_or(0));

            if decimals == 0 {
                decimals = token_amount
                    .and_then(|ta| ta.get("decimals"))
                    .and_then(|d| d.as_u64())
                    .and_then(|d| u8::try_from(d).ok())
                    .unwrap_or(0);
            }
        }

        Ok((total, decimals))
    }

    async fn get_transaction_json_parsed(
        &self,
        signature: &Signature,
    ) -> Result<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature.to_string(),
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });
        let resp = self.client.post(&self.http_url).json(&body).send().await?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("getTransaction not json")?;
        if !status.is_success() {
            anyhow::bail!("getTransaction http status {} body={}", status, v);
        }
        Ok(v)
    }

    async fn get_token_supply(&self, mint: &str) -> Result<(u128, u8)> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [ mint ]
        });
        let resp = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .context("rpc getTokenSupply post failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("getTokenSupply not json")?;
        if !status.is_success() {
            anyhow::bail!("getTokenSupply http status {} body={}", status, v);
        }
        let val = v
            .get("result")
            .and_then(|r| r.get("value"))
            .context("getTokenSupply missing result.value")?;
        let amount_str = val
            .get("amount")
            .and_then(|a| a.as_str())
            .context("getTokenSupply missing value.amount")?;
        let decimals = val
            .get("decimals")
            .and_then(|d| d.as_u64())
            .context("getTokenSupply missing value.decimals")? as u8;
        let amount = amount_str
            .parse::<u128>()
            .context("getTokenSupply amount is not u128")?;
        Ok((amount, decimals))
    }
}

#[derive(Clone)]
struct JitoClient {
    endpoint: String,
    client: reqwest::Client,
}

impl JitoClient {
    fn new(endpoint: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(12))
            .build()
            .context("failed building reqwest client")?;
        // Users often set JITO_BUNDLE_ENDPOINT to the base host (e.g. https://ny.mainnet.block-engine.jito.wtf).
        // The JSON-RPC endpoint for bundles is typically at /api/v1/bundles.
        let endpoint = normalize_jito_endpoint(&endpoint);
        Ok(Self { endpoint, client })
    }

    async fn send_bundle_base58(&self, txs_base58: Vec<String>) -> Result<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [ txs_base58 ]
        });

        let resp = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await
            .context("jito sendBundle failed")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let v: serde_json::Value = serde_json::from_str(&text).with_context(|| {
            let snip: String = text.chars().take(600).collect();
            format!(
                "jito sendBundle not json status={} body_snip={}",
                status, snip
            )
        })?;
        if !status.is_success() {
            anyhow::bail!("jito sendBundle http status {} body={}", status, v);
        }
        Ok(v)
    }
}

fn normalize_jito_endpoint(raw: &str) -> String {
    let mut e = raw.trim().to_string();
    while e.ends_with('/') {
        e.pop();
    }
    // If they already passed a full API path, respect it.
    if e.contains("/api/v1/") {
        return e;
    }
    format!("{}/api/v1/bundles", e)
}

fn load_keypair(path: &str) -> Result<Keypair> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed reading KEYPAIR_PATH={path}"))?;
    let arr: Vec<u8> =
        serde_json::from_slice(&bytes).context("keypair file must be json array of bytes")?;
    Keypair::try_from(arr.as_slice()).context("invalid keypair bytes")
}

fn replace_recent_blockhash(mut msg: VersionedMessage, bh: Hash) -> VersionedMessage {
    match &mut msg {
        VersionedMessage::Legacy(m) => m.recent_blockhash = bh,
        VersionedMessage::V0(m) => m.recent_blockhash = bh,
    }
    msg
}

fn decode_and_sign_swap_tx(
    swap_tx_base64: &str,
    keypair: &Keypair,
    bh: Hash,
) -> Result<(VersionedTransaction, Signature)> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(swap_tx_base64)
        .context("swapTransaction is not valid base64")?;

    let vtx: VersionedTransaction =
        bincode::deserialize(&raw).context("failed decoding VersionedTransaction")?;
    let msg = replace_recent_blockhash(vtx.message, bh);

    let signed = VersionedTransaction::try_new(msg, &[keypair]).context("failed signing tx")?;
    let sig = signed.signatures.get(0).cloned().unwrap_or_default();
    Ok((signed, sig))
}

fn encode_tx_base58(tx: &VersionedTransaction) -> Result<String> {
    let raw = bincode::serialize(tx).context("failed serializing tx")?;
    Ok(bs58::encode(raw).into_string())
}

fn encode_tx_base64(tx: &VersionedTransaction) -> Result<String> {
    let raw = bincode::serialize(tx).context("failed serializing tx")?;
    Ok(base64::engine::general_purpose::STANDARD.encode(raw))
}

fn is_jito_rate_limited(err: &anyhow::Error) -> bool {
    // Jito returns 429 with messages like:
    // - "http status 429 Too Many Requests"
    // - "globally rate limited"
    // - "Network congested. Endpoint is globally rate limited."
    let s = err.to_string().to_lowercase();
    s.contains("http status 429")
        || s.contains("too many requests")
        || s.contains("rate limited")
        || s.contains("globally rate limited")
        || s.contains("network congested")
}

fn build_jito_tip_tx(keypair: &Keypair, bh: Hash, tip_lamports: u64) -> VersionedTransaction {
    // v1: a single known Jito tip account (we can rotate across the official list later).
    let tip_account = "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY"
        .parse()
        .expect("valid pubkey");
    let legacy = system_transaction::transfer(keypair, &tip_account, tip_lamports, bh);
    VersionedTransaction::from(legacy)
}

pub async fn run_executor_worker(cfg: ExecutorConfig, redis: RedisState) -> Result<()> {
    if !cfg.enabled {
        info!("executor disabled; idling");
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }

    let rpc = RpcClient::new(cfg.rpc_http_url.clone())?;
    let jup = JupiterClient::new(
        cfg.jupiter_base_url.clone(),
        cfg.jupiter_api_key.clone(),
        Duration::from_secs(12),
    )?
    .with_prioritization_fee_max_lamports(
        (cfg.max_priority_fee_lamports > 0).then_some(cfg.max_priority_fee_lamports),
    );
    let birdeye = cfg
        .birdeye_api_key
        .clone()
        .and_then(|k| BirdeyeClient::new(k).ok());
    let jito = cfg
        .jito_bundle_endpoint
        .as_ref()
        .map(|e| JitoClient::new(e.clone()))
        .transpose()?;

    // When sending is enabled (dry_run=false && simulate_only=false), Jupiter swap building
    // must use the SAME pubkey as the signing keypair, otherwise signing will fail.
    let sending_enabled = !cfg.simulate_only && !cfg.dry_run;
    let keypair: Option<Keypair> = if sending_enabled {
        match cfg.keypair_path.as_deref() {
            Some(path) => match load_keypair(path) {
                Ok(kp) => Some(kp),
                Err(e) => {
                    warn!(error = %e, "failed loading KEYPAIR_PATH; cannot sign/send");
                    None
                }
            },
            None => {
                warn!("sending enabled but KEYPAIR_PATH not set; cannot sign/send");
                None
            }
        }
    } else {
        None
    };

    let mut user_public_key = cfg.user_public_key.clone();
    if let Some(kp) = keypair.as_ref() {
        let kp_pub = kp.pubkey().to_string();
        if !user_public_key.is_empty() && user_public_key != kp_pub {
            warn!(
                cfg_user_public_key = %user_public_key,
                keypair_pubkey = %kp_pub,
                "USER_PUBLIC_KEY does not match KEYPAIR_PATH pubkey; using keypair pubkey"
            );
        }
        user_public_key = kp_pub;
    }

    info!(
        dry_run = cfg.dry_run,
        simulate_only = cfg.simulate_only,
        slippage_bps = cfg.slippage_bps,
        fixed_buy_sol = cfg.fixed_buy_sol,
        jito_always_on = true,
        "executor worker started (Block 6 sim / Block 7 send)"
    );

    let buy_dip_cfg = cfg.clone();
    let buy_dip_redis = redis.clone();
    tokio::spawn(async move {
        if let Err(e) = run_buy_dip_worker(buy_dip_cfg, buy_dip_redis).await {
            warn!(error = %e, "buy_dip worker task exited");
        }
    });

    loop {
        let popped = match redis.pop_exec_order().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (executor); retrying");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let Some(raw) = popped else {
            sleep(Duration::from_millis(cfg.idle_sleep_ms)).await;
            continue;
        };

        let order: ExecOrder = serde_json::from_str(&raw).context("failed parsing ExecOrder")?;

        // Resolve buy/sell strategy ids for this intent.
        let buy_strategy_id: String = if order.intent.wallet.starts_with("telegram:") {
            if cfg.telegram_buy_strategy_id.trim().is_empty() {
                "mirror_immediate".into()
            } else {
                cfg.telegram_buy_strategy_id.clone()
            }
        } else {
            cfg.wallet_buy_strategy_id
                .get(&order.intent.wallet)
                .cloned()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| {
                    if cfg.default_wallet_buy_strategy_id.trim().is_empty() {
                        "mirror_immediate".into()
                    } else {
                        cfg.default_wallet_buy_strategy_id.clone()
                    }
                })
        };
        let sell_strategy_id: String = if order.intent.wallet.starts_with("telegram:") {
            if cfg.telegram_sell_strategy_id.trim().is_empty() {
                "tg_calls".into()
            } else {
                cfg.telegram_sell_strategy_id.clone()
            }
        } else {
            cfg.wallet_sell_strategy_id
                .get(&order.intent.wallet)
                .cloned()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| {
                    if cfg.default_wallet_sell_strategy_id.trim().is_empty() {
                        "in_and_out".into()
                    } else {
                        cfg.default_wallet_sell_strategy_id.clone()
                    }
                })
        };
        let now = Utc::now();
        let obs_age_ms = order
            .intent
            .observed_at
            .map(|t| (now - t).num_milliseconds())
            .unwrap_or(-1);
        let exec_queue_delay_ms = order
            .exec_enqueued_at
            .map(|t| (now - t).num_milliseconds())
            .unwrap_or(-1);
        let classify_to_exec_queue_ms = match (order.intent.classified_at, order.exec_enqueued_at) {
            (Some(c), Some(e)) => (e - c).num_milliseconds(),
            _ => -1,
        };

        info!(
            signature = %order.intent.signature,
            side = ?order.intent.side,
            mint = %order.intent.mint,
            obs_age_ms,
            exec_queue_delay_ms,
            classify_to_exec_queue_ms,
            "executor received exec order"
        );

        // Hard guard: we must know which wallet we're trading from.
        // If this is empty, any getBalance/getTokenAccountsByOwner calls will be nonsense.
        if user_public_key.trim().is_empty() {
            let alert = state::types::AlertEvent {
                ts: chrono::Utc::now(),
                kind: "bot_misconfig_missing_pubkey".into(),
                    message: "CLAWDIO BOT misconfig: USER_PUBLIC_KEY is empty (and no valid KEYPAIR_PATH loaded). Fix core/.env and restart core-app.".into(),
            };
            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
            warn!("USER_PUBLIC_KEY is empty (and no valid KEYPAIR_PATH loaded); cannot proceed");
            continue;
        }

        // Task 7: global controls (Redis flags)
        let emergency_stop = redis
            .get_flag(state::keys::Keys::CTRL_EMERGENCY_STOP)
            .await
            .unwrap_or(false);
        let pause_buys = redis
            .get_flag(state::keys::Keys::CTRL_PAUSE_BUYS)
            .await
            .unwrap_or(false);
        let bot_off = state::is_bot_off_from_flags(pause_buys, emergency_stop);

        if emergency_stop {
            if !bot_off {
                let alert = state::types::AlertEvent {
                    ts: chrono::Utc::now(),
                    kind: "ctrl_emergency_stop".into(),
                    message: format!(
                        "CLAWDIO BOT EMERGENCY STOP active (blocking ALL)\nside: {:?}\nmint: {}",
                        order.intent.side, order.intent.mint
                    ),
                };
                let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
            }
            continue;
        }
        if pause_buys && order.intent.side == state::types::TradeSide::Buy {
            if !bot_off {
                let alert = state::types::AlertEvent {
                    ts: chrono::Utc::now(),
                    kind: "ctrl_pause_buys".into(),
                    message: format!(
                        "CLAWDIO BOT PAUSE_BUYS active (buy skipped)\nmint: {}",
                        order.intent.mint
                    ),
                };
                let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
            }
            continue;
        }

        // Task 4: for SELLs, enforce per-position sell-lock so only one sell attempt runs at once.
        // We derive position id from the synthetic signature prefixes we generate.
        let sell_pos_id = if order.intent.side == state::types::TradeSide::Sell {
            order
                .intent
                .signature
                .strip_prefix("exit:")
                .or_else(|| order.intent.signature.strip_prefix("alpha_sell:"))
                .map(|s| s.split(':').next().unwrap_or(""))
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        } else {
            None
        };
        if let Some(pos_id) = sell_pos_id.as_deref() {
            let locked = redis.try_lock_sell(pos_id, 30).await.unwrap_or(false);
            if !locked {
                continue;
            }
        }

        // Task 2: lateness guard (BUYs only). If too late, skip the trade and alert.
        //
        // NOTE: our classifier uses `getTransaction` at `confirmed` (Helius requires it),
        // so classification can legitimately take several seconds. Keep this window wide.
        if order.intent.side == state::types::TradeSide::Buy {
            if let Some(obs) = order.intent.observed_at {
                let now = chrono::Utc::now();
                let age_ms = (now - obs).num_milliseconds();
                let max_age_ms: i64 = 15_000;
                if age_ms > max_age_ms {
                    let alert = state::types::AlertEvent {
                        ts: now,
                        kind: "bot_skip_late".into(),
                        message: format!(
                            "CLAWDIO BOT SKIP (late)\nage_ms: {}\nmax_age_ms: {}\ntoken: {}",
                            age_ms, max_age_ms, order.intent.mint
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    continue;
                }
            }
        }

        // Task 6: SELL retry loop remains (Task 4). We keep it Jito-first; fallback is handled per-attempt below.
        if order.intent.side == state::types::TradeSide::Sell {
            let mut attempts: u32 = 0;
            let mut consecutive_jito_errors: u32 = 0;
            let mut last_retry_alert_at: Option<chrono::DateTime<chrono::Utc>> = None;

            loop {
                attempts += 1;
                if let Some(pos_id) = sell_pos_id.as_deref() {
                    let _ = redis.refresh_sell_lock(pos_id, 30).await;
                }
                let now = chrono::Utc::now();
                if attempts > 1
                    && last_retry_alert_at
                        .map(|last| (now - last).num_seconds() >= 30)
                        .unwrap_or(true)
                {
                    last_retry_alert_at = Some(now);
                    let alert = state::types::AlertEvent {
                        ts: now,
                        kind: "bot_sell_retrying".into(),
                        message: format!(
                            "CLAWDIO BOT SELL still retrying\nmint: {}\nattempts: {}",
                            order.intent.mint, attempts
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                }

                // Determine sell amount (ALWAYS use live balance for safety).
                // Stored amounts can be stale (dust, partial fills, ATA changes).
                let (bal, decimals) = rpc
                    .get_token_balance_base_units_and_decimals(&user_public_key, &order.intent.mint)
                    .await
                    .unwrap_or((0, 0));

                // Dust cleanup: treat tiny leftover balances as "sold" so we don't retry-sell forever.
                // (Many routes reject tiny inputs; those end up spamming retries + Telegram alerts.)
                let dust_threshold =
                    sell_dust_threshold_base_units(cfg.min_sell_token_ui, decimals);
                let is_dust = dust_threshold > 0 && bal <= dust_threshold;
                if bal == 0 || is_dust {
                    if bal == 0 {
                        warn!(mint = %order.intent.mint, "sell requested but token balance is 0; closing position");
                    } else {
                        warn!(
                            mint = %order.intent.mint,
                            bal,
                            decimals,
                            dust_threshold,
                            min_sell_token_ui = cfg.min_sell_token_ui,
                            "sell requested but token balance is dust; closing position"
                        );
                        // Emit a deduped Telegram alert so this doesn't spam if stale sell orders exist.
                        let dedupe_key = if let Some(pos_id) = sell_pos_id.as_deref() {
                            format!("sell_dust:{}:{}", order.intent.mint, pos_id)
                        } else {
                            format!("sell_dust:{}", order.intent.mint)
                        };
                        let should_alert = redis
                            .dedupe_signature(&dedupe_key, cfg.sell_dust_alert_ttl_seconds as usize)
                            .await
                            .unwrap_or(true);
                        if should_alert {
                            let alert = state::types::AlertEvent {
                                ts: chrono::Utc::now(),
                                kind: "bot_sell_dust_balance".into(),
                                message: format!(
                                    "CLAWDIO BOT SELL stop: dust balance (treat as sold)\nmint: {}\nbal_base_units: {}\ndecimals: {}\nmin_sell_token_ui: {}\ndust_threshold_base_units: {}",
                                    order.intent.mint, bal, decimals, cfg.min_sell_token_ui, dust_threshold
                                ),
                            };
                            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                        }
                    }
                    if let Some(pos_id) = sell_pos_id.as_deref() {
                        let _ = redis.close_position(pos_id).await;
                    }
                    break;
                }
                let requested_amount = order
                    .intent
                    .amount_in_base_units
                    .as_deref()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0);
                let amount_u64 = requested_amount.map(|v| v.min(bal)).unwrap_or(bal);
                if amount_u64 == 0 {
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }

                let input_mint = order.intent.mint.clone();
                let output_mint = WSOL_MINT.to_string();

                // Quote -> swap build.
                let quote = match jup
                    .quote(
                        &input_mint,
                        &output_mint,
                        &amount_u64.to_string(),
                        cfg.slippage_bps,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(mint = %input_mint, error = %e, "sell quote failed; retrying");
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let swap = match jup
                    .swap(quote, &user_public_key, cfg.wrap_and_unwrap_sol)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(mint = %input_mint, error = %e, "sell swap build failed; retrying");
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let swap_tx = match swap.get("swapTransaction").and_then(|t| t.as_str()) {
                    Some(s) => s,
                    None => {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };

                // In live mode, sign+send (we assume simulate_only/dry_run are false for Task 4 relevance).
                if cfg.simulate_only || cfg.dry_run {
                    break;
                }

                let Some(kp_path) = cfg.keypair_path.as_ref() else {
                    break;
                };
                let Some(jito) = jito.as_ref() else {
                    break;
                };
                let keypair = match load_keypair(kp_path) {
                    Ok(k) => k,
                    Err(e) => {
                        warn!(error=%e, "failed loading KEYPAIR_PATH for sell; cannot send");
                        break;
                    }
                };
                let bh = match rpc.get_latest_blockhash().await {
                    Ok(b) => b,
                    Err(_) => {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let (signed_swap, sig) = match decode_and_sign_swap_tx(swap_tx, &keypair, bh) {
                    Ok(v) => v,
                    Err(_) => {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
                let mut bundle_txs: Vec<VersionedTransaction> = vec![signed_swap];
                if cfg.jito_tip_sol > 0.0 {
                    let tip_lamports = (cfg.jito_tip_sol * 1_000_000_000.0).round() as u64;
                    if tip_lamports > 0 {
                        bundle_txs.push(build_jito_tip_tx(&keypair, bh, tip_lamports));
                    }
                }
                let mut bundle_base58: Vec<String> = Vec::with_capacity(bundle_txs.len());
                for t in &bundle_txs {
                    bundle_base58.push(encode_tx_base58(t)?);
                }

                // Jito-first; fallback to RPC after 2 consecutive Jito errors.
                // (We keep this for behavior; message no longer includes `via`.)
                if let Err(e) = jito.send_bundle_base58(bundle_base58.clone()).await {
                    consecutive_jito_errors = consecutive_jito_errors.saturating_add(1);
                    warn!(error=%e, consecutive_jito_errors, "sell submit failed (jito)");
                    if consecutive_jito_errors >= 2 {
                        let tx_b64 = encode_tx_base64(&bundle_txs[0])?;
                        if let Err(e2) = rpc.send_transaction_base64(&tx_b64).await {
                            warn!(error=%e2, "sell RPC fallback failed; retrying");
                            sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    } else {
                        sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                } else {
                    consecutive_jito_errors = 0;
                }

                // Poll for a short time; if confirmed, close and exit.
                let poll_deadline = chrono::Utc::now() + chrono::Duration::seconds(4);
                let mut confirmed = false;
                while chrono::Utc::now() < poll_deadline {
                    if let Ok(Some(st)) = rpc.get_signature_status(&sig).await {
                        let err = st.get("err");
                        if err.is_some_and(|e| !e.is_null()) {
                            break;
                        }
                        if st.get("confirmationStatus").and_then(|s| s.as_str())
                            == Some("confirmed")
                            || st.get("confirmationStatus").and_then(|s| s.as_str())
                                == Some("finalized")
                        {
                            confirmed = true;
                            break;
                        }
                    }
                    sleep(Duration::from_millis(200)).await;
                }

                if confirmed {
                    // Best-effort enrich: compute received SOL from tx meta and add market cap if available.
                    let mut received_sol: f64 = 0.0;
                    let mut wallet_delta_sol: f64 = 0.0;
                    if let Ok(tx) = rpc.get_transaction_json_parsed(&sig).await {
                        let sol_delta = sol_delta_from_tx_meta(&user_public_key, &tx).unwrap_or(0);
                        received_sol = (sol_delta).max(0) as f64 / 1e9;
                        wallet_delta_sol = sol_delta as f64 / 1e9;
                    }
                    let token_info =
                        token_info_cached(&redis, birdeye.as_ref(), &input_mint, 300).await;
                    let mcap_line = match token_info.and_then(|t| t.market_cap_usd) {
                        Some(mc) => format!(" at ${} mcap", format_compact(mc)),
                        None => "".into(),
                    };
                    let alert = state::types::AlertEvent {
                        ts: chrono::Utc::now(),
                        kind: "bot_sell_confirmed".into(),
                        message: format!(
                            "CLAWDIO BOT SELL🔴\nToken: {}\nMarket Cap: {}\nSOL received: {:.3} SOL\nBalance: {}\nAttempts: {}",
                            input_mint,
                            mcap_line
                                .strip_prefix(" at $")
                                .and_then(|value| value.strip_suffix(" mcap"))
                                .map(|value| format!("${value}"))
                                .unwrap_or_else(|| "N/A".into()),
                            received_sol,
                            format_signed_sol(wallet_delta_sol),
                            attempts
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    if let Some(pos_id) = sell_pos_id.as_deref() {
                        reconcile_position_after_sell(
                            &rpc,
                            &redis,
                            &user_public_key,
                            pos_id,
                            &input_mint,
                            cfg.min_sell_token_ui,
                        )
                        .await?;
                    }
                    break;
                }

                sleep(Duration::from_secs(2)).await;
            }

            continue;
        }

        // Task 6: BUY has at most 2 landing attempts:
        // - attempt #1 immediately
        // - attempt #2 after 2s ONLY if outAmount hasn't dropped more than 20% (token-per-SOL >= 80%),
        //   and only if we still don't hold the token.
        let (input_mint, output_mint, amount_str) = match order.intent.side {
            state::types::TradeSide::Buy => {
                let buy_mode = cfg
                    .buy_strategy_mode
                    .get(&buy_strategy_id)
                    .map(|mode| mode.as_str())
                    .unwrap_or("mirror_immediate");
                if buy_mode == "buy_dip" {
                    maybe_start_buy_dip_setup(
                        &redis,
                        birdeye.as_ref(),
                        &cfg,
                        &order.intent,
                        &buy_strategy_id,
                    )
                    .await?;
                    continue;
                }

                let mut execution_mode = buy_mode;
                let mut alpha_playbook_requested_buy_sol: Option<f64> = None;
                if buy_mode == "alpha_playbook" {
                    let Some(mut token_state) = redis
                        .get_alpha_playbook_token_state(&order.intent.mint)
                        .await?
                    else {
                        let alert = state::types::AlertEvent {
                            ts: chrono::Utc::now(),
                            kind: "alpha_playbook_state_missing".into(),
                            message: format!(
                                "CLAWDIO BOT alpha_playbook skipped\nmint: {}\nreason: token state missing",
                                order.intent.mint
                            ),
                        };
                        let _ = enqueue_alert_if_bot_active(&redis, &alert).await;
                        continue;
                    };

                    let alpha_playbook_open_positions =
                        count_open_alpha_playbook_positions(&redis).await.unwrap_or(0);
                    match decide_alpha_playbook_action(
                        &token_state,
                        &order.intent.wallet,
                        alpha_playbook_open_positions,
                    ) {
                        AlphaPlaybookDecision::Execute {
                            buy_sol,
                            execution_mode: resolved_mode,
                        } => {
                            alpha_playbook_requested_buy_sol = Some(buy_sol);
                            execution_mode = resolved_mode;
                            token_state.chosen_playbook =
                                token_state.scenario.map(|scenario| match scenario {
                                    AlphaPlaybookScenario::NewHot => "new_hot".to_string(),
                                    AlphaPlaybookScenario::OldDormantSpiked => {
                                        "old_dormant_spiked".to_string()
                                    }
                                    AlphaPlaybookScenario::MidTrend => "mid_trend".to_string(),
                                });
                            token_state.chosen_size_sol = Some(buy_sol);
                            token_state.updated_at = Utc::now();
                            redis.set_alpha_playbook_token_state(&token_state).await?;
                        }
                        AlphaPlaybookDecision::ArmWatch => {
                            arm_old_dormant_watch(&redis, &order.intent, &mut token_state).await?;
                            let alert = state::types::AlertEvent {
                                ts: chrono::Utc::now(),
                                kind: "alpha_playbook_watch_armed".into(),
                                message: format!(
                                    "CLAWDIO BOT alpha_playbook watch armed\nmint: {}\nscenario: old_dormant_spiked\nexpiry: 5m",
                                    order.intent.mint
                                ),
                            };
                            let _ = enqueue_alert_if_bot_active(&redis, &alert).await;
                            continue;
                        }
                        AlphaPlaybookDecision::Skip(reason) => {
                            let alert = state::types::AlertEvent {
                                ts: chrono::Utc::now(),
                                kind: "alpha_playbook_skipped".into(),
                                message: format!(
                                    "CLAWDIO BOT alpha_playbook skipped\nmint: {}\nreason: {}",
                                    order.intent.mint, reason
                                ),
                            };
                            let _ = enqueue_alert_if_bot_active(&redis, &alert).await;
                            continue;
                        }
                    }
                }

                if execution_mode != "mirror_immediate" && execution_mode != "copytrade_fast" {
                    let alert = state::types::AlertEvent {
                        ts: chrono::Utc::now(),
                        kind: "bot_buy_strategy_not_implemented".into(),
                        message: format!(
                            "CLAWDIO BOT BUY skipped: strategy={} mode={} is not live yet\nmint: {}",
                            buy_strategy_id, execution_mode, order.intent.mint
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    continue;
                }

                // Strategy buy sizing.
                let buy_sol = cfg
                    .buy_strategy_sol
                    .get(&buy_strategy_id)
                    .copied()
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or(cfg.fixed_buy_sol);
                let requested_buy_sol = order
                    .intent
                    .requested_buy_sol
                    .or(alpha_playbook_requested_buy_sol)
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or(buy_sol);

                // Task 3: "buy each mint only once" (recent ring buffer + in-flight lock).
                if buy_strategy_id != "buy_dip"
                    && redis
                        .recent_mints_contains(&order.intent.mint, 10)
                        .await
                        .unwrap_or(false)
                {
                    let alert = state::types::AlertEvent {
                        ts: chrono::Utc::now(),
                        kind: "bot_skip_duplicate_mint".into(),
                        message: format!(
                            "CLAWDIO BOT SKIP duplicate mint (recent)\nmint: {}",
                            order.intent.mint
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    continue;
                }
                let locked = redis
                    .try_lock_mint(&order.intent.mint, 60)
                    .await
                    .unwrap_or(false);
                if !locked {
                    continue;
                }

                // Task 7: per-mint cap (lamports). Cap is our strategy buy size.
                let cap_lamports = (buy_sol * 1_000_000_000.0).round() as u64;
                let spent = redis
                    .get_spent_mint_lamports(&order.intent.mint)
                    .await
                    .unwrap_or(0);
                if spent >= cap_lamports {
                    let alert = state::types::AlertEvent {
                        ts: chrono::Utc::now(),
                        kind: "bot_skip_per_mint_cap".into(),
                        message: format!(
                            "CLAWDIO BOT SKIP per-mint cap\nmint: {}\nspent_sol_est: {:.6}\ncap_sol: {:.6}",
                            order.intent.mint,
                            spent as f64 / 1e9,
                            cap_lamports as f64 / 1e9
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    continue;
                }

                // Safety: ensure enough SOL to attempt the trade.
                let bal = rpc
                    .get_balance_lamports(&user_public_key)
                    .await
                    .unwrap_or(0);
                let needed = (requested_buy_sol * 1_000_000_000.0).round() as u64;
                let buffer = (0.02 * 1_000_000_000.0) as u64; // basic fee/tip buffer
                if bal < needed.saturating_add(buffer) {
                    let alert = state::types::AlertEvent {
                        ts: chrono::Utc::now(),
                        kind: "bot_skip_insufficient_sol".into(),
                        message: format!(
                            "CLAWDIO BOT skip BUY: insufficient SOL\npubkey: {}\nbalance: {:.4} SOL\nneeded: {:.4} SOL (+buffer)\n",
                            user_public_key,
                            bal as f64 / 1e9,
                            (needed + buffer) as f64 / 1e9
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    continue;
                }
                let amt = (requested_buy_sol * 1_000_000_000.0).round() as u64;
                (
                    WSOL_MINT.to_string(),
                    order.intent.mint.clone(),
                    amt.to_string(),
                )
            }
            state::types::TradeSide::Sell => {
                let amt = match order.intent.amount_in_base_units.clone() {
                    Some(a) if a.parse::<u64>().unwrap_or(0) > 0 => a,
                    _ => {
                        // Fetch live balance to avoid wrong sell amount.
                        let (bal, decimals) = rpc
                            .get_token_balance_base_units_and_decimals(
                                &user_public_key,
                                &order.intent.mint,
                            )
                            .await
                            .unwrap_or((0, 0));
                        let dust_threshold =
                            sell_dust_threshold_base_units(cfg.min_sell_token_ui, decimals);
                        let is_dust = dust_threshold > 0 && bal <= dust_threshold;
                        if bal == 0 || is_dust {
                            if bal == 0 {
                                warn!(mint = %order.intent.mint, "sell requested but token balance is 0; skipping");
                            } else {
                                warn!(
                                    mint = %order.intent.mint,
                                    bal,
                                    decimals,
                                    dust_threshold,
                                    min_sell_token_ui = cfg.min_sell_token_ui,
                                    "sell requested but token balance is dust; skipping"
                                );
                                let dedupe_key = if let Some(pos_id) = sell_pos_id.as_deref() {
                                    format!("sell_dust:{}:{}", order.intent.mint, pos_id)
                                } else {
                                    format!("sell_dust:{}", order.intent.mint)
                                };
                                let should_alert = redis
                                    .dedupe_signature(
                                        &dedupe_key,
                                        cfg.sell_dust_alert_ttl_seconds as usize,
                                    )
                                    .await
                                    .unwrap_or(true);
                                if should_alert {
                                    let alert = state::types::AlertEvent {
                                        ts: chrono::Utc::now(),
                                        kind: "bot_sell_dust_balance".into(),
                                        message: format!(
                                            "CLAWDIO BOT SELL stop: dust balance (treat as sold)\nmint: {}\nbal_base_units: {}\ndecimals: {}\nmin_sell_token_ui: {}\ndust_threshold_base_units: {}",
                                            order.intent.mint, bal, decimals, cfg.min_sell_token_ui, dust_threshold
                                        ),
                                    };
                                    let _ =
                                        redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                                }
                            }
                            if let Some(pos_id) = sell_pos_id.as_deref() {
                                let _ = redis.close_position(pos_id).await;
                            }
                            continue;
                        }
                        bal.to_string()
                    }
                };
                (order.intent.mint.clone(), WSOL_MINT.to_string(), amt)
            }
        };

        // Task 6 BUY attempt loop: max 2 attempts with a 2s gap and price-move guard.
        let mut attempt_no: u32 = 0;
        let mut first_quote_out: Option<u64> = None;
        let mut consecutive_jito_errors: u32 = 0;
        let mut submitted_buy_sigs: Vec<(Signature, chrono::DateTime<chrono::Utc>)> = Vec::new();
        let mut buy_confirmed = false;

        loop {
            attempt_no += 1;

            if order.intent.side == state::types::TradeSide::Buy && attempt_no > 2 {
                break;
            }

            if order.intent.side == state::types::TradeSide::Buy && attempt_no == 2 {
                // wait 2 seconds before retrying
                sleep(Duration::from_secs(2)).await;

                // If we already got tokens, do not retry.
                let bal = rpc
                    .get_token_balance_base_units(&user_public_key, &output_mint)
                    .await
                    .unwrap_or(0);
                if bal > 0 {
                    break;
                }
            }

            let strategy_slippage_bps = if order.intent.side == state::types::TradeSide::Buy {
                cfg.buy_strategy_slippage_bps
                    .get(&buy_strategy_id)
                    .copied()
                    .unwrap_or(cfg.slippage_bps)
            } else {
                cfg.slippage_bps
            };

            // Quote -> Swap build
            let t_quote_start = Utc::now();
            let quote = match jup
                .quote(
                    &input_mint,
                    &output_mint,
                    &amount_str.parse::<u64>().unwrap_or(0).to_string(),
                    strategy_slippage_bps,
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(mint = %output_mint, error = %e, "jupiter quote failed");
                    // Short, deduped alert (this is a common reason BUYs don't land).
                    let key = format!("buy_quote_fail:{}:{}", output_mint, attempt_no);
                    if redis.dedupe_signature(&key, 120).await.unwrap_or(false) {
                        let err_s: String = e.to_string().chars().take(140).collect();
                        let alert = state::types::AlertEvent {
                            ts: chrono::Utc::now(),
                            kind: "bot_buy_quote_failed".into(),
                            message: format!(
                                "CLAWDIO BOT BUY failed: quote token={} attempt={} slip_bps={} err={}",
                                output_mint, attempt_no, strategy_slippage_bps, err_s
                            ),
                        };
                        let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    }
                    break;
                }
            };
            let quote_ms = (Utc::now() - t_quote_start).num_milliseconds();
            let quote_out_amount_u64 = quote
                .get("outAmount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            if order.intent.side == state::types::TradeSide::Buy {
                if attempt_no == 1 {
                    first_quote_out = Some(quote_out_amount_u64);
                } else if let Some(first) = first_quote_out {
                    // Guard: only retry if token-per-SOL hasn't dropped >20% (outAmount >= 80% of first).
                    if quote_out_amount_u64 < (first.saturating_mul(80) / 100) {
                        let alert = state::types::AlertEvent {
                            ts: chrono::Utc::now(),
                            kind: "bot_skip_buy_price_moved".into(),
                            message: format!(
                                "CLAWDIO BOT SKIP BUY retry (price moved)\nmint: {}\nfirst_out: {}\nnow_out: {}",
                                output_mint, first, quote_out_amount_u64
                            ),
                        };
                        let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                        break;
                    }
                }
            }

            let t_swap_start = Utc::now();
            let swap = match jup
                .swap(quote, &user_public_key, cfg.wrap_and_unwrap_sol)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(mint = %output_mint, error = %e, "jupiter swap build failed");
                    let key = format!("buy_swap_build_fail:{}:{}", output_mint, attempt_no);
                    if redis.dedupe_signature(&key, 120).await.unwrap_or(false) {
                        let err_s: String = e.to_string().chars().take(140).collect();
                        let alert = state::types::AlertEvent {
                            ts: chrono::Utc::now(),
                            kind: "bot_buy_swap_failed".into(),
                            message: format!(
                                "CLAWDIO BOT BUY failed: swap-build token={} attempt={} slip_bps={} err={}",
                                output_mint, attempt_no, strategy_slippage_bps, err_s
                            ),
                        };
                        let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    }
                    break;
                }
            };
            let swap_build_ms = (Utc::now() - t_swap_start).num_milliseconds();

            let swap_tx = match swap.get("swapTransaction").and_then(|t| t.as_str()) {
                Some(s) => s,
                None => {
                    warn!(mint = %output_mint, "jupiter swap response missing swapTransaction");
                    let key = format!("buy_swap_missing_tx:{}", output_mint);
                    if redis.dedupe_signature(&key, 300).await.unwrap_or(false) {
                        let alert = state::types::AlertEvent {
                            ts: chrono::Utc::now(),
                            kind: "bot_buy_swap_failed".into(),
                            message: format!(
                                "CLAWDIO BOT BUY failed: swap missing tx token={} (Jupiter response)",
                                output_mint
                            ),
                        };
                        let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    }
                    break;
                }
            };

            if cfg.simulate_only || cfg.dry_run {
                // Block 6 behavior: simulate only. We pass sigVerify=false so a missing signature doesn't block the sim.
                let sim = match rpc.simulate_transaction_base64(swap_tx).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(mint = %output_mint, error = %e, "simulateTransaction failed");
                        let key = format!("buy_sim_rpc_fail:{}", output_mint);
                        if redis.dedupe_signature(&key, 300).await.unwrap_or(false) {
                            let err_s: String = e.to_string().chars().take(140).collect();
                            let alert = state::types::AlertEvent {
                                ts: chrono::Utc::now(),
                                kind: "bot_buy_sim_failed".into(),
                                message: format!(
                                    "CLAWDIO BOT BUY failed: simulate token={} err={}",
                                    output_mint, err_s
                                ),
                            };
                            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                        }
                        continue;
                    }
                };

                let sim_err = sim
                    .get("result")
                    .and_then(|r| r.get("value"))
                    .and_then(|v| v.get("err"));

                if sim_err.is_some_and(|e| !e.is_null()) {
                    warn!(
                        signature = %order.intent.signature,
                        mint = %output_mint,
                        err = %sim_err.unwrap(),
                        "simulation error (not sent)"
                    );
                    let key = format!("buy_sim_err:{}", output_mint);
                    if redis.dedupe_signature(&key, 300).await.unwrap_or(false) {
                        let alert = state::types::AlertEvent {
                            ts: chrono::Utc::now(),
                            kind: "bot_buy_sim_failed".into(),
                            message: format!(
                                "CLAWDIO BOT BUY failed: simulate err token={}",
                                output_mint
                            ),
                        };
                        let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    }
                    continue;
                }

                info!(
                    signature = %order.intent.signature,
                    mint = %output_mint,
                    venue = %order.intent.venue.clone().unwrap_or_else(|| "none".into()),
                    notional_sol = order.intent.notional_sol,
                    "simulation ok (not sent)"
                );

                // Block 9: alert on our simulated trades.
                let bot_msg = match order.intent.side {
                    state::types::TradeSide::Buy => format!(
                        "CLAWDIO BOT simulated BUY\ntoken: {}\nsize: {} SOL",
                        output_mint,
                        amount_str.parse::<u64>().unwrap_or(0) as f64 / 1e9
                    ),
                    state::types::TradeSide::Sell => format!(
                        "CLAWDIO BOT simulated SELL\ntoken: {}\nexpected_out: {:.6} SOL",
                        input_mint, order.intent.notional_sol
                    ),
                };
                let alert = state::types::AlertEvent {
                    ts: chrono::Utc::now(),
                    kind: "bot_trade_sim".into(),
                    message: bot_msg,
                };
                let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;

                // Record "paper position" on BUY simulation success (v1 testing).
                if order.intent.side == state::types::TradeSide::Buy {
                    // Best-effort: use the current quote outAmount (base units). If missing, record "0".
                    let pos = state::types::Position {
                        id: order.intent.signature.clone(),
                        wallet: order.intent.wallet.clone(),
                        mint: output_mint.clone(),
                        opened_at: chrono::Utc::now(),
                        buy_sig: order.intent.signature.clone(),
                        size_sol: amount_str.parse::<u64>().unwrap_or(0) as f64 / 1e9,
                        strategy_id: Some(sell_strategy_id.clone()),
                        buy_strategy_id: Some(buy_strategy_id.clone()),
                        sell_strategy_id: Some(sell_strategy_id.clone()),
                        token_amount: quote_out_amount_u64.to_string(),
                    };
                    let _ = redis.upsert_position(&pos).await;
                    let plan = exit_plan_for_strategy(
                        &pos.id,
                        &sell_strategy_id,
                        &cfg.sell_strategy_templates,
                        &cfg.default_exit_plan_template,
                    );
                    let _ = redis.set_position_exit_plan(&plan).await;
                } else {
                    // On sell simulation success, close position if this is an exit-generated signature.
                    if let Some(rest) = order.intent.signature.strip_prefix("exit:") {
                        let mut it = rest.split(':');
                        let pos_id = it.next().unwrap_or("");
                        let phase = it.next(); // "tp1" / "final" / <uuid> (legacy)
                        let is_partial = matches!(phase, Some("tp1"));
                        if !pos_id.is_empty() && !is_partial {
                            let _ = redis.close_position(pos_id).await;
                        }
                    }
                }
                continue;
            }

            // Block 7: sign + send via Jito bundles (always-on).
            if sending_enabled && keypair.is_none() {
                let alert = state::types::AlertEvent {
                ts: chrono::Utc::now(),
                kind: "bot_misconfig_keypair".into(),
                message: format!(
                    "CLAWDIO BOT misconfig: KEYPAIR_PATH is missing/invalid; cannot sign.\nKEYPAIR_PATH={}\nexpected: path to a Solana keypair JSON (64-byte array)\n",
                    cfg.keypair_path.clone().unwrap_or_else(|| "(not set)".into())
                ),
            };
                let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                warn!("sending enabled but keypair could not be loaded; cannot send");
                continue;
            }
            let Some(jito) = jito.as_ref() else {
                warn!("sending enabled but JITO_BUNDLE_ENDPOINT not set; cannot send");
                continue;
            };

            let keypair = keypair.as_ref().expect("checked above");

            let t_bh_start = Utc::now();
            let bh = match rpc.get_latest_blockhash().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "failed fetching latest blockhash");
                    break;
                }
            };
            let blockhash_ms = (Utc::now() - t_bh_start).num_milliseconds();

            let t_sign_start = Utc::now();
            let (signed_swap, sig) = match decode_and_sign_swap_tx(swap_tx, &keypair, bh) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "failed signing swap tx");
                    break;
                }
            };
            let sign_ms = (Utc::now() - t_sign_start).num_milliseconds();

            let mut bundle_txs: Vec<VersionedTransaction> = vec![signed_swap];
            if cfg.jito_tip_sol > 0.0 {
                let tip_lamports = (cfg.jito_tip_sol * 1_000_000_000.0).round() as u64;
                if tip_lamports > 0 {
                    bundle_txs.push(build_jito_tip_tx(&keypair, bh, tip_lamports));
                }
            }

            let mut bundle_base58: Vec<String> = Vec::with_capacity(bundle_txs.len());
            for t in &bundle_txs {
                bundle_base58.push(encode_tx_base58(t)?);
            }

            // Task 6: Jito-first, fallback after 2 consecutive Jito errors.
            let submitted_at = chrono::Utc::now();
            let mut via = "jito";

            // Track consecutive Jito errors across BUY attempts.
            // (Declared outside the attempt loop; see below.)
            let t_submit_start = Utc::now();
            if let Err(e) = jito.send_bundle_base58(bundle_base58).await {
                consecutive_jito_errors = consecutive_jito_errors.saturating_add(1);
                warn!(error=%e, consecutive_jito_errors, "jito submit failed");
                // If Jito is rate-limiting (429), fall back immediately for BUYs.
                // Waiting for a second failure just wastes the short reaction window.
                let jito_rate_limited = is_jito_rate_limited(&e);
                if order.intent.side == state::types::TradeSide::Buy && jito_rate_limited {
                    let tx_b64 = encode_tx_base64(&bundle_txs[0])?;
                    match rpc.send_transaction_base64(&tx_b64).await {
                        Ok(_) => {
                            via = "rpc_fallback";
                        }
                        Err(e2) => {
                            let alert = state::types::AlertEvent {
                                ts: submitted_at,
                                kind: "bot_submit_failed".into(),
                                message: format!(
                                    "CLAWDIO BOT submit failed (Jito rate-limited + RPC fallback)\nside: {:?}\nmint: {}\njito_errs: {}\nerr: {}",
                                    order.intent.side, output_mint, consecutive_jito_errors, e2
                                ),
                            };
                            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                            // For BUYs, allow attempt #2 after 2s.
                            continue;
                        }
                    }
                } else if consecutive_jito_errors >= 2 {
                    let tx_b64 = encode_tx_base64(&bundle_txs[0])?;
                    match rpc.send_transaction_base64(&tx_b64).await {
                        Ok(_) => {
                            via = "rpc_fallback";
                        }
                        Err(e2) => {
                            let alert = state::types::AlertEvent {
                                ts: submitted_at,
                                kind: "bot_submit_failed".into(),
                                message: format!(
                                    "CLAWDIO BOT submit failed (Jito + RPC fallback)\nside: {:?}\nmint: {}\njito_errs: {}\nerr: {}",
                                    order.intent.side, output_mint, consecutive_jito_errors, e2
                                ),
                            };
                            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                            break;
                        }
                    }
                } else {
                    // For BUY: allow attempt #2 after 2s (and then RPC fallback after 2 consecutive Jito errors).
                    // For SELL: retry is handled by the sell retry loop (outside this attempt loop).
                    if order.intent.side == state::types::TradeSide::Buy {
                        continue;
                    }
                    break;
                }
            } else {
                // Successful Jito submission resets the consecutive error counter.
                consecutive_jito_errors = 0;
            }
            let submit_ms = (Utc::now() - t_submit_start).num_milliseconds();

            info!(
                swap_sig = %sig,
                mint = %output_mint,
                via = %via,
                quote_ms,
                swap_build_ms,
                blockhash_ms,
                sign_ms,
                submit_ms,
                "submitted swap"
            );

            // Task 2: latency report on submit.
            let obs = order.intent.observed_at;
            let cls = order.intent.classified_at;
            let execq = order.exec_enqueued_at.unwrap_or_else(chrono::Utc::now);
            let _o2c = obs.and_then(|o| cls.map(|c| (c - o).num_milliseconds()));
            let _o2q = obs.map(|o| (execq - o).num_milliseconds());
            let _o2s = obs.map(|o| (submitted_at - o).num_milliseconds());

            let alert = state::types::AlertEvent {
                ts: submitted_at,
                kind: "bot_submitted".into(),
                message: format!("CLAWDIO BOT submitted BUY for token {}...", output_mint),
            };
            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
            submitted_buy_sigs.push((sig, submitted_at));

            // Confirmation polling (best-effort). Shorter for BUY attempts.
            let max_polls = if order.intent.side == state::types::TradeSide::Buy {
                10
            } else {
                40
            };
            for _ in 0..max_polls {
                match rpc.get_signature_status(&sig).await {
                    Ok(Some(st)) => {
                        let err = st.get("err");
                        if err.is_some_and(|e| !e.is_null()) {
                            warn!(swap_sig = %sig, err = %err.unwrap(), "swap failed");
                            let alert = state::types::AlertEvent {
                                ts: chrono::Utc::now(),
                                kind: "bot_failed".into(),
                                message: format!(
                                "CLAWDIO BOT failed\ntoken: {}\nerr: {}\nlat_ms submitted→fail={}",
                                output_mint,
                                err.unwrap(),
                                (chrono::Utc::now() - submitted_at).num_milliseconds()
                            ),
                            };
                            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                            break;
                        }
                        if let Some(cs) = st.get("confirmationStatus").and_then(|s| s.as_str()) {
                            if !is_strictly_confirmed_status(cs) {
                                sleep(Duration::from_millis(200)).await;
                                continue;
                            }
                            info!(swap_sig = %sig, confirmation_status = %cs, "swap confirmed");
                            handle_confirmed_buy(
                                &rpc,
                                &redis,
                                birdeye.as_ref(),
                                cfg.helius_api_key.as_deref(),
                                &cfg.helius_api_base_url,
                                &user_public_key,
                                &output_mint,
                                &sig,
                                submitted_at,
                                order.intent.observed_at,
                                &order.intent.signature,
                                &buy_strategy_id,
                                &sell_strategy_id,
                                order.intent.notional_sol,
                                order.intent.token_delta_base_units.as_deref(),
                                cfg.buy_strategy_max_fill_delay_ms
                                    .get(&buy_strategy_id)
                                    .copied()
                                    .unwrap_or(3_000),
                                cfg.buy_strategy_max_price_above_alpha_pct
                                    .get(&buy_strategy_id)
                                    .copied()
                                    .unwrap_or(10.0),
                                &cfg.sell_strategy_templates,
                                &cfg.default_exit_plan_template,
                                cfg.openclaw_event_url.as_deref(),
                                cfg.openclaw_api_key.as_deref(),
                            )
                            .await?;
                            buy_confirmed = true;
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => warn!(swap_sig = %sig, error = %e, "status poll error"),
                }
                sleep(Duration::from_millis(200)).await;
            }

            if buy_confirmed {
                break;
            }

            // BUY attempt 1 falls through to attempt 2; attempt 2 ends.
            if order.intent.side == state::types::TradeSide::Buy && attempt_no >= 2 {
                break;
            }
        } // attempt loop

        // Post-submit reconciliation for BUYs:
        // if we submitted at least one tx but did not reach confirmed/finalized in the short poll,
        // keep checking signature statuses for a longer window before declaring failure.
        if order.intent.side == state::types::TradeSide::Buy
            && !buy_confirmed
            && !submitted_buy_sigs.is_empty()
        {
            let reconcile_deadline = chrono::Utc::now() + chrono::Duration::seconds(45);
            while chrono::Utc::now() < reconcile_deadline {
                for (sig, submitted_at) in &submitted_buy_sigs {
                    let st = match rpc.get_signature_status(sig).await {
                        Ok(Some(v)) => v,
                        Ok(None) => continue,
                        Err(e) => {
                            warn!(swap_sig = %sig, error = %e, "reconcile status poll error");
                            continue;
                        }
                    };

                    if let Some(err) = st.get("err").filter(|e| !e.is_null()) {
                        warn!(swap_sig = %sig, err = %err, "reconcile observed failed swap");
                        continue;
                    }

                    let Some(cs) = st.get("confirmationStatus").and_then(|s| s.as_str()) else {
                        continue;
                    };
                    if !is_strictly_confirmed_status(cs) {
                        continue;
                    }

                    handle_confirmed_buy(
                        &rpc,
                        &redis,
                        birdeye.as_ref(),
                        cfg.helius_api_key.as_deref(),
                        &cfg.helius_api_base_url,
                        &user_public_key,
                        &output_mint,
                        sig,
                        *submitted_at,
                        order.intent.observed_at,
                        &order.intent.signature,
                        &buy_strategy_id,
                        &sell_strategy_id,
                        order.intent.notional_sol,
                        order.intent.token_delta_base_units.as_deref(),
                        cfg.buy_strategy_max_fill_delay_ms
                            .get(&buy_strategy_id)
                            .copied()
                            .unwrap_or(3_000),
                        cfg.buy_strategy_max_price_above_alpha_pct
                            .get(&buy_strategy_id)
                            .copied()
                            .unwrap_or(10.0),
                        &cfg.sell_strategy_templates,
                        &cfg.default_exit_plan_template,
                        cfg.openclaw_event_url.as_deref(),
                        cfg.openclaw_api_key.as_deref(),
                    )
                    .await?;
                    buy_confirmed = true;
                    break;
                }

                if buy_confirmed {
                    break;
                }
                sleep(Duration::from_millis(500)).await;
            }

            if !buy_confirmed {
                let alert = state::types::AlertEvent {
                    ts: chrono::Utc::now(),
                    kind: "bot_unconfirmed_timeout".into(),
                    message: format!(
                        "CLAWDIO BOT BUY unresolved after reconciliation window\nmint: {}\nattempts_submitted: {}",
                        output_mint,
                        submitted_buy_sigs.len()
                    ),
                };
                let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
            }
        }
    }
}

fn position_from_tx_meta(
    owner: &str,
    mint: &str,
    size_sol: f64,
    buy_sig: &str,
    get_tx: &serde_json::Value,
) -> Option<state::types::Position> {
    let result = get_tx.get("result")?;
    let meta = result.get("meta")?;
    let pre = meta
        .get("preTokenBalances")?
        .as_array()
        .cloned()
        .unwrap_or_default();
    let post = meta
        .get("postTokenBalances")?
        .as_array()
        .cloned()
        .unwrap_or_default();

    // Sum across all token accounts owned by `owner` for this mint.
    let mut pre_amt: i128 = 0;
    let mut post_amt: i128 = 0;

    for e in pre {
        if e.get("owner")?.as_str()? != owner {
            continue;
        }
        if e.get("mint")?.as_str()? != mint {
            continue;
        }
        let a = e
            .get("uiTokenAmount")?
            .get("amount")?
            .as_str()?
            .parse::<i128>()
            .ok()?;
        pre_amt = pre_amt.saturating_add(a);
    }
    for e in post {
        if e.get("owner")?.as_str()? != owner {
            continue;
        }
        if e.get("mint")?.as_str()? != mint {
            continue;
        }
        let a = e
            .get("uiTokenAmount")?
            .get("amount")?
            .as_str()?
            .parse::<i128>()
            .ok()?;
        post_amt = post_amt.saturating_add(a);
    }
    let delta = post_amt.saturating_sub(pre_amt);
    if delta <= 0 {
        return None;
    }

    Some(state::types::Position {
        id: buy_sig.to_string(),
        wallet: owner.to_string(),
        mint: mint.to_string(),
        opened_at: chrono::Utc::now(),
        buy_sig: buy_sig.to_string(),
        size_sol,
        strategy_id: None,
        buy_strategy_id: None,
        sell_strategy_id: None,
        token_amount: delta.to_string(),
    })
}

fn is_strictly_confirmed_status(status: &str) -> bool {
    matches!(status, "confirmed" | "finalized")
}

fn format_signed_sol(delta_sol: f64) -> String {
    if delta_sol.is_sign_negative() {
        format!("-{:.3} SOL", delta_sol.abs())
    } else {
        format!("+{:.3} SOL", delta_sol)
    }
}

async fn maybe_start_buy_dip_setup(
    redis: &RedisState,
    birdeye: Option<&BirdeyeClient>,
    cfg: &ExecutorConfig,
    intent: &TradeIntent,
    buy_strategy_id: &str,
) -> Result<()> {
    if redis.is_bot_off().await.unwrap_or(false) {
        info!(mint = %intent.mint, wallet = %intent.wallet, "bot off; skipping buy_dip setup");
        return Ok(());
    }

    let setup_id = format!("{}:{}", intent.wallet, intent.mint);
    let dedupe_key = format!("buy_dip_request:{setup_id}");
    if !redis
        .dedupe_signature(&dedupe_key, 900)
        .await
        .unwrap_or(false)
    {
        return Ok(());
    }

    let token_info = token_info_cached(redis, birdeye, &intent.mint, 60).await;
    let Some(alpha_market_cap_usd) = token_info.as_ref().and_then(|t| t.market_cap_usd) else {
        let alert = state::types::AlertEvent {
            ts: Utc::now(),
            kind: "buy_dip_skipped_mcap_unknown".into(),
            message: format!(
                "CLAWDIO BOT BUY DIP skipped\nToken: {}\nReason: alpha entry market cap unavailable",
                intent.mint
            ),
        };
        let _ = enqueue_alert_if_bot_active(redis, &alert).await;
        return Ok(());
    };
    if !(10_000.0..=100_000.0).contains(&alpha_market_cap_usd) {
        let alert = state::types::AlertEvent {
            ts: Utc::now(),
            kind: "buy_dip_skipped_mcap_bounds".into(),
            message: format!(
                "CLAWDIO BOT BUY DIP skipped\nToken: {}\nAlpha entry market cap: ${}\nRequired range: $10K-$100K",
                intent.mint,
                format_compact(alpha_market_cap_usd)
            ),
        };
        let _ = enqueue_alert_if_bot_active(redis, &alert).await;
        return Ok(());
    }

    let Some(url) = cfg.openclaw_event_url.as_deref() else {
        let alert = state::types::AlertEvent {
            ts: Utc::now(),
            kind: "buy_dip_missing_openclaw".into(),
            message: format!(
                "CLAWDIO BOT BUY DIP skipped\nToken: {}\nReason: OPENCLAW_EVENT_URL not configured",
                intent.mint
            ),
        };
        let _ = enqueue_alert_if_bot_active(redis, &alert).await;
        return Ok(());
    };

    let analysis_id = format!("oc:buydip:{}:{}", setup_id, Utc::now().timestamp_millis());
    let event = OpenClawBuyDipRequestedEvent {
        event_type: "buy_dip_watch_requested",
        analysis_id: analysis_id.clone(),
        chain: "solana",
        setup_id,
        wallet: &intent.wallet,
        mint: &intent.mint,
        strategy_id: buy_strategy_id,
        alpha_observed_at: intent.observed_at.unwrap_or_else(Utc::now),
        alpha_notional_sol: intent.notional_sol,
        alpha_market_cap_usd,
        valid_for_seconds: 900,
        token: OpenClawTokenInfo {
            symbol: token_info.as_ref().and_then(|info| info.symbol.as_deref()),
            name: token_info.as_ref().and_then(|info| info.name.as_deref()),
            market_cap_usd: token_info.as_ref().and_then(|info| info.market_cap_usd),
            description: token_info
                .as_ref()
                .and_then(|info| info.description.as_deref()),
            description_source: token_info
                .as_ref()
                .and_then(|info| info.description_source.as_deref()),
            description_source_url: token_info
                .as_ref()
                .and_then(|info| info.description_source_url.as_deref()),
            pumpfun_url: format!("https://pump.fun/coin/{}", intent.mint),
            dexscreener_url: format!("https://dexscreener.com/solana/{}", intent.mint),
        },
    };

    send_openclaw_buy_dip_event(url, cfg.openclaw_api_key.as_deref(), &event).await?;

    let alert = state::types::AlertEvent {
        ts: Utc::now(),
        kind: "buy_dip_analysis_requested".into(),
        message: format!(
            "CLAWDIO BOT BUY DIP analysis requested\nToken: {}\nAlpha entry market cap: ${}\nBudget: 1.0 SOL\nWaiting for OpenClaw buy points",
            intent.mint,
            format_compact(alpha_market_cap_usd)
        ),
    };
    let _ = enqueue_alert_if_bot_active(redis, &alert).await;
    Ok(())
}

async fn run_buy_dip_worker(cfg: ExecutorConfig, redis: RedisState) -> Result<()> {
    let birdeye = cfg
        .birdeye_api_key
        .clone()
        .and_then(|k| BirdeyeClient::new(k).ok());

    loop {
        if redis.is_bot_off().await.unwrap_or(false) {
            sleep(Duration::from_millis(500)).await;
            continue;
        }

        for _ in 0..20 {
            let raw = match redis.pop_buy_dip_setup_update().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "redis unavailable (buy_dip updates); retrying");
                    break;
                }
            };
            let Some(raw) = raw else { break };
            let update: state::types::BuyDipSetupUpdate = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, payload = %raw, "invalid buy_dip setup update");
                    continue;
                }
            };
            redis.upsert_buy_dip_setup(&update.setup).await?;
            let alert = state::types::AlertEvent {
                ts: Utc::now(),
                kind: if update.setup.cancelled {
                    "buy_dip_discarded".into()
                } else {
                    "buy_dip_armed".into()
                },
                message: if update.setup.cancelled {
                    format!(
                        "CLAWDIO BOT BUY DIP discarded\nToken: {}\nReason: {}",
                        update.setup.mint,
                        update
                            .setup
                            .analysis_summary
                            .clone()
                            .unwrap_or_else(|| "OpenClaw rejected the setup.".into())
                    )
                } else {
                    format!(
                        "CLAWDIO BOT BUY DIP order set\nToken: {}\nBuy 1: ${}\nBuy 2: ${}\nToken analysis:\n{}",
                        update.setup.mint,
                        update
                            .setup
                            .buy_point_1_market_cap_usd
                            .map(format_compact)
                            .unwrap_or_else(|| "N/A".into()),
                        update
                            .setup
                            .buy_point_2_market_cap_usd
                            .map(format_compact)
                            .unwrap_or_else(|| "N/A".into()),
                        update
                            .setup
                            .analysis_summary
                            .clone()
                            .unwrap_or_else(|| "OpenClaw armed the setup.".into())
                    )
                },
            };
            let _ = enqueue_alert_if_bot_active(&redis, &alert).await;
        }

        for _ in 0..20 {
            let raw = match redis.pop_alpha_playbook_add_update().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "redis unavailable (alpha_playbook add updates); retrying");
                    break;
                }
            };
            let Some(raw) = raw else { break };
            let update: state::types::AlphaPlaybookAddSetupUpdate = match serde_json::from_str(&raw)
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, payload = %raw, "invalid alpha_playbook add update");
                    continue;
                }
            };
            redis.upsert_alpha_playbook_add_setup(&update.setup).await?;
            let alert = state::types::AlertEvent {
                ts: Utc::now(),
                kind: if update.setup.cancelled {
                    "alpha_playbook_add_discarded".into()
                } else {
                    "alpha_playbook_add_armed".into()
                },
                message: if update.setup.cancelled {
                    format!(
                        "CLAWDIO BOT alpha_playbook add discarded\nToken: {}\nReason: {}",
                        update.setup.mint,
                        update
                            .setup
                            .analysis_summary
                            .clone()
                            .unwrap_or_else(|| "OpenClaw rejected the add setup.".into())
                    )
                } else {
                    format!(
                        "CLAWDIO BOT alpha_playbook add armed\nToken: {}\nTarget mcap: ${}\nBudget: {:.3} SOL",
                        update.setup.mint,
                        format_compact(update.setup.target_market_cap_usd),
                        update.setup.budget_sol
                    )
                },
            };
            let _ = enqueue_alert_if_bot_active(&redis, &alert).await;
        }

        let active_ids = match redis.list_active_buy_dip_setup_ids().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (buy_dip active setups); retrying");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        for setup_id in active_ids {
            let Some(mut setup) = redis.get_buy_dip_setup(&setup_id).await? else {
                let _ = redis.remove_buy_dip_setup_from_active(&setup_id).await;
                continue;
            };
            if setup.cancelled || setup.expires_at <= Utc::now() {
                if !setup.cancelled && setup.expires_at <= Utc::now() {
                    setup.cancelled = true;
                    setup.cancel_reason = Some("expired".into());
                    setup.updated_at = Utc::now();
                    let alert = state::types::AlertEvent {
                        ts: Utc::now(),
                        kind: "buy_dip_expired".into(),
                        message: format!(
                            "CLAWDIO BOT BUY DIP expired\nToken: {}\nWindow: 15 minutes",
                            setup.mint
                        ),
                    };
                    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
                    let _ = redis.upsert_buy_dip_setup(&setup).await;
                } else {
                    let _ = redis.remove_buy_dip_setup_from_active(&setup.id).await;
                }
                continue;
            }

            let current_market_cap_usd =
                token_info_cached(&redis, birdeye.as_ref(), &setup.mint, 20)
                    .await
                    .and_then(|info| info.market_cap_usd);
            let Some(current_market_cap_usd) = current_market_cap_usd else {
                continue;
            };

            if !setup.entry1_filled
                && setup.entry2_armed
                && setup
                    .buy_point_2_market_cap_usd
                    .is_some_and(|target| current_market_cap_usd <= target)
            {
                enqueue_buy_dip_exec_order(
                    &redis,
                    &setup,
                    "bp2_direct",
                    setup.total_budget_sol,
                    Utc::now(),
                )
                .await?;
                setup.entry1_armed = false;
                setup.entry2_armed = false;
                setup.updated_at = Utc::now();
                redis.upsert_buy_dip_setup(&setup).await?;
                let _ = redis.remove_buy_dip_setup_from_active(&setup.id).await;
                continue;
            }

            if !setup.entry1_filled
                && setup.entry1_armed
                && setup
                    .buy_point_1_market_cap_usd
                    .is_some_and(|target| current_market_cap_usd <= target)
            {
                enqueue_buy_dip_exec_order(
                    &redis,
                    &setup,
                    "bp1",
                    setup.entry1_budget_sol,
                    Utc::now(),
                )
                .await?;
                setup.entry1_armed = false;
                setup.updated_at = Utc::now();
                redis.upsert_buy_dip_setup(&setup).await?;
                continue;
            }

            if setup.entry1_filled
                && !setup.entry2_filled
                && setup.entry2_armed
                && setup
                    .buy_point_2_market_cap_usd
                    .is_some_and(|target| current_market_cap_usd <= target)
            {
                enqueue_buy_dip_exec_order(
                    &redis,
                    &setup,
                    "bp2",
                    setup.entry2_budget_sol,
                    Utc::now(),
                )
                .await?;
                setup.entry2_armed = false;
                setup.updated_at = Utc::now();
                redis.upsert_buy_dip_setup(&setup).await?;
                let _ = redis.remove_buy_dip_setup_from_active(&setup.id).await;
            }
        }

        let add_setup_ids = match redis.list_active_alpha_playbook_add_setup_ids().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (alpha_playbook add setups); retrying");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        for setup_id in add_setup_ids {
            let Some(mut setup) = redis.get_alpha_playbook_add_setup(&setup_id).await? else {
                let _ = redis.remove_alpha_playbook_add_setup_from_active(&setup_id).await;
                continue;
            };
            if setup.cancelled || setup.filled || setup.expires_at <= Utc::now() {
                if !setup.cancelled && !setup.filled && setup.expires_at <= Utc::now() {
                    setup.cancelled = true;
                    setup.cancel_reason = Some("expired".into());
                    setup.updated_at = Utc::now();
                    let _ = redis.upsert_alpha_playbook_add_setup(&setup).await;
                } else {
                    let _ = redis.remove_alpha_playbook_add_setup_from_active(&setup.id).await;
                }
                continue;
            }

            let current_market_cap_usd = token_info_cached(&redis, birdeye.as_ref(), &setup.mint, 20)
                .await
                .and_then(|info| info.market_cap_usd);
            let Some(current_market_cap_usd) = current_market_cap_usd else {
                continue;
            };
            if current_market_cap_usd > setup.target_market_cap_usd {
                continue;
            }

            enqueue_alpha_playbook_exec_order(
                &redis,
                &setup.wallet,
                &setup.mint,
                setup.budget_sol,
                "alpha_playbook:openclaw_add",
                "alpha_playbook_openclaw_add",
            )
            .await?;
            setup.armed = false;
            setup.filled = true;
            setup.updated_at = Utc::now();
            redis.upsert_alpha_playbook_add_setup(&setup).await?;
            let _ = redis.remove_alpha_playbook_add_setup_from_active(&setup.id).await;
        }

        let watch_mints = match redis.list_active_alpha_playbook_watch_mints().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (alpha_playbook watch mints); retrying");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        for mint in watch_mints {
            let Some(mut watch) = redis.get_alpha_playbook_watch_state(&mint).await? else {
                let _ = redis.clear_alpha_playbook_watch_state(&mint).await;
                continue;
            };
            if watch.cancelled || watch.expires_at.is_some_and(|expiry| expiry <= Utc::now()) {
                watch.cancelled = true;
                if watch.cancel_reason.is_none() {
                    watch.cancel_reason = Some("expired".into());
                }
                watch.updated_at = Utc::now();
                redis.set_alpha_playbook_watch_state(&watch).await?;
                continue;
            }

            let current_market_cap_usd = token_info_cached(&redis, birdeye.as_ref(), &mint, 20)
                .await
                .and_then(|info| info.market_cap_usd);
            let Some(current_market_cap_usd) = current_market_cap_usd else {
                continue;
            };

            if watch
                .retrace_50_level_usd
                .is_some_and(|target| current_market_cap_usd <= target)
            {
                let wallet = watch.first_wallet.clone().unwrap_or_default();
                enqueue_alpha_playbook_exec_order(
                    &redis,
                    &wallet,
                    &mint,
                    0.3,
                    "alpha_playbook:old_dormant_retrace_50",
                    "alpha_playbook_old_dormant_retrace_50",
                )
                .await?;
                watch.cancelled = true;
                watch.cancel_reason = Some("filled_retrace_50".into());
                watch.updated_at = Utc::now();
                redis.set_alpha_playbook_watch_state(&watch).await?;
                if let Some(mut token_state) = redis.get_alpha_playbook_token_state(&mint).await? {
                    token_state.watch_active = false;
                    token_state.chosen_size_sol = Some(0.3);
                    token_state.updated_at = Utc::now();
                    redis.set_alpha_playbook_token_state(&token_state).await?;
                }
                continue;
            }

            if watch
                .retrace_15_level_usd
                .is_some_and(|target| current_market_cap_usd <= target)
            {
                let is_new_low = watch
                    .lowest_market_cap_usd
                    .is_none_or(|lowest| current_market_cap_usd < lowest);
                if is_new_low {
                    watch.lowest_market_cap_usd = Some(current_market_cap_usd);
                    watch.last_low_observed_at = Some(Utc::now());
                    watch.stabilization_deadline_at = Some(Utc::now() + chrono::Duration::seconds(5));
                    watch.updated_at = Utc::now();
                    redis.set_alpha_playbook_watch_state(&watch).await?;
                    continue;
                }

                if watch
                    .stabilization_deadline_at
                    .is_some_and(|deadline| Utc::now() >= deadline)
                {
                    let wallet = watch.first_wallet.clone().unwrap_or_default();
                    enqueue_alpha_playbook_exec_order(
                        &redis,
                        &wallet,
                        &mint,
                        0.3,
                        "alpha_playbook:old_dormant_stabilized",
                        "alpha_playbook_old_dormant_stabilized",
                    )
                    .await?;
                    watch.cancelled = true;
                    watch.cancel_reason = Some("filled_stabilized".into());
                    watch.updated_at = Utc::now();
                    redis.set_alpha_playbook_watch_state(&watch).await?;
                    if let Some(mut token_state) = redis.get_alpha_playbook_token_state(&mint).await?
                    {
                        token_state.watch_active = false;
                        token_state.chosen_size_sol = Some(0.3);
                        token_state.updated_at = Utc::now();
                        redis.set_alpha_playbook_token_state(&token_state).await?;
                    }
                }
            }
        }

        sleep(Duration::from_millis(350)).await;
    }
}

async fn enqueue_buy_dip_exec_order(
    redis: &RedisState,
    setup: &state::types::BuyDipSetup,
    stage: &str,
    requested_buy_sol: f64,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let intent = TradeIntent {
        signature: format!("buy_dip:{}:{}:{}", setup.id, stage, now.timestamp_millis()),
        slot: 0,
        wallet: setup.wallet.clone(),
        side: TradeSide::Buy,
        mint: setup.mint.clone(),
        notional_sol: setup.alpha_notional_sol,
        venue: Some(format!("buy_dip:{stage}")),
        observed_at: Some(now),
        classified_at: Some(now),
        amount_in_base_units: None,
        token_delta_base_units: None,
        requested_buy_sol: Some(requested_buy_sol),
        source_wallet_exit_full: false,
        source_wallet_sold_pct: None,
        created_at: now,
    };
    let order = ExecOrder {
        intent,
        filter_reason: format!("buy_dip:{stage}"),
        exec_enqueued_at: Some(now),
    };
    redis
        .enqueue_exec_order(&serde_json::to_string(&order)?)
        .await?;
    Ok(())
}

async fn aggregate_buy_dip_position(
    redis: &RedisState,
    mut new_pos: state::types::Position,
) -> Result<state::types::Position> {
    let ids = redis.list_open_position_ids().await?;
    for id in ids {
        let Some(existing) = redis.get_position(&id).await? else {
            continue;
        };
        if existing.wallet != new_pos.wallet || existing.mint != new_pos.mint {
            continue;
        }
        if existing.buy_strategy_id.as_deref() != Some("buy_dip") {
            continue;
        }
        let existing_amount = existing.token_amount.parse::<u128>().unwrap_or(0);
        let new_amount = new_pos.token_amount.parse::<u128>().unwrap_or(0);
        new_pos.id = existing.id;
        new_pos.buy_sig = existing.buy_sig;
        new_pos.opened_at = existing.opened_at;
        new_pos.size_sol = existing.size_sol + new_pos.size_sol;
        new_pos.token_amount = existing_amount.saturating_add(new_amount).to_string();
        break;
    }
    Ok(new_pos)
}

async fn aggregate_alpha_playbook_position(
    redis: &RedisState,
    mut new_pos: state::types::Position,
) -> Result<state::types::Position> {
    let ids = redis.list_open_position_ids().await?;
    for id in ids {
        let Some(existing) = redis.get_position(&id).await? else {
            continue;
        };
        if existing.wallet != new_pos.wallet || existing.mint != new_pos.mint {
            continue;
        }
        if existing.buy_strategy_id.as_deref() != Some("alpha_playbook") {
            continue;
        }
        let existing_amount = existing.token_amount.parse::<u128>().unwrap_or(0);
        let new_amount = new_pos.token_amount.parse::<u128>().unwrap_or(0);
        new_pos.id = existing.id;
        new_pos.buy_sig = existing.buy_sig;
        new_pos.opened_at = existing.opened_at;
        new_pos.size_sol = existing.size_sol + new_pos.size_sol;
        new_pos.token_amount = existing_amount.saturating_add(new_amount).to_string();
        break;
    }
    Ok(new_pos)
}

fn alpha_playbook_position_plan(
    scenario: AlphaPlaybookScenario,
    position_id: &str,
) -> state::types::PositionExitPlan {
    match scenario {
        AlphaPlaybookScenario::NewHot => state::types::PositionExitPlan {
            position_id: position_id.to_string(),
            mode: "alpha_playbook_new_hot".into(),
            source: "alpha_playbook:new_hot".into(),
            take_profit_pct: Some(100.0),
            stop_loss_pct: Some(30.0),
            sell_percent_on_take_profit: Some(50.0),
            sell_percent_on_stop_loss: Some(100.0),
            notes: Some("Sell 50% at 2x, then clip the rest over time or further spikes.".into()),
            tp1_upside_pct: None,
            moonbag_target_multiple: None,
            stale_exit_after_seconds: None,
            stale_exit_requires_non_loss: None,
            updated_at: Utc::now(),
        },
        AlphaPlaybookScenario::OldDormantSpiked => state::types::PositionExitPlan {
            position_id: position_id.to_string(),
            mode: "alpha_playbook_old_dormant".into(),
            source: "alpha_playbook:old_dormant_spiked".into(),
            take_profit_pct: Some(50.0),
            stop_loss_pct: Some(33.0),
            sell_percent_on_take_profit: Some(50.0),
            sell_percent_on_stop_loss: Some(100.0),
            notes: Some("Take 50% at +50%, then follow tracked-wallet full exit or stale non-loss.".into()),
            tp1_upside_pct: None,
            moonbag_target_multiple: None,
            stale_exit_after_seconds: Some(7200),
            stale_exit_requires_non_loss: Some(true),
            updated_at: Utc::now(),
        },
        AlphaPlaybookScenario::MidTrend => state::types::PositionExitPlan {
            position_id: position_id.to_string(),
            mode: "basic".into(),
            source: "alpha_playbook:mid_trend".into(),
            take_profit_pct: Some(40.0),
            stop_loss_pct: None,
            sell_percent_on_take_profit: Some(100.0),
            sell_percent_on_stop_loss: None,
            notes: Some("Baseline mid_trend exit while OpenClaw may override.".into()),
            tp1_upside_pct: None,
            moonbag_target_multiple: None,
            stale_exit_after_seconds: None,
            stale_exit_requires_non_loss: None,
            updated_at: Utc::now(),
        },
    }
}

async fn update_buy_dip_setup_after_fill(
    redis: &RedisState,
    original_intent_signature: &str,
    pos: &state::types::Position,
) -> Result<()> {
    let Some(rest) = original_intent_signature.strip_prefix("buy_dip:") else {
        return Ok(());
    };
    let mut parts = rest.split(':');
    let setup_id = parts.next().unwrap_or_default();
    let stage = parts.next().unwrap_or_default();
    if setup_id.is_empty() || stage.is_empty() {
        return Ok(());
    }
    let Some(mut setup) = redis.get_buy_dip_setup(setup_id).await? else {
        return Ok(());
    };
    match stage {
        "bp1" => {
            setup.entry1_filled = true;
            setup.updated_at = Utc::now();
            redis.upsert_buy_dip_setup(&setup).await?;
            let alert = state::types::AlertEvent {
                ts: Utc::now(),
                kind: "buy_dip_entry1_filled".into(),
                message: format!(
                    "CLAWDIO BOT BUY DIP buy point 1 hit!\nToken: {}\nNext buy point: ${}",
                    setup.mint,
                    setup
                        .buy_point_2_market_cap_usd
                        .map(format_compact)
                        .unwrap_or_else(|| "N/A".into())
                ),
            };
            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
        }
        "bp2" => {
            setup.entry2_filled = true;
            setup.updated_at = Utc::now();
            redis.upsert_buy_dip_setup(&setup).await?;
            let alert = state::types::AlertEvent {
                ts: Utc::now(),
                kind: "buy_dip_entry2_filled".into(),
                message: format!(
                    "CLAWDIO BOT BUY DIP buy point 2 hit!\nToken: {}\nTotal size now: {:.3} SOL",
                    setup.mint, pos.size_sol
                ),
            };
            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
        }
        "bp2_direct" => {
            setup.entry1_filled = true;
            setup.entry2_filled = true;
            setup.updated_at = Utc::now();
            redis.upsert_buy_dip_setup(&setup).await?;
            let alert = state::types::AlertEvent {
                ts: Utc::now(),
                kind: "buy_dip_direct_bp2_filled".into(),
                message: format!(
                    "CLAWDIO BOT BUY DIP direct fill at buy point 2\nToken: {}\nSize: {:.3} SOL",
                    setup.mint, pos.size_sol
                ),
            };
            let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_confirmed_buy(
    rpc: &RpcClient,
    redis: &RedisState,
    birdeye: Option<&BirdeyeClient>,
    helius_api_key: Option<&str>,
    helius_api_base_url: &str,
    user_public_key: &str,
    output_mint: &str,
    sig: &Signature,
    submitted_at: chrono::DateTime<chrono::Utc>,
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
    original_intent_signature: &str,
    buy_strategy_id: &str,
    sell_strategy_id: &str,
    alpha_notional_sol: f64,
    alpha_token_amount_base_units: Option<&str>,
    max_fill_delay_ms: i64,
    max_price_above_alpha_pct: f64,
    sell_strategy_templates: &HashMap<String, state::types::ExitPlanTemplate>,
    default_exit_plan_template: &state::types::ExitPlanTemplate,
    openclaw_event_url: Option<&str>,
    openclaw_api_key: Option<&str>,
) -> Result<()> {
    let confirmed_at = chrono::Utc::now();
    let alert = state::types::AlertEvent {
        ts: confirmed_at,
        kind: "bot_confirmed".into(),
        message: format!(
            "CLAWDIO BOT BUY confirmed\ntoken: {}\nstatus: confirmed/finalized\nlat_ms submitted→confirmed={} observed→confirmed={:?}",
            output_mint,
            (confirmed_at - submitted_at).num_milliseconds(),
            observed_at.map(|o| (confirmed_at - o).num_milliseconds())
        ),
    };
    let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;

    let tx = match rpc.get_transaction_json_parsed(sig).await {
        Ok(v) => v,
        Err(e) => {
            warn!(swap_sig = %sig, error = %e, "confirmed BUY but getTransaction unavailable");
            return Ok(());
        }
    };

    // Compute actual SOL spent from tx meta (best-effort).
    let sol_delta = sol_delta_from_tx_meta(user_public_key, &tx).unwrap_or(0);
    let spent = (-sol_delta).max(0) as f64 / 1e9;
    let spent_lamports = (-sol_delta).max(0) as u64;
    let _ = redis
        .add_spent_mint_lamports(output_mint, spent_lamports)
        .await;

    if let Some(mut pos) = position_from_tx_meta(
        user_public_key,
        output_mint,
        spent.max(0.0),
        &sig.to_string(),
        &tx,
    ) {
        // Record strategy for exit engine decisions and OpenClaw context.
        pos.strategy_id = Some(if buy_strategy_id == "alpha_playbook" {
            buy_strategy_id.to_string()
        } else {
            sell_strategy_id.to_string()
        });
        pos.buy_strategy_id = Some(buy_strategy_id.to_string());
        pos.sell_strategy_id = Some(sell_strategy_id.to_string());
        if buy_strategy_id == "buy_dip" {
            pos = aggregate_buy_dip_position(redis, pos).await?;
        }
        if buy_strategy_id == "alpha_playbook" {
            pos = aggregate_alpha_playbook_position(redis, pos).await?;
        }
        let _ = redis.upsert_position(&pos).await;
        let plan = if buy_strategy_id == "alpha_playbook" {
            let token_state = redis.get_alpha_playbook_token_state(output_mint).await?;
            token_state
                .and_then(|state| state.scenario.map(|scenario| alpha_playbook_position_plan(scenario, &pos.id)))
                .unwrap_or_else(|| {
                    exit_plan_for_strategy(
                        &pos.id,
                        sell_strategy_id,
                        sell_strategy_templates,
                        default_exit_plan_template,
                    )
                })
        } else {
            exit_plan_for_strategy(
                &pos.id,
                sell_strategy_id,
                sell_strategy_templates,
                default_exit_plan_template,
            )
        };
        let _ = redis.set_position_exit_plan(&plan).await;
        let _ = redis.recent_mints_record(output_mint, 10).await;

        // Birdeye market cap (best-effort).
        let token_info = token_info_cached(redis, birdeye, output_mint, 300).await;
        let mcap_line = match token_info.as_ref().and_then(|t| t.market_cap_usd) {
            Some(mc) => format!(" at ${} mcap", format_compact(mc)),
            None => "".into(),
        };

        let market_cap_display = token_info
            .as_ref()
            .and_then(|t| t.market_cap_usd)
            .map(format_compact)
            .unwrap_or_else(|| "N/A".into());
        let alert = state::types::AlertEvent {
            ts: chrono::Utc::now(),
            kind: "bot_fill_buy".into(),
            message: format!(
                "CLAWDIO BOT BUY🟢\nToken: {}\nMarket Cap: ${}\nSOL spent: {:.4} SOL",
                output_mint, market_cap_display, spent
            ),
        };
        let _ = redis.enqueue_alert(&serde_json::to_string(&alert)?).await;

        if buy_strategy_id == "alpha_playbook" {
            mark_alpha_playbook_fill(redis, output_mint, &pos.wallet).await?;
        }

        if buy_strategy_id == "buy_dip" {
            update_buy_dip_setup_after_fill(redis, original_intent_signature, &pos).await?;
        }

        if buy_strategy_id == "copytrade_fast" {
            let fill_delay_ms = observed_at
                .map(|observed| (confirmed_at - observed).num_milliseconds())
                .unwrap_or(i64::MAX);
            let alpha_token_amount = alpha_token_amount_base_units
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| *value > 0.0);
            let our_token_amount = pos
                .token_amount
                .parse::<f64>()
                .ok()
                .filter(|value| *value > 0.0);
            let alpha_price_per_base = alpha_token_amount.map(|amount| alpha_notional_sol / amount);
            let our_price_per_base = our_token_amount.map(|amount| spent / amount);
            let price_above_alpha_pct = match (alpha_price_per_base, our_price_per_base) {
                (Some(alpha_price), Some(our_price)) if alpha_price > 0.0 => {
                    ((our_price / alpha_price) - 1.0) * 100.0
                }
                _ => f64::INFINITY,
            };
            let valid_timing = fill_delay_ms <= max_fill_delay_ms;
            let valid_price = price_above_alpha_pct <= max_price_above_alpha_pct;
            if !valid_timing || !valid_price {
                let invalid_alert = state::types::AlertEvent {
                    ts: chrono::Utc::now(),
                    kind: "bot_copytrade_fast_invalid_buy".into(),
                    message: format!(
                        "CLAWDIO BOT BUY INVALID🟠\nToken: {}\nFill delay: {} ms\nPrice above alpha: {:.2}%\nAction: immediate full sell",
                        output_mint, fill_delay_ms, price_above_alpha_pct
                    ),
                };
                let _ = redis
                    .enqueue_alert(&serde_json::to_string(&invalid_alert)?)
                    .await;

                let intent = TradeIntent {
                    signature: format!(
                        "exit:{}:fast_invalid:{}",
                        pos.id,
                        Utc::now().timestamp_nanos_opt().unwrap_or_default()
                    ),
                    slot: 0,
                    wallet: pos.wallet.clone(),
                    side: TradeSide::Sell,
                    mint: pos.mint.clone(),
                    notional_sol: spent,
                    venue: Some("copytrade_fast_invalid".into()),
                    observed_at: Some(Utc::now()),
                    classified_at: Some(Utc::now()),
                    amount_in_base_units: Some(pos.token_amount.clone()),
                    token_delta_base_units: None,
                    requested_buy_sol: None,
                    source_wallet_exit_full: false,
                    source_wallet_sold_pct: None,
                    created_at: Utc::now(),
                };
                let order = serde_json::json!({
                    "intent": intent,
                    "filter_reason": "copytrade_fast_invalid_buy",
                });
                redis.enqueue_exec_order(&order.to_string()).await?;
            }
        }

        if let Some(url) = openclaw_event_url {
            let token_info =
                enrich_token_info_for_openclaw(token_info, helius_api_key, helius_api_base_url)
                    .await;
            if let Err(e) = send_openclaw_position_event(
                url,
                openclaw_api_key,
                &pos,
                &plan,
                token_info.as_ref(),
            )
            .await
            {
                warn!(pos_id = %pos.id, mint = %pos.mint, error = %e, "OpenClaw position event failed");
            }
        }
    }

    Ok(())
}

fn exit_plan_for_strategy(
    position_id: &str,
    strategy_id: &str,
    strategy_exit_templates: &HashMap<String, state::types::ExitPlanTemplate>,
    default_exit_plan_template: &state::types::ExitPlanTemplate,
) -> state::types::PositionExitPlan {
    if let Some(template) = strategy_exit_templates.get(strategy_id) {
        return template.to_position_plan(position_id, &format!("strategy:{strategy_id}"));
    }

    default_exit_plan_template.to_position_plan(position_id, "default_config")
}

async fn send_openclaw_position_event(
    url: &str,
    api_key: Option<&str>,
    pos: &state::types::Position,
    baseline_exit_plan: &state::types::PositionExitPlan,
    token_info: Option<&common::token_info::TokenInfo>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .context("failed building OpenClaw client")?;

    let body = OpenClawPositionOpenedEvent {
        event_type: "position_opened",
        analysis_id: format!("oc:{}:{}", pos.id, Utc::now().timestamp_millis()),
        chain: "solana",
        position_id: &pos.id,
        wallet: &pos.wallet,
        mint: &pos.mint,
        buy_signature: &pos.buy_sig,
        strategy_id: pos
            .buy_strategy_id
            .as_deref()
            .or(pos.strategy_id.as_deref())
            .unwrap_or("default"),
        opened_at: pos.opened_at,
        spent_sol: pos.size_sol,
        token_amount_base_units: &pos.token_amount,
        baseline_exit_plan,
        token: OpenClawTokenInfo {
            symbol: token_info.and_then(|info| info.symbol.as_deref()),
            name: token_info.and_then(|info| info.name.as_deref()),
            market_cap_usd: token_info.and_then(|info| info.market_cap_usd),
            description: token_info.and_then(|info| info.description.as_deref()),
            description_source: token_info.and_then(|info| info.description_source.as_deref()),
            description_source_url: token_info
                .and_then(|info| info.description_source_url.as_deref()),
            pumpfun_url: format!("https://pump.fun/coin/{}", pos.mint),
            dexscreener_url: format!("https://dexscreener.com/solana/{}", pos.mint),
        },
    };

    let mut req = client.post(url).json(&body);
    if let Some(key) = api_key.filter(|key| !key.trim().is_empty()) {
        req = req.bearer_auth(key);
    }

    let resp = req.send().await.context("OpenClaw request failed")?;
    let status = resp.status();
    let response_body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("OpenClaw http status {} body={}", status, response_body);
    }

    info!(pos_id = %pos.id, mint = %pos.mint, "sent OpenClaw position_opened event");
    Ok(())
}

async fn send_openclaw_buy_dip_event(
    url: &str,
    api_key: Option<&str>,
    event: &OpenClawBuyDipRequestedEvent<'_>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .context("failed building OpenClaw client")?;

    let mut req = client.post(url).json(event);
    if let Some(key) = api_key.filter(|key| !key.trim().is_empty()) {
        req = req.bearer_auth(key);
    }

    let resp = req.send().await.context("OpenClaw request failed")?;
    let status = resp.status();
    let response_body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("OpenClaw http status {} body={}", status, response_body);
    }

    info!(
        analysis_id = %event.analysis_id,
        mint = %event.mint,
        "sent OpenClaw buy_dip_watch_requested event"
    );
    Ok(())
}

async fn enrich_token_info_for_openclaw(
    token_info: Option<TokenInfo>,
    helius_api_key: Option<&str>,
    helius_api_base_url: &str,
) -> Option<TokenInfo> {
    let Some(mut token_info) = token_info else {
        return None;
    };
    let Some(api_key) = helius_api_key.filter(|value| !value.trim().is_empty()) else {
        return Some(token_info);
    };
    if token_info.description.is_some() {
        return Some(token_info);
    }
    if let Err(error) =
        enrich_with_helius_metadata(&mut token_info, api_key, helius_api_base_url).await
    {
        warn!(mint = %token_info.mint, error = %error, "failed to enrich token metadata for OpenClaw");
    }
    Some(token_info)
}

async fn reconcile_position_after_sell(
    rpc: &RpcClient,
    redis: &RedisState,
    user_public_key: &str,
    pos_id: &str,
    mint: &str,
    min_sell_token_ui: f64,
) -> Result<()> {
    let (remaining_balance, decimals) = rpc
        .get_token_balance_base_units_and_decimals(user_public_key, mint)
        .await
        .unwrap_or((0, 0));
    let dust_threshold = sell_dust_threshold_base_units(min_sell_token_ui, decimals);
    if remaining_balance == 0 || (dust_threshold > 0 && remaining_balance <= dust_threshold) {
        let _ = redis.close_position(pos_id).await;
        return Ok(());
    }

    let Some(mut pos) = redis.get_position(pos_id).await? else {
        return Ok(());
    };
    let previous_token_amount = pos.token_amount.parse::<u128>().unwrap_or(0);
    let remaining_u128 = remaining_balance as u128;

    let exit_state = redis.get_exit_state(pos_id).await.unwrap_or_default();
    if exit_state.get("stage").map(|v| v.as_str()) == Some("tp1_enqueued") {
        let runner_entry_value_sol = exit_state
            .get("runner_entry_value_sol")
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(pos.size_sol);
        pos.size_sol = runner_entry_value_sol;
        pos.token_amount = remaining_balance.to_string();
        let _ = redis
            .set_exit_state_fields(pos_id, &[("stage", "runner")])
            .await;
    } else if previous_token_amount > 0 && remaining_u128 < previous_token_amount {
        let remaining_ratio = (remaining_u128 as f64) / (previous_token_amount as f64);
        pos.size_sol *= remaining_ratio;
        pos.token_amount = remaining_balance.to_string();
    } else {
        pos.token_amount = remaining_balance.to_string();
    }

    let _ = redis.upsert_position(&pos).await;
    Ok(())
}

fn sol_delta_from_tx_meta(owner: &str, get_tx: &serde_json::Value) -> Option<i64> {
    let result = get_tx.get("result")?;
    let tx = result.get("transaction")?;
    let keys = tx.get("message")?.get("accountKeys")?.as_array()?;
    let meta = result.get("meta")?;
    let pre = meta.get("preBalances")?.as_array()?;
    let post = meta.get("postBalances")?.as_array()?;

    let mut idx: Option<usize> = None;
    for (i, k) in keys.iter().enumerate() {
        let pk = k
            .get("pubkey")
            .and_then(|p| p.as_str())
            .or_else(|| k.as_str());
        if pk == Some(owner) {
            idx = Some(i);
            break;
        }
    }
    let i = idx?;
    let pre_i = pre.get(i)?.as_i64()?;
    let post_i = post.get(i)?.as_i64()?;
    Some(post_i - pre_i)
}

#[allow(dead_code)]
fn dust_threshold_base_units(decimals: u8) -> u64 {
    // Define "dust" as <= 1e-6 tokens (only meaningful when decimals >= 6).
    // Example: decimals=6 => 1 base unit, decimals=9 => 1000 base units.
    let exp = decimals.saturating_sub(6) as u32;
    let mut v: u64 = 1;
    for _ in 0..exp {
        v = v.saturating_mul(10);
    }
    v
}

fn sell_dust_threshold_base_units(min_sell_token_ui: f64, decimals: u8) -> u64 {
    if !min_sell_token_ui.is_finite() || min_sell_token_ui <= 0.0 {
        return 0;
    }
    // If decimals are unknown (0) we can't safely convert sub-1.0 UI thresholds.
    // Only apply dust handling for whole tokens in that case.
    if decimals == 0 {
        if min_sell_token_ui >= 1.0 {
            let v = min_sell_token_ui.ceil();
            if v >= (u64::MAX as f64) {
                return u64::MAX;
            }
            return v as u64;
        }
        return 0;
    }

    // Convert UI amount to integer base units (ceil).
    let factor = 10_f64.powi(decimals as i32);
    let v = (min_sell_token_ui * factor).ceil();
    if !v.is_finite() || v <= 0.0 {
        return 0;
    }
    if v >= (u64::MAX as f64) {
        return u64::MAX;
    }
    v as u64
}

#[cfg(test)]
mod tests {
    use super::{decide_alpha_playbook_action, AlphaPlaybookDecision};
    use chrono::Utc;
    use state::types::AlphaPlaybookScenario;

    fn token_state(scenario: AlphaPlaybookScenario) -> state::types::AlphaPlaybookTokenState {
        let now = Utc::now();
        state::types::AlphaPlaybookTokenState {
            mint: "mint".into(),
            scenario: Some(scenario),
            chosen_playbook: None,
            chosen_size_sol: None,
            first_wallet: Some("wallet-a".into()),
            first_buy_signature: Some("sig".into()),
            first_buy_at: Some(now),
            latest_wallet: Some("wallet-a".into()),
            latest_event_at: Some(now),
            confirmation_wallets: vec!["wallet-a".into(), "wallet-b".into()],
            confirmation_triggered: true,
            entry_executed: false,
            entry_executed_at: None,
            confirmation_entry_executed: false,
            confirmation_entry_executed_at: None,
            cooldown_active: false,
            cooldown_expires_at: None,
            watch_active: false,
            token_age_seconds: Some(300),
            token_age_bucket: Some("lte_10m".into()),
            market_cap_usd: Some(25_000.0),
            peak_market_cap_usd: Some(25_000.0),
            volume_5m_usd: Some(25_000.0),
            volume_30m_usd: Some(30_000.0),
            recent_activity_bucket: Some("hot".into()),
            risk_tier: None,
            updated_at: now,
            created_at: now,
        }
    }

    #[test]
    fn alpha_playbook_new_hot_executes_initial_entry() {
        let state = token_state(AlphaPlaybookScenario::NewHot);
        match decide_alpha_playbook_action(&state, "wallet-a", 0) {
            AlphaPlaybookDecision::Execute { buy_sol, .. } => assert_eq!(buy_sol, 0.8),
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn alpha_playbook_old_dormant_arms_watch_before_confirmation() {
        let state = token_state(AlphaPlaybookScenario::OldDormantSpiked);
        match decide_alpha_playbook_action(&state, "wallet-a", 0) {
            AlphaPlaybookDecision::ArmWatch => {}
            _ => panic!("expected watch arm"),
        }
    }

    #[test]
    fn alpha_playbook_mid_trend_confirmation_executes_add() {
        let mut state = token_state(AlphaPlaybookScenario::MidTrend);
        state.entry_executed = true;
        match decide_alpha_playbook_action(&state, "wallet-b", 0) {
            AlphaPlaybookDecision::Execute { buy_sol, .. } => assert_eq!(buy_sol, 0.5),
            _ => panic!("expected execute"),
        }
    }
}
