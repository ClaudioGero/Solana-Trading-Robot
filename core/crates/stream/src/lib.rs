use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{collections::HashMap, time::Duration};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};
use url::Url;

use state::{types::WalletEvent, RedisState};

#[derive(Clone, Debug)]
pub struct TrackedWallet {
    pub address: String,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct StreamerConfig {
    pub ws_url: Url,
    pub commitment: String, // processed/confirmed/finalized
    pub dedupe_ttl_seconds: usize,
    pub include_failed: bool,
}

/// Start a websocket loop that subscribes to logs for each enabled wallet and enqueues events into Redis.
///
/// Notes:
/// - Uses `logsSubscribe` with `"mentions": [wallet]` filter (fast, widely supported)
/// - On reconnect, resubscribes
pub async fn run_wallet_streamer(
    cfg: StreamerConfig,
    redis: RedisState,
    enabled_wallets: Vec<TrackedWallet>,
) -> Result<()> {
    if enabled_wallets.is_empty() {
        warn!("no enabled wallets; streamer will idle");
    }

    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(10);

    loop {
        match run_once(&cfg, &redis, &enabled_wallets).await {
            Ok(()) => {
                warn!("wallet streamer ended; reconnecting");
            }
            Err(e) => {
                warn!(error = %e, "wallet streamer error; reconnecting");
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn run_once(
    cfg: &StreamerConfig,
    redis: &RedisState,
    enabled_wallets: &[TrackedWallet],
) -> Result<()> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(cfg.ws_url.as_str())
        .await
        .context("ws connect failed")?;

    info!(ws_url = %cfg.ws_url, wallets = enabled_wallets.len(), "ws connected");

    // subscription_id -> wallet mapping
    let mut sub_map: HashMap<u64, TrackedWallet> = HashMap::new();

    for (idx, wallet) in enabled_wallets.iter().enumerate() {
        // logsSubscribe
        let req_id = (idx as u64) + 1;
        let req = json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "logsSubscribe",
            "params": [
                { "mentions": [wallet.address] },
                { "commitment": cfg.commitment }
            ]
        });

        ws.send(Message::Text(req.to_string().into()))
            .await
            .context("failed sending logsSubscribe")?;

        // Read the subscription response (best-effort, but keep ordering to avoid mixing)
        let msg = ws
            .next()
            .await
            .context("ws closed while awaiting subscribe response")?
            .context("ws returned error message")?;

        let text: String = match msg {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => {
                String::from_utf8(b.to_vec()).context("subscribe response not utf8")?
            }
            other => {
                warn!(?other, "unexpected ws message while subscribing; skipping");
                continue;
            }
        };

        let v: serde_json::Value =
            serde_json::from_str(&text).context("failed parsing subscribe response")?;
        let sub_id = v
            .get("result")
            .and_then(|r| r.as_u64())
            .context("subscribe response missing result subscription id")?;

        sub_map.insert(sub_id, wallet.clone());
        info!(wallet = %wallet.address, label = %wallet.label, sub_id, "subscribed");
    }

    // Process notifications until socket closes.
    while let Some(msg) = ws.next().await {
        let msg = msg.context("ws message error")?;
        let text: String = match msg {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                Ok(s) => s,
                Err(_) => continue,
            },
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => break,
            _ => continue,
        };

        let v: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // logsNotification
        if v.get("method").and_then(|m| m.as_str()) != Some("logsNotification") {
            continue;
        }

        let params = match v.get("params") {
            Some(p) => p,
            None => continue,
        };

        let sub_id = match params.get("subscription").and_then(|s| s.as_u64()) {
            Some(s) => s,
            None => continue,
        };

        let wallet = match sub_map.get(&sub_id) {
            Some(w) => w.clone(),
            None => continue,
        };

        let result = match params.get("result") {
            Some(r) => r,
            None => continue,
        };

        let slot = result
            .get("context")
            .and_then(|c| c.get("slot"))
            .and_then(|s| s.as_u64())
            .unwrap_or(0);

        let value = match result.get("value") {
            Some(v) => v,
            None => continue,
        };

        // Skip failed txns by default (can be enabled if desired).
        let err_is_some = value.get("err").map(|e| !e.is_null()).unwrap_or(false);
        if err_is_some && !cfg.include_failed {
            continue;
        }

        let sig = match value.get("signature").and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        // Dedupe in Redis.
        let is_new = redis
            .dedupe_signature(&sig, cfg.dedupe_ttl_seconds)
            .await
            .unwrap_or(false);
        if !is_new {
            continue;
        }

        let evt = WalletEvent {
            signature: sig.clone(),
            slot,
            wallet: wallet.address.clone(),
            wallet_label: Some(wallet.label.clone()),
            observed_at: Utc::now(),
        };

        let payload = serde_json::to_string(&evt).context("failed serializing WalletEvent")?;
        redis.enqueue_wallet_event(&payload).await?;

        info!(wallet = %wallet.address, label = %wallet.label, signature = %sig, slot, "enqueued wallet event");
    }

    Ok(())
}
