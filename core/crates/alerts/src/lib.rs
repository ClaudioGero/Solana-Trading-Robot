use anyhow::{Context, Result};
use state::{types::AlertEvent, RedisState};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: String,
}

#[derive(Clone)]
struct TelegramClient {
    cfg: TelegramConfig,
    client: reqwest::Client,
}

impl TelegramClient {
    fn new(cfg: TelegramConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed building reqwest client")?;
        Ok(Self { cfg, client })
    }

    async fn send_message(&self, text: &str) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.cfg.bot_token
        );
        let body = serde_json::json!({
            "chat_id": self.cfg.chat_id,
            "text": text,
            "disable_web_page_preview": true
        });
        let resp = self.client.post(url).json(&body).send().await?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.unwrap_or_else(|_| serde_json::json!({}));
        if !status.is_success() {
            anyhow::bail!("telegram send failed status={} body={}", status, v);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AlertWorkerConfig {
    pub enabled: bool,
    pub idle_sleep_ms: u64,
    pub telegram: Option<TelegramConfig>,
}

pub async fn run_alert_worker(cfg: AlertWorkerConfig, redis: RedisState) -> Result<()> {
    if !cfg.enabled {
        info!("alerts disabled; idling");
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }

    let tg = cfg.telegram.clone().map(TelegramClient::new).transpose()?;
    if tg.is_none() {
        warn!("alerts enabled but TELEGRAM config missing; will drain queue but not send");
    }

    info!("alert worker started");
    //nha
    loop {
        let popped = match redis.pop_alert().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "redis unavailable (alerts); retrying");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let Some(raw) = popped else {
            sleep(Duration::from_millis(cfg.idle_sleep_ms)).await;
            continue;
        };

        let evt: AlertEvent = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let is_debug_kind = evt.kind.starts_with("manual_test") || evt.kind.starts_with("bot_");
        if is_debug_kind {
            info!(kind = %evt.kind, "drained alert event");
        }

        let text = evt.message;
        if redis.is_bot_off().await.unwrap_or(false) {
            if is_debug_kind {
                info!(kind = %evt.kind, "suppressed alert because bot is off");
            }
            continue;
        }

        if let Some(tg) = tg.as_ref() {
            if let Err(e) = tg.send_message(&text).await {
                warn!(error = %e, "telegram send failed");
            } else if is_debug_kind {
                info!(kind = %evt.kind, "telegram sent alert");
            }
        }
    }
}
