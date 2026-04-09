use anyhow::{Context, Result};
use serde_json::json;
use std::time::Duration;

/// Minimal Jupiter HTTP client shared across crates.
///
/// IMPORTANT: Keep request shapes/headers identical to previous implementations
/// to avoid behavior changes.
#[derive(Clone)]
pub struct JupiterClient {
    base_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
    /// Optional cap for priority fees added by Jupiter when building the transaction.
    /// Unit: lamports (NOT SOL).
    prioritization_fee_max_lamports: Option<u64>,
}

impl JupiterClient {
    pub fn new(base_url: String, api_key: Option<String>, timeout: Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("failed building reqwest client")?;
        Ok(Self {
            base_url,
            api_key,
            client,
            prioritization_fee_max_lamports: None,
        })
    }

    pub fn with_prioritization_fee_max_lamports(mut self, max_lamports: Option<u64>) -> Self {
        self.prioritization_fee_max_lamports = max_lamports.filter(|v| *v > 0);
        self
    }

    /// Quote with `amount` as an integer base-units string.
    pub async fn quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount: &str,
        slippage_bps: u64,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/swap/v1/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}",
            self.base_url, input_mint, output_mint, amount, slippage_bps
        );

        let mut req = self.client.get(&url);
        if let Some(k) = self.api_key.as_deref() {
            // Jupiter's API gateway expects the key in `x-api-key`.
            // Sending an `Authorization: Bearer ...` header can cause 401s depending on gateway config.
            req = req.header("x-api-key", k);
        }
        let resp = req.send().await.context("jupiter quote failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("jupiter quote not json")?;
        if !status.is_success() {
            anyhow::bail!("jupiter quote http status {} body={}", status, v);
        }
        Ok(v)
    }

    pub async fn swap(
        &self,
        quote_response: serde_json::Value,
        user_public_key: &str,
        wrap_and_unwrap_sol: bool,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/swap/v1/swap", self.base_url);
        let mut body = serde_json::Map::from_iter([
            ("quoteResponse".to_string(), quote_response),
            ("userPublicKey".to_string(), json!(user_public_key)),
            ("wrapAndUnwrapSol".to_string(), json!(wrap_and_unwrap_sol)),
            ("dynamicComputeUnitLimit".to_string(), json!(true)),
        ]);
        if let Some(max_lamports) = self.prioritization_fee_max_lamports {
            // Let Jupiter choose a priority fee up to `maxLamports`.
            // Docs: prioritizationFeeLamports.priorityLevelWithMaxLamports
            body.insert(
                "prioritizationFeeLamports".to_string(),
                json!({
                    "priorityLevelWithMaxLamports": {
                        "maxLamports": max_lamports,
                        "priorityLevel": "veryHigh"
                    }
                }),
            );
        }
        let body = serde_json::Value::Object(body);

        let mut req = self.client.post(&url).json(&body);
        if let Some(k) = self.api_key.as_deref() {
            // Jupiter's API gateway expects the key in `x-api-key`.
            // Sending an `Authorization: Bearer ...` header can cause 401s depending on gateway config.
            req = req.header("x-api-key", k);
        }
        let resp = req.send().await.context("jupiter swap failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("jupiter swap not json")?;
        if !status.is_success() {
            anyhow::bail!("jupiter swap http status {} body={}", status, v);
        }
        Ok(v)
    }
}
