use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

#[derive(Clone)]
pub struct RpcClient {
    http_url: String,
    client: Client,
}

impl RpcClient {
    pub fn new(http_url: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed building reqwest client")?;
        Ok(Self { http_url, client })
    }

    pub async fn get_transaction_with_commitment(
        &self,
        signature: &str,
        commitment: &str,
    ) -> Result<serde_json::Value> {
        // We use jsonParsed so token balances include mint/owner/uiTokenAmount.
        // Keep maxSupportedTransactionVersion=0 for broad compatibility.
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": commitment,
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });

        let resp = self
            .client
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .context("rpc post failed")?;

        let status = resp.status();
        let v: serde_json::Value = resp.json().await.context("rpc response not json")?;
        if !status.is_success() {
            anyhow::bail!("rpc http status {} body={}", status, v);
        }
        // Many providers return HTTP 200 with a JSON-RPC "error" object.
        // Treat that as an error so callers can fall back/retry.
        if v.get("error").is_some_and(|e| !e.is_null()) {
            anyhow::bail!("rpc json error body={}", v);
        }
        Ok(v)
    }

    pub async fn get_transaction(&self, signature: &str) -> Result<serde_json::Value> {
        // Default behavior: confirmed (Helius requirement).
        self.get_transaction_with_commitment(signature, "confirmed")
            .await
    }

    pub async fn get_token_supply(&self, mint: &str) -> Result<(u128, u8)> {
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
