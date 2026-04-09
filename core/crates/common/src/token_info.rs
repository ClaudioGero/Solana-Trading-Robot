use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub mint: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    /// Best-effort market cap in USD (from Birdeye).
    #[serde(default)]
    pub market_cap_usd: Option<f64>,
    /// Best-effort token description from metadata providers such as Helius.
    #[serde(default)]
    pub description: Option<String>,
    /// Best-effort URL associated with the token description, if any.
    #[serde(default)]
    pub description_source_url: Option<String>,
    /// Metadata provider for the token description.
    #[serde(default)]
    pub description_source: Option<String>,
    /// Best-effort pair creation time from DexScreener.
    #[serde(default)]
    pub pair_created_at: Option<DateTime<Utc>>,
    /// Best-effort 5-minute volume in USD from DexScreener.
    #[serde(default)]
    pub volume_5m_usd: Option<f64>,
    /// Best-effort 1-hour volume in USD from DexScreener.
    #[serde(default)]
    pub volume_1h_usd: Option<f64>,
    /// Best-effort liquidity in USD from DexScreener.
    #[serde(default)]
    pub liquidity_usd: Option<f64>,
    pub fetched_at: DateTime<Utc>,
    pub source: String,
}

#[derive(Clone)]
pub struct BirdeyeClient {
    base_url: String,
    api_key: String,
    client: reqwest::Client,
}

impl BirdeyeClient {
    pub fn new(api_key: String) -> Result<Self> {
        Self::new_with_base_url(api_key, "https://public-api.birdeye.so".to_string())
    }

    /// Create a Birdeye client with an explicit base URL.
    /// This exists mainly to enable deterministic unit tests with a local mock server.
    pub fn new_with_base_url(api_key: String, base_url: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .build()
            .context("failed building reqwest client")?;
        Ok(Self {
            base_url,
            api_key,
            client,
        })
    }

    pub async fn market_data(&self, mint: &str) -> Result<TokenInfo> {
        let url = format!(
            "{}/defi/v3/token/market-data?address={}",
            self.base_url, mint
        );

        let resp = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key)
            // Birdeye uses chain routing for some endpoints.
            .header("x-chain", "solana")
            .send()
            .await
            .context("birdeye market-data request failed")?;

        let status = resp.status();
        let v: serde_json::Value = resp
            .json()
            .await
            .context("birdeye market-data response not json")?;
        if !status.is_success() {
            anyhow::bail!("birdeye market-data http status {} body={}", status, v);
        }

        // Birdeye response shapes can vary. Be permissive and search deeply.
        let data = v.get("data").unwrap_or(&v);

        // Birdeye response shapes can vary. Be permissive:
        // - Sometimes name/symbol are present
        // - Sometimes only market cap is present
        let name = deep_pick_str(data, &["name", "tokenName"]);
        let symbol = deep_pick_str(data, &["symbol", "tokenSymbol"]);
        let market_cap_usd =
            deep_pick_f64(data, &["market_cap", "marketCap", "market_cap_usd", "mcap"]);

        Ok(TokenInfo {
            mint: mint.to_string(),
            name,
            symbol,
            market_cap_usd,
            description: None,
            description_source_url: None,
            description_source: None,
            pair_created_at: None,
            volume_5m_usd: None,
            volume_1h_usd: None,
            liquidity_usd: None,
            fetched_at: Utc::now(),
            source: "birdeye".into(),
        })
    }
}

pub async fn fetch_token_info_with_fallback(
    birdeye: Option<&BirdeyeClient>,
    mint: &str,
) -> Option<TokenInfo> {
    if let Some(birdeye) = birdeye {
        if let Ok(mut info) = birdeye.market_data(mint).await {
            if info.market_cap_usd.is_some() {
                if let Ok(ds_info) = fetch_dexscreener_token_info(mint).await {
                    if info.name.is_none() {
                        info.name = ds_info.name;
                    }
                    if info.symbol.is_none() {
                        info.symbol = ds_info.symbol;
                    }
                    info.pair_created_at = ds_info.pair_created_at;
                    info.volume_5m_usd = ds_info.volume_5m_usd;
                    info.volume_1h_usd = ds_info.volume_1h_usd;
                    info.liquidity_usd = ds_info.liquidity_usd;
                }
                return Some(info);
            }
        }
    }

    fetch_dexscreener_token_info(mint).await.ok()
}

async fn fetch_dexscreener_token_info(mint: &str) -> Result<TokenInfo> {
    let base_url = std::env::var("DEXSCREENER_BASE_URL")
        .unwrap_or_else(|_| "https://api.dexscreener.com".into());
    let url = format!(
        "{}/latest/dex/tokens/{}",
        base_url.trim_end_matches('/'),
        mint
    );
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .context("failed building DexScreener client")?;

    let resp = client
        .get(&url)
        .header("accept", "application/json")
        .send()
        .await
        .context("DexScreener request failed")?;
    let status = resp.status();
    let payload: serde_json::Value = resp.json().await.context("DexScreener response not json")?;
    if !status.is_success() {
        anyhow::bail!("DexScreener http status {} body={}", status, payload);
    }

    let pairs = payload
        .get("pairs")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let normalized = mint.to_lowercase();
    let pair = pairs
        .into_iter()
        .filter(|pair| {
            pair.get("baseToken")
                .and_then(|base| base.get("address"))
                .and_then(|value| value.as_str())
                .map(|value| value.eq_ignore_ascii_case(&normalized))
                .unwrap_or(false)
        })
        .max_by(|left, right| score_pair(left).total_cmp(&score_pair(right)))
        .context("DexScreener returned no matching pair")?;

    let name = pair
        .get("baseToken")
        .and_then(|base| base.get("name"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let symbol = pair
        .get("baseToken")
        .and_then(|base| base.get("symbol"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let market_cap_usd = pair
        .get("marketCap")
        .and_then(|value| value.as_f64())
        .or_else(|| pair.get("fdv").and_then(|value| value.as_f64()));
    let pair_created_at = pair
        .get("pairCreatedAt")
        .and_then(|value| value.as_i64())
        .and_then(chrono::DateTime::<Utc>::from_timestamp_millis);
    let volume_5m_usd = pair
        .get("volume")
        .and_then(|value| value.get("m5"))
        .and_then(|value| value.as_f64());
    let volume_1h_usd = pair
        .get("volume")
        .and_then(|value| value.get("h1"))
        .and_then(|value| value.as_f64());
    let liquidity_usd = pair
        .get("liquidity")
        .and_then(|value| value.get("usd"))
        .and_then(|value| value.as_f64());

    Ok(TokenInfo {
        mint: mint.to_string(),
        name,
        symbol,
        market_cap_usd,
        description: None,
        description_source_url: None,
        description_source: None,
        pair_created_at,
        volume_5m_usd,
        volume_1h_usd,
        liquidity_usd,
        fetched_at: Utc::now(),
        source: "dexscreener".into(),
    })
}

fn score_pair(pair: &serde_json::Value) -> f64 {
    let liquidity = pair
        .get("liquidity")
        .and_then(|value| value.get("usd"))
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0);
    let volume = pair
        .get("volume")
        .and_then(|value| value.get("h24"))
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0);
    let market_cap = pair
        .get("marketCap")
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0);
    liquidity * 10.0 + volume + (market_cap / 100.0)
}

pub async fn enrich_with_helius_metadata(
    token_info: &mut TokenInfo,
    helius_api_key: &str,
    helius_api_base_url: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .context("failed building helius metadata client")?;

    let response = client
        .post(format!(
            "{}/?api-key={}",
            helius_api_base_url.trim_end_matches('/'),
            urlencoding::encode(helius_api_key)
        ))
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": "token-metadata",
            "method": "getAsset",
            "params": {
                "id": token_info.mint,
            }
        }))
        .send()
        .await
        .context("helius getAsset request failed")?;

    let status = response.status();
    let payload: serde_json::Value = response
        .json()
        .await
        .context("helius getAsset response not json")?;
    if !status.is_success() {
        anyhow::bail!("helius getAsset http status {} body={}", status, payload);
    }
    if let Some(message) = payload
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(|message| message.as_str())
    {
        anyhow::bail!("helius rpc error: {}", message);
    }

    let result = payload.get("result").unwrap_or(&payload);
    token_info.description = deep_pick_str(result, &["description"])
        .map(|description| compact_description(&description));
    token_info.description_source_url = deep_pick_str(
        result,
        &[
            "metadata_uri",
            "json_uri",
            "uri",
            "external_url",
            "externalUrl",
            "website",
        ],
    );
    token_info.description_source = token_info
        .description
        .as_ref()
        .map(|_| "helius".to_string());

    Ok(())
}

fn pick_str(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(s) = v.get(*k).and_then(|x| x.as_str()) {
            if !s.trim().is_empty() {
                return Some(s.trim().to_string());
            }
        }
    }
    None
}

fn pick_f64(v: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(x) = v.get(*k) {
            if let Some(n) = x.as_f64() {
                return Some(n);
            }
            if let Some(s) = x.as_str() {
                if let Ok(n) = s.parse::<f64>() {
                    return Some(n);
                }
            }
        }
    }
    None
}

fn deep_pick_str(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    // Try shallow first.
    if let Some(s) = pick_str(v, keys) {
        return Some(s);
    }
    match v {
        serde_json::Value::Object(map) => {
            for (_k, child) in map {
                if let Some(s) = deep_pick_str(child, keys) {
                    return Some(s);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr {
                if let Some(s) = deep_pick_str(child, keys) {
                    return Some(s);
                }
            }
        }
        _ => {}
    }
    None
}

fn deep_pick_f64(v: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    if let Some(n) = pick_f64(v, keys) {
        return Some(n);
    }
    match v {
        serde_json::Value::Object(map) => {
            for (_k, child) in map {
                if let Some(n) = deep_pick_f64(child, keys) {
                    return Some(n);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr {
                if let Some(n) = deep_pick_f64(child, keys) {
                    return Some(n);
                }
            }
        }
        _ => {}
    }
    None
}

fn compact_description(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn birdeye_market_data_sends_api_key_and_parses_fields() {
        let server = MockServer::start().await;

        let body = serde_json::json!({
            "success": true,
            "data": {
                "name": "Test Token",
                "symbol": "TEST",
                "market_cap": 6300000.0
            }
        });

        Mock::given(method("GET"))
            .and(path("/defi/v3/token/market-data"))
            .and(query_param(
                "address",
                "Hoi9Lo8s2PP7EM9mv9bZjQ3aSB7ijyS238sTqQjbpump",
            ))
            .and(header("X-API-KEY", "test_key"))
            .and(header("x-chain", "solana"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let c = BirdeyeClient::new_with_base_url("test_key".into(), server.uri()).unwrap();
        let ti = c
            .market_data("Hoi9Lo8s2PP7EM9mv9bZjQ3aSB7ijyS238sTqQjbpump")
            .await
            .unwrap();

        assert_eq!(ti.mint, "Hoi9Lo8s2PP7EM9mv9bZjQ3aSB7ijyS238sTqQjbpump");
        assert_eq!(ti.name.as_deref(), Some("Test Token"));
        assert_eq!(ti.symbol.as_deref(), Some("TEST"));
        assert_eq!(ti.market_cap_usd, Some(6_300_000.0));
        assert_eq!(ti.source, "birdeye");
    }

    #[tokio::test]
    async fn birdeye_market_data_parses_string_numbers_too() {
        let server = MockServer::start().await;

        let body = serde_json::json!({
            "data": {
                "tokenName": "Alt Name",
                "tokenSymbol": "ALT",
                "marketCap": "12345.67"
            }
        });

        Mock::given(method("GET"))
            .and(path("/defi/v3/token/market-data"))
            .and(query_param(
                "address",
                "5o3RfTApj8fF1Msw1KLAiNuyCAVFUmr4X2Kt5mopump",
            ))
            .and(header("X-API-KEY", "k"))
            .and(header("x-chain", "solana"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let c = BirdeyeClient::new_with_base_url("k".into(), server.uri()).unwrap();
        let ti = c
            .market_data("5o3RfTApj8fF1Msw1KLAiNuyCAVFUmr4X2Kt5mopump")
            .await
            .unwrap();

        assert_eq!(ti.name.as_deref(), Some("Alt Name"));
        assert_eq!(ti.symbol.as_deref(), Some("ALT"));
        assert_eq!(ti.market_cap_usd, Some(12345.67));
    }

    #[tokio::test]
    async fn birdeye_market_data_errors_on_non_200() {
        let server = MockServer::start().await;

        let body = serde_json::json!({"error":"bad key"});

        Mock::given(method("GET"))
            .and(path("/defi/v3/token/market-data"))
            .and(query_param(
                "address",
                "9FTivm4idjHRN2qekCsGAJsd1hZhiED5zXLnvGXxpump",
            ))
            .and(header("X-API-KEY", "bad"))
            .and(header("x-chain", "solana"))
            .respond_with(ResponseTemplate::new(401).set_body_json(body))
            .mount(&server)
            .await;

        let c = BirdeyeClient::new_with_base_url("bad".into(), server.uri()).unwrap();
        let err = c
            .market_data("9FTivm4idjHRN2qekCsGAJsd1hZhiED5zXLnvGXxpump")
            .await
            .unwrap_err();

        let s = format!("{err:#}");
        assert!(s.contains("http status 401"), "unexpected error: {s}");
    }

    /// Live smoke test against real Birdeye. This is ignored by default to avoid flakiness and
    /// requiring secrets in CI. Run manually:
    ///   BIRDEYE_API_KEY=... cargo test -p common -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn birdeye_live_smoke_test_real_api() {
        let key = std::env::var("BIRDEYE_API_KEY").unwrap_or_default();
        if key.trim().is_empty() {
            eprintln!("Skipping live Birdeye test: BIRDEYE_API_KEY not set");
            return;
        }

        let c = BirdeyeClient::new(key).unwrap();

        // Use a stable mint to avoid “token not indexed” flakiness.
        let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        // If the endpoint is rate-limited (429) or temporarily failing, don't fail the whole suite.
        // This test is only meant as a manual “does my key work?” check.
        let ti = match c.market_data(usdc).await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Birdeye USDC market_data error: {e}");
                return;
            }
        };
        eprintln!("Birdeye USDC market_data: mcap={:?}", ti.market_cap_usd);
        assert!(ti.market_cap_usd.unwrap_or(0.0) > 0.0);

        // Also try user-provided mints (best-effort; don't hard-fail if Birdeye lacks data).
        for mint in [
            "Hoi9Lo8s2PP7EM9mv9bZjQ3aSB7ijyS238sTqQjbpump",
            "5o3RfTApj8fF1Msw1KLAiNuyCAVFUmr4X2Kt5mopump",
            "9FTivm4idjHRN2qekCsGAJsd1hZhiED5zXLnvGXxpump",
        ] {
            match c.market_data(mint).await {
                Ok(v) => eprintln!("Birdeye {} market_data: mcap={:?}", mint, v.market_cap_usd),
                Err(e) => eprintln!("Birdeye {} market_data error: {}", mint, e),
            }
        }
    }
}
