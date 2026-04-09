use common::token_info::{fetch_token_info_with_fallback, BirdeyeClient, TokenInfo};

use crate::RedisState;

/// Fetch token info from Redis cache, otherwise from the provider fallback chain, then cache it.
///
/// IMPORTANT: This is intentionally the same logic as the previous duplicated implementations
/// in `classify` and `executor` to avoid behavior changes.
pub async fn token_info_cached(
    redis: &RedisState,
    birdeye: Option<&BirdeyeClient>,
    mint: &str,
    ttl_seconds: usize,
) -> Option<TokenInfo> {
    if let Ok(Some(raw)) = redis.get_token_info(mint).await {
        if let Ok(v) = serde_json::from_str::<TokenInfo>(&raw) {
            if (v.market_cap_usd.is_some() || birdeye.is_none()) && v.pair_created_at.is_some() {
                return Some(v);
            }
            if v.market_cap_usd.is_some() && birdeye.is_none() {
                return Some(v);
            }
            // Fresh pumps can briefly return metadata without market cap. If Birdeye is
            // available, fall through and refetch instead of pinning an "unknown" cache entry.
        }
    }
    let ti = fetch_token_info_with_fallback(birdeye, mint).await?;
    if let Ok(payload) = serde_json::to_string(&ti) {
        let _ = redis.set_token_info(mint, &payload, ttl_seconds).await;
    }
    Some(ti)
}
