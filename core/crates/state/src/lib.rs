pub mod keys;
pub mod token_cache;
pub mod types;

use anyhow::{Context, Result};
use redis::AsyncCommands;

use crate::keys::Keys;

#[derive(Clone)]
pub struct RedisState {
    client: redis::Client,
}

impl RedisState {
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url).context("failed to create redis client")?;
        Ok(Self { client })
    }

    pub async fn conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .context("failed to connect to redis")
    }

    /// Returns true if signature was newly observed (not a duplicate).
    /// Uses SET key value NX EX <ttl>.
    pub async fn dedupe_signature(&self, sig: &str, ttl_seconds: usize) -> Result<bool> {
        let mut c = self.conn().await?;
        let key = Keys::dedupe_sig(sig);
        // SET key value NX EX seconds
        let res: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET NX EX failed")?;

        Ok(res.is_some())
    }

    pub async fn enqueue_wallet_event(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_WALLET_EVENTS, payload_json)
            .await
            .context("redis LPUSH wallet_events failed")?;
        Ok(())
    }

    pub async fn enqueue_trade_intent(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_TRADE_INTENTS, payload_json)
            .await
            .context("redis LPUSH trade_intents failed")?;
        Ok(())
    }

    pub async fn enqueue_exec_order(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_EXEC_ORDERS, payload_json)
            .await
            .context("redis LPUSH exec_orders failed")?;
        Ok(())
    }

    /// Blocking pop semantics are implemented in workers later; keep API minimal for now.
    pub async fn pop_wallet_event(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_WALLET_EVENTS, None)
            .await
            .context("redis RPOP wallet_events failed")?;
        Ok(res)
    }

    pub async fn pop_trade_intent(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_TRADE_INTENTS, None)
            .await
            .context("redis RPOP trade_intents failed")?;
        Ok(res)
    }

    pub async fn pop_exec_order(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_EXEC_ORDERS, None)
            .await
            .context("redis RPOP exec_orders failed")?;
        Ok(res)
    }

    pub async fn enqueue_alert(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_ALERTS, payload_json)
            .await
            .context("redis LPUSH alerts failed")?;
        Ok(())
    }

    pub async fn pop_alert(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_ALERTS, None)
            .await
            .context("redis RPOP alerts failed")?;
        Ok(res)
    }

    pub async fn get_token_info(&self, mint: &str) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let key = Keys::token_info(mint);
        let res: Option<String> = c.get(key).await.context("redis GET token_info failed")?;
        Ok(res)
    }

    pub async fn set_token_info(
        &self,
        mint: &str,
        payload_json: &str,
        ttl_seconds: usize,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::token_info(mint);
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(payload_json)
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET EX token_info failed")?;
        Ok(())
    }

    pub async fn get_alpha_playbook_token_state(
        &self,
        mint: &str,
    ) -> Result<Option<types::AlphaPlaybookTokenState>> {
        let mut c = self.conn().await?;
        let raw: Option<String> = c
            .get(Keys::token_state(mint))
            .await
            .context("redis GET alpha_playbook token_state failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let state = serde_json::from_str::<types::AlphaPlaybookTokenState>(&raw)
            .context("failed parsing alpha_playbook token_state json")?;
        Ok(Some(state))
    }

    pub async fn set_alpha_playbook_token_state(
        &self,
        state: &types::AlphaPlaybookTokenState,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let payload = serde_json::to_string(state)
            .context("failed serializing alpha_playbook token_state")?;
        let _: () = c
            .set(Keys::token_state(&state.mint), payload)
            .await
            .context("redis SET alpha_playbook token_state failed")?;
        Ok(())
    }

    pub async fn clear_alpha_playbook_token_state(&self, mint: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: () = c
            .del(Keys::token_state(mint))
            .await
            .context("redis DEL alpha_playbook token_state failed")?;
        Ok(())
    }

    pub async fn get_alpha_playbook_watch_state(
        &self,
        mint: &str,
    ) -> Result<Option<types::AlphaPlaybookWatchState>> {
        let mut c = self.conn().await?;
        let raw: Option<String> = c
            .get(Keys::token_watch(mint))
            .await
            .context("redis GET alpha_playbook watch_state failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let state = serde_json::from_str::<types::AlphaPlaybookWatchState>(&raw)
            .context("failed parsing alpha_playbook watch_state json")?;
        Ok(Some(state))
    }

    pub async fn set_alpha_playbook_watch_state(
        &self,
        state: &types::AlphaPlaybookWatchState,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let payload = serde_json::to_string(state)
            .context("failed serializing alpha_playbook watch_state")?;
        let _: () = c
            .set(Keys::token_watch(&state.mint), payload)
            .await
            .context("redis SET alpha_playbook watch_state failed")?;
        if !state.cancelled && state.expires_at.is_none_or(|expiry| expiry > chrono::Utc::now()) {
            let _: () = c
                .sadd(Keys::ALPHA_PLAYBOOK_WATCH_MINTS_ACTIVE, state.mint.as_str())
                .await
                .context("redis SADD alpha_playbook watch active failed")?;
        } else {
            let _: () = c
                .srem(Keys::ALPHA_PLAYBOOK_WATCH_MINTS_ACTIVE, state.mint.as_str())
                .await
                .context("redis SREM alpha_playbook watch active failed")?;
        }
        Ok(())
    }

    pub async fn clear_alpha_playbook_watch_state(&self, mint: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: () = c
            .del(Keys::token_watch(mint))
            .await
            .context("redis DEL alpha_playbook watch_state failed")?;
        let _: () = c
            .srem(Keys::ALPHA_PLAYBOOK_WATCH_MINTS_ACTIVE, mint)
            .await
            .context("redis SREM alpha_playbook watch active failed")?;
        Ok(())
    }

    pub async fn list_active_alpha_playbook_watch_mints(&self) -> Result<Vec<String>> {
        let mut c = self.conn().await?;
        let ids: Vec<String> = c
            .smembers(Keys::ALPHA_PLAYBOOK_WATCH_MINTS_ACTIVE)
            .await
            .context("redis SMEMBERS alpha_playbook watch active failed")?;
        Ok(ids)
    }

    pub async fn get_alpha_playbook_cooldown_state(
        &self,
        mint: &str,
    ) -> Result<Option<types::AlphaPlaybookCooldownState>> {
        let mut c = self.conn().await?;
        let raw: Option<String> = c
            .get(Keys::token_cooldown(mint))
            .await
            .context("redis GET alpha_playbook cooldown_state failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let state = serde_json::from_str::<types::AlphaPlaybookCooldownState>(&raw)
            .context("failed parsing alpha_playbook cooldown_state json")?;
        Ok(Some(state))
    }

    pub async fn set_alpha_playbook_cooldown_state(
        &self,
        state: &types::AlphaPlaybookCooldownState,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let payload = serde_json::to_string(state)
            .context("failed serializing alpha_playbook cooldown_state")?;
        let _: () = c
            .set(Keys::token_cooldown(&state.mint), payload)
            .await
            .context("redis SET alpha_playbook cooldown_state failed")?;
        Ok(())
    }

    pub async fn clear_alpha_playbook_cooldown_state(&self, mint: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: () = c
            .del(Keys::token_cooldown(mint))
            .await
            .context("redis DEL alpha_playbook cooldown_state failed")?;
        Ok(())
    }

    pub async fn get_alpha_playbook_confirmation_state(
        &self,
        mint: &str,
    ) -> Result<Option<types::AlphaPlaybookConfirmationState>> {
        let mut c = self.conn().await?;
        let raw: Option<String> = c
            .get(Keys::token_confirmation(mint))
            .await
            .context("redis GET alpha_playbook confirmation_state failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let state = serde_json::from_str::<types::AlphaPlaybookConfirmationState>(&raw)
            .context("failed parsing alpha_playbook confirmation_state json")?;
        Ok(Some(state))
    }

    pub async fn set_alpha_playbook_confirmation_state(
        &self,
        state: &types::AlphaPlaybookConfirmationState,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let payload = serde_json::to_string(state)
            .context("failed serializing alpha_playbook confirmation_state")?;
        let _: () = c
            .set(Keys::token_confirmation(&state.mint), payload)
            .await
            .context("redis SET alpha_playbook confirmation_state failed")?;
        Ok(())
    }

    pub async fn clear_alpha_playbook_confirmation_state(&self, mint: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: () = c
            .del(Keys::token_confirmation(mint))
            .await
            .context("redis DEL alpha_playbook confirmation_state failed")?;
        Ok(())
    }

    /// Returns true if this is the first time we've seen this wallet BUY this mint within TTL.
    pub async fn try_mark_alpha_first_buy(
        &self,
        wallet: &str,
        mint: &str,
        ttl_seconds: usize,
    ) -> Result<bool> {
        let mut c = self.conn().await?;
        let key = Keys::alpha_first_buy(wallet, mint);
        let res: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET NX EX alpha_first_buy failed")?;
        Ok(res.is_some())
    }

    /// Returns true if we should emit an alpha_unclassified alert for this wallet right now.
    /// Uses SET NX EX to rate-limit spam.
    pub async fn try_rate_limit_alpha_unclassified(
        &self,
        wallet: &str,
        ttl_seconds: usize,
    ) -> Result<bool> {
        let mut c = self.conn().await?;
        let key = Keys::alpha_unclassified_rl(wallet);
        let res: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET NX EX alpha_unclassified rl failed")?;
        Ok(res.is_some())
    }

    pub async fn enqueue_alpha_sell_signal(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_ALPHA_SELL_SIGNALS, payload_json)
            .await
            .context("redis LPUSH alpha_sell_signals failed")?;
        Ok(())
    }

    pub async fn pop_alpha_sell_signal(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_ALPHA_SELL_SIGNALS, None)
            .await
            .context("redis RPOP alpha_sell_signals failed")?;
        Ok(res)
    }

    pub async fn enqueue_position_exit_plan_update(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_POSITION_EXIT_PLAN_UPDATES, payload_json)
            .await
            .context("redis LPUSH position_exit_plan_updates failed")?;
        Ok(())
    }

    pub async fn pop_position_exit_plan_update(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_POSITION_EXIT_PLAN_UPDATES, None)
            .await
            .context("redis RPOP position_exit_plan_updates failed")?;
        Ok(res)
    }

    pub async fn enqueue_buy_dip_setup_update(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_BUY_DIP_SETUP_UPDATES, payload_json)
            .await
            .context("redis LPUSH buy_dip_setup_updates failed")?;
        Ok(())
    }

    pub async fn pop_buy_dip_setup_update(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_BUY_DIP_SETUP_UPDATES, None)
            .await
            .context("redis RPOP buy_dip_setup_updates failed")?;
        Ok(res)
    }

    pub async fn enqueue_alpha_playbook_add_update(&self, payload_json: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: i64 = c
            .lpush(Keys::Q_ALPHA_PLAYBOOK_ADD_UPDATES, payload_json)
            .await
            .context("redis LPUSH alpha_playbook_add_updates failed")?;
        Ok(())
    }

    pub async fn pop_alpha_playbook_add_update(&self) -> Result<Option<String>> {
        let mut c = self.conn().await?;
        let res: Option<String> = c
            .rpop(Keys::Q_ALPHA_PLAYBOOK_ADD_UPDATES, None)
            .await
            .context("redis RPOP alpha_playbook_add_updates failed")?;
        Ok(res)
    }

    /// Returns true if we acquired the mint lock (i.e. it's not already in-flight).
    pub async fn try_lock_mint(&self, mint: &str, ttl_seconds: usize) -> Result<bool> {
        let mut c = self.conn().await?;
        let key = Keys::inflight_mint(mint);
        let res: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET NX EX mint lock failed")?;
        Ok(res.is_some())
    }

    /// Returns true if we acquired the sell lock for a position (prevents sell spam).
    pub async fn try_lock_sell(&self, pos_id: &str, ttl_seconds: usize) -> Result<bool> {
        let mut c = self.conn().await?;
        let key = Keys::inflight_sell(pos_id);
        let res: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET NX EX sell lock failed")?;
        Ok(res.is_some())
    }

    pub async fn refresh_sell_lock(&self, pos_id: &str, ttl_seconds: usize) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::inflight_sell(pos_id);
        let _: String = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut c)
            .await
            .context("redis SET EX sell lock refresh failed")?;
        Ok(())
    }

    pub async fn recent_mints_contains(&self, mint: &str, n: isize) -> Result<bool> {
        let mut c = self.conn().await?;
        let items: Vec<String> = c
            .lrange(Keys::RECENT_MINTS, 0, n - 1)
            .await
            .context("redis LRANGE recent mints failed")?;
        Ok(items.iter().any(|m| m == mint))
    }

    pub async fn recent_mints_record(&self, mint: &str, n: isize) -> Result<()> {
        let mut c = self.conn().await?;
        // Remove existing occurrences (if any), then push to front, then trim.
        let _: i64 = c
            .lrem(Keys::RECENT_MINTS, 0, mint)
            .await
            .context("redis LREM recent mints failed")?;
        let _: i64 = c
            .lpush(Keys::RECENT_MINTS, mint)
            .await
            .context("redis LPUSH recent mints failed")?;
        let _: () = c
            .ltrim(Keys::RECENT_MINTS, 0, n - 1)
            .await
            .context("redis LTRIM recent mints failed")?;
        Ok(())
    }

    pub async fn upsert_position(&self, pos: &types::Position) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::position(&pos.id);
        let opened_at = pos.opened_at.to_rfc3339();
        let strategy_id = pos.strategy_id.clone().unwrap_or_default();
        let buy_strategy_id = pos.buy_strategy_id.clone().unwrap_or_default();
        let sell_strategy_id = pos.sell_strategy_id.clone().unwrap_or_default();

        let _: () = c
            .hset_multiple(
                &key,
                &[
                    ("id", pos.id.as_str()),
                    ("wallet", pos.wallet.as_str()),
                    ("mint", pos.mint.as_str()),
                    ("opened_at", opened_at.as_str()),
                    ("buy_sig", pos.buy_sig.as_str()),
                    ("size_sol", &pos.size_sol.to_string()),
                    ("strategy_id", strategy_id.as_str()),
                    ("buy_strategy_id", buy_strategy_id.as_str()),
                    ("sell_strategy_id", sell_strategy_id.as_str()),
                    ("token_amount", pos.token_amount.as_str()),
                ],
            )
            .await
            .context("redis HSET pos failed")?;

        let _: () = c
            .sadd(Keys::OPEN_POSITIONS, pos.id.as_str())
            .await
            .context("redis SADD open positions failed")?;

        Ok(())
    }

    pub async fn list_open_position_ids(&self) -> Result<Vec<String>> {
        let mut c = self.conn().await?;
        let ids: Vec<String> = c
            .smembers(Keys::OPEN_POSITIONS)
            .await
            .context("redis SMEMBERS open positions failed")?;
        Ok(ids)
    }

    pub async fn get_position(&self, id: &str) -> Result<Option<types::Position>> {
        let mut c = self.conn().await?;
        let key = Keys::position(id);
        let map: std::collections::HashMap<String, String> =
            c.hgetall(&key).await.context("redis HGETALL pos failed")?;
        if map.is_empty() {
            return Ok(None);
        }
        let opened_at = map
            .get("opened_at")
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        Ok(Some(types::Position {
            id: map.get("id").cloned().unwrap_or_else(|| id.to_string()),
            wallet: map.get("wallet").cloned().unwrap_or_default(),
            mint: map.get("mint").cloned().unwrap_or_default(),
            opened_at,
            buy_sig: map.get("buy_sig").cloned().unwrap_or_default(),
            size_sol: map
                .get("size_sol")
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or_default(),
            strategy_id: map.get("strategy_id").cloned().filter(|s| !s.is_empty()),
            buy_strategy_id: map
                .get("buy_strategy_id")
                .cloned()
                .filter(|s| !s.is_empty()),
            sell_strategy_id: map
                .get("sell_strategy_id")
                .cloned()
                .filter(|s| !s.is_empty()),
            token_amount: map.get("token_amount").cloned().unwrap_or_default(),
        }))
    }

    pub async fn close_position(&self, id: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::position(id);
        let _: () = c.del(&key).await.context("redis DEL pos failed")?;
        // Best-effort cleanup of exit state (if present).
        let _: () = c
            .del(Keys::exit_state(id))
            .await
            .context("redis DEL exit_state failed")?;
        let _: () = c
            .del(Keys::position_exit_plan(id))
            .await
            .context("redis DEL position_exit_plan failed")?;
        let _: () = c
            .srem(Keys::OPEN_POSITIONS, id)
            .await
            .context("redis SREM open positions failed")?;
        Ok(())
    }

    pub async fn get_exit_state(
        &self,
        pos_id: &str,
    ) -> Result<std::collections::HashMap<String, String>> {
        let mut c = self.conn().await?;
        let key = Keys::exit_state(pos_id);
        let map: std::collections::HashMap<String, String> = c
            .hgetall(&key)
            .await
            .context("redis HGETALL exit_state failed")?;
        Ok(map)
    }

    pub async fn set_exit_state_fields(&self, pos_id: &str, fields: &[(&str, &str)]) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::exit_state(pos_id);
        let mut fv: Vec<(String, String)> = Vec::with_capacity(fields.len());
        for (k, v) in fields {
            fv.push((k.to_string(), v.to_string()));
        }
        let _: () = c
            .hset_multiple(&key, &fv)
            .await
            .context("redis HSET exit_state failed")?;
        Ok(())
    }

    pub async fn get_position_exit_plan(
        &self,
        pos_id: &str,
    ) -> Result<Option<types::PositionExitPlan>> {
        let mut c = self.conn().await?;
        let key = Keys::position_exit_plan(pos_id);
        let raw: Option<String> = c
            .get(&key)
            .await
            .context("redis GET position_exit_plan failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let plan = serde_json::from_str::<types::PositionExitPlan>(&raw)
            .context("failed parsing position_exit_plan json")?;
        Ok(Some(plan))
    }

    pub async fn set_position_exit_plan(&self, plan: &types::PositionExitPlan) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::position_exit_plan(&plan.position_id);
        let payload =
            serde_json::to_string(plan).context("failed serializing position_exit_plan")?;
        let _: () = c
            .set(&key, payload)
            .await
            .context("redis SET position_exit_plan failed")?;
        Ok(())
    }

    pub async fn upsert_buy_dip_setup(&self, setup: &types::BuyDipSetup) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::buy_dip_setup(&setup.id);
        let payload = serde_json::to_string(setup).context("failed serializing buy_dip_setup")?;
        let _: () = c
            .set(&key, payload)
            .await
            .context("redis SET buy_dip_setup failed")?;
        if !setup.cancelled && setup.expires_at > chrono::Utc::now() {
            let _: () = c
                .sadd(Keys::BUY_DIP_SETUPS_ACTIVE, setup.id.as_str())
                .await
                .context("redis SADD buy_dip active failed")?;
        } else {
            let _: () = c
                .srem(Keys::BUY_DIP_SETUPS_ACTIVE, setup.id.as_str())
                .await
                .context("redis SREM buy_dip active failed")?;
        }
        Ok(())
    }

    pub async fn get_buy_dip_setup(&self, setup_id: &str) -> Result<Option<types::BuyDipSetup>> {
        let mut c = self.conn().await?;
        let key = Keys::buy_dip_setup(setup_id);
        let raw: Option<String> = c
            .get(&key)
            .await
            .context("redis GET buy_dip_setup failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let setup = serde_json::from_str::<types::BuyDipSetup>(&raw)
            .context("failed parsing buy_dip_setup json")?;
        Ok(Some(setup))
    }

    pub async fn list_active_buy_dip_setup_ids(&self) -> Result<Vec<String>> {
        let mut c = self.conn().await?;
        let ids: Vec<String> = c
            .smembers(Keys::BUY_DIP_SETUPS_ACTIVE)
            .await
            .context("redis SMEMBERS buy_dip active failed")?;
        Ok(ids)
    }

    pub async fn remove_buy_dip_setup_from_active(&self, setup_id: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: () = c
            .srem(Keys::BUY_DIP_SETUPS_ACTIVE, setup_id)
            .await
            .context("redis SREM buy_dip active failed")?;
        Ok(())
    }

    pub async fn upsert_alpha_playbook_add_setup(
        &self,
        setup: &types::AlphaPlaybookAddSetup,
    ) -> Result<()> {
        let mut c = self.conn().await?;
        let key = Keys::alpha_playbook_add_setup(&setup.id);
        let payload =
            serde_json::to_string(setup).context("failed serializing alpha_playbook_add_setup")?;
        let _: () = c
            .set(&key, payload)
            .await
            .context("redis SET alpha_playbook_add_setup failed")?;
        if !setup.cancelled && !setup.filled && setup.armed && setup.expires_at > chrono::Utc::now()
        {
            let _: () = c
                .sadd(Keys::ALPHA_PLAYBOOK_ADD_SETUPS_ACTIVE, setup.id.as_str())
                .await
                .context("redis SADD alpha_playbook_add active failed")?;
        } else {
            let _: () = c
                .srem(Keys::ALPHA_PLAYBOOK_ADD_SETUPS_ACTIVE, setup.id.as_str())
                .await
                .context("redis SREM alpha_playbook_add active failed")?;
        }
        Ok(())
    }

    pub async fn get_alpha_playbook_add_setup(
        &self,
        setup_id: &str,
    ) -> Result<Option<types::AlphaPlaybookAddSetup>> {
        let mut c = self.conn().await?;
        let raw: Option<String> = c
            .get(Keys::alpha_playbook_add_setup(setup_id))
            .await
            .context("redis GET alpha_playbook_add_setup failed")?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let setup = serde_json::from_str::<types::AlphaPlaybookAddSetup>(&raw)
            .context("failed parsing alpha_playbook_add_setup json")?;
        Ok(Some(setup))
    }

    pub async fn list_active_alpha_playbook_add_setup_ids(&self) -> Result<Vec<String>> {
        let mut c = self.conn().await?;
        let ids: Vec<String> = c
            .smembers(Keys::ALPHA_PLAYBOOK_ADD_SETUPS_ACTIVE)
            .await
            .context("redis SMEMBERS alpha_playbook_add active failed")?;
        Ok(ids)
    }

    pub async fn remove_alpha_playbook_add_setup_from_active(&self, setup_id: &str) -> Result<()> {
        let mut c = self.conn().await?;
        let _: () = c
            .srem(Keys::ALPHA_PLAYBOOK_ADD_SETUPS_ACTIVE, setup_id)
            .await
            .context("redis SREM alpha_playbook_add active failed")?;
        Ok(())
    }

    pub async fn get_flag(&self, key: &str) -> Result<bool> {
        let mut c = self.conn().await?;
        let v: Option<String> = c.get(key).await.context("redis GET flag failed")?;
        Ok(matches!(
            v.as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        ))
    }

    pub async fn is_bot_off(&self) -> Result<bool> {
        let pause_buys = self.get_flag(Keys::CTRL_PAUSE_BUYS).await?;
        let emergency_stop = self.get_flag(Keys::CTRL_EMERGENCY_STOP).await?;
        Ok(is_bot_off_from_flags(pause_buys, emergency_stop))
    }

    pub async fn get_spent_mint_lamports(&self, mint: &str) -> Result<u64> {
        let mut c = self.conn().await?;
        let key = Keys::spent_mint_lamports(mint);
        let v: Option<String> = c.get(&key).await.context("redis GET spent mint failed")?;
        Ok(v.and_then(|s| s.parse::<u64>().ok()).unwrap_or(0))
    }

    pub async fn add_spent_mint_lamports(&self, mint: &str, lamports: u64) -> Result<u64> {
        let mut c = self.conn().await?;
        let key = Keys::spent_mint_lamports(mint);
        let new_val: u64 = c
            .incr(&key, lamports)
            .await
            .context("redis INCR spent mint failed")?;
        Ok(new_val)
    }
}

pub fn is_bot_off_from_flags(pause_buys: bool, emergency_stop: bool) -> bool {
    pause_buys && emergency_stop
}

#[cfg(test)]
mod tests {
    use super::is_bot_off_from_flags;

    #[test]
    fn bot_off_requires_both_control_flags() {
        assert!(!is_bot_off_from_flags(false, false));
        assert!(!is_bot_off_from_flags(true, false));
        assert!(!is_bot_off_from_flags(false, true));
        assert!(is_bot_off_from_flags(true, true));
    }
}
