use anyhow::{Context, Result};
use chrono::Utc;
use state::types::{TradeIntent, TradeSide, WalletEvent};
use std::collections::HashMap;

// Known program ids (best-effort tagging only).
// Pump.fun bonding curve program id (commonly used on mainnet).
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
// Wrapped SOL (WSOL) mint. We treat this as "cash leg" and try to avoid classifying it as the traded mint.
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

pub fn classify_from_get_transaction(
    wallet_evt: &WalletEvent,
    get_txn_resp: &serde_json::Value,
) -> Result<Option<TradeIntent>> {
    let result = match get_txn_resp.get("result") {
        Some(r) if !r.is_null() => r,
        _ => return Ok(None),
    };

    // Pull accountKeys (for SOL delta + program tagging).
    let account_keys = result
        .get("transaction")
        .and_then(|t| t.get("message"))
        .and_then(|m| m.get("accountKeys"))
        .and_then(|k| k.as_array())
        .context("missing transaction.message.accountKeys")?;

    let mut wallet_index: Option<usize> = None;
    // Best-effort venue tagging (used by filters). AccountKeys alone isn't always sufficient,
    // so we also scan top-level + inner instruction program ids below.
    let mut saw_pumpfun = false;
    let mut saw_jupiter = false;

    for (i, ak) in account_keys.iter().enumerate() {
        // jsonParsed uses objects: { pubkey, signer, writable, source }
        if let Some(pk) = ak.get("pubkey").and_then(|p| p.as_str()) {
            if pk == wallet_evt.wallet {
                wallet_index = Some(i);
            }
            // Heuristic: consider non-signer/non-writable keys can still be programs; better: check "source": "transaction"
            // For tagging, we scan all pubkeys and match known program ids.
            if pk == PUMPFUN_PROGRAM {
                saw_pumpfun = true;
            } else if pk == JUPITER_V6_PROGRAM {
                saw_jupiter = true;
            }
        } else if let Some(pk) = ak.as_str() {
            if pk == wallet_evt.wallet {
                wallet_index = Some(i);
            }
            if pk == PUMPFUN_PROGRAM {
                saw_pumpfun = true;
            } else if pk == JUPITER_V6_PROGRAM {
                saw_jupiter = true;
            }
        }
    }

    // Additional venue tagging: scan instruction program ids.
    // This catches cases where accountKeys parsing differs but program IDs are still present in instructions.
    if let Some(msg) = result.get("transaction").and_then(|t| t.get("message")) {
        scan_program_ids(msg, &mut saw_pumpfun, &mut saw_jupiter);
    }
    if let Some(meta) = result.get("meta") {
        scan_inner_program_ids(meta, &mut saw_pumpfun, &mut saw_jupiter);
    }

    // Compute SOL delta for the tracked wallet (lamports).
    let meta = result.get("meta").context("missing meta")?;
    let pre_balances = meta.get("preBalances").and_then(|b| b.as_array());
    let post_balances = meta.get("postBalances").and_then(|b| b.as_array());

    let sol_delta_lamports: i64 = if let Some(idx) = wallet_index {
        let pre = pre_balances
            .and_then(|arr| arr.get(idx))
            .and_then(|v| v.as_i64());
        let post = post_balances
            .and_then(|arr| arr.get(idx))
            .and_then(|v| v.as_i64());
        match (pre, post) {
            (Some(pre), Some(post)) => post - pre,
            _ => 0, // not fatal; token delta can still classify side
        }
    } else {
        // Not fatal; token delta can still classify side.
        0
    };

    // Token deltas for this wallet by mint.
    // We compute deltas in base units (string "amount") to avoid float rounding.
    let pre_tokens = meta.get("preTokenBalances").and_then(|v| v.as_array());
    let post_tokens = meta.get("postTokenBalances").and_then(|v| v.as_array());

    let mut deltas_base: HashMap<String, i128> = HashMap::new();

    if let Some(pre_tokens) = pre_tokens {
        for entry in pre_tokens {
            if entry.get("owner").and_then(|o| o.as_str()) != Some(wallet_evt.wallet.as_str()) {
                continue;
            }
            let mint = match entry.get("mint").and_then(|m| m.as_str()) {
                Some(m) => m.to_string(),
                None => continue,
            };
            let amt = base_amount(entry);
            *deltas_base.entry(mint).or_insert(0) -= amt;
        }
    }

    if let Some(post_tokens) = post_tokens {
        for entry in post_tokens {
            if entry.get("owner").and_then(|o| o.as_str()) != Some(wallet_evt.wallet.as_str()) {
                continue;
            }
            let mint = match entry.get("mint").and_then(|m| m.as_str()) {
                Some(m) => m.to_string(),
                None => continue,
            };
            let amt = base_amount(entry);
            *deltas_base.entry(mint).or_insert(0) += amt;
        }
    }

    // Pick the "main traded mint" + infer side.
    //
    // We prefer using SOL delta direction when available:
    // - SOL decreases => wallet spent SOL => BUY (token delta should be positive)
    // - SOL increases => wallet received SOL => SELL (token delta should be negative)
    // This is more reliable than comparing UI floats across tokens with different decimals.
    //
    // Ignore WSOL when possible (it's often the cash leg).
    let mut candidates: Vec<(String, i128)> = deltas_base
        .into_iter()
        .filter(|(m, d)| d.abs() > 0 && m.as_str() != WSOL_MINT)
        .collect();
    if candidates.is_empty() {
        return Ok(None);
    }

    let sol_dir = sol_delta_lamports.signum(); // -1 spent SOL, +1 received SOL, 0 unknown/flat
    let (mint, token_delta_base, side) = if sol_dir < 0 {
        // BUY: pick the largest positive token delta.
        let pick = candidates
            .iter()
            .filter(|(_, d)| *d > 0)
            .max_by_key(|(_, d)| d.abs());
        if let Some((m, d)) = pick {
            (m.clone(), *d, TradeSide::Buy)
        } else {
            // Fallback: token sign.
            let (m, d) = candidates.into_iter().max_by_key(|(_, d)| d.abs()).unwrap();
            let side = if d > 0 {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            };
            (m, d, side)
        }
    } else if sol_dir > 0 {
        // SELL: pick the largest negative token delta.
        let pick = candidates
            .iter()
            .filter(|(_, d)| *d < 0)
            .max_by_key(|(_, d)| d.abs());
        if let Some((m, d)) = pick {
            (m.clone(), *d, TradeSide::Sell)
        } else {
            let (m, d) = candidates.into_iter().max_by_key(|(_, d)| d.abs()).unwrap();
            let side = if d > 0 {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            };
            (m, d, side)
        }
    } else {
        // SOL delta unknown; fall back to largest token delta magnitude.
        let (m, d) = candidates.into_iter().max_by_key(|(_, d)| d.abs()).unwrap();
        let side = if d > 0 {
            TradeSide::Buy
        } else {
            TradeSide::Sell
        };
        (m, d, side)
    };

    // Notional SOL from SOL delta: buy typically decreases SOL (negative lamports delta).
    let notional_sol = (sol_delta_lamports.abs() as f64) / 1_000_000_000.0;

    let venue = if saw_pumpfun {
        Some("pumpfun".to_string())
    } else if saw_jupiter {
        Some("jupiter".to_string())
    } else {
        Some("unknown".to_string())
    };

    let (source_wallet_exit_full, source_wallet_sold_pct) = if side == TradeSide::Sell {
        source_wallet_sell_stats(meta, wallet_evt.wallet.as_str(), mint.as_str())
    } else {
        (false, None)
    };

    Ok(Some(TradeIntent {
        signature: wallet_evt.signature.clone(),
        slot: wallet_evt.slot,
        wallet: wallet_evt.wallet.clone(),
        side,
        mint,
        notional_sol,
        venue,
        observed_at: Some(wallet_evt.observed_at),
        classified_at: Some(Utc::now()),
        // For SELL intents, pass the token amount (base units) when we have it.
        // This helps any downstream logic that needs a concrete sell size.
        amount_in_base_units: if side == TradeSide::Sell {
            Some(token_delta_base.abs().to_string())
        } else {
            None
        },
        token_delta_base_units: Some(token_delta_base.abs().to_string()),
        requested_buy_sol: None,
        source_wallet_exit_full,
        source_wallet_sold_pct,
        created_at: Utc::now(),
    }))
}

fn source_wallet_sell_stats(
    meta: &serde_json::Value,
    wallet: &str,
    mint: &str,
) -> (bool, Option<f64>) {
    let pre_tokens = meta
        .get("preTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let post_tokens = meta
        .get("postTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let pre_amount: i128 = pre_tokens
        .iter()
        .filter(|entry| entry.get("owner").and_then(|o| o.as_str()) == Some(wallet))
        .filter(|entry| entry.get("mint").and_then(|m| m.as_str()) == Some(mint))
        .map(base_amount)
        .sum();
    let post_amount: i128 = post_tokens
        .iter()
        .filter(|entry| entry.get("owner").and_then(|o| o.as_str()) == Some(wallet))
        .filter(|entry| entry.get("mint").and_then(|m| m.as_str()) == Some(mint))
        .map(base_amount)
        .sum();
    let source_wallet_exit_full = post_amount <= 0;
    let sold_pct = if pre_amount > 0 && pre_amount >= post_amount {
        Some(((pre_amount - post_amount) as f64 / pre_amount as f64) * 100.0)
    } else {
        None
    };
    (source_wallet_exit_full, sold_pct)
}

fn base_amount(entry: &serde_json::Value) -> i128 {
    // Prefer meta.{pre,post}TokenBalances[].uiTokenAmount.amount (base-units string).
    if let Some(s) = entry
        .get("uiTokenAmount")
        .and_then(|u| u.get("amount"))
        .and_then(|v| v.as_str())
    {
        if let Ok(v) = s.parse::<i128>() {
            return v;
        }
    }

    // Fallback: some providers may omit `amount` but include uiAmountString + decimals.
    let ui = entry
        .get("uiTokenAmount")
        .and_then(|u| u.get("uiAmountString"))
        .and_then(|v| v.as_str());
    let decimals = entry
        .get("uiTokenAmount")
        .and_then(|u| u.get("decimals"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u8;
    if let Some(ui) = ui {
        return ui_amount_string_to_base(ui, decimals);
    }

    0
}

fn ui_amount_string_to_base(ui: &str, decimals: u8) -> i128 {
    // Parse a decimal string like "12.34" into base units integer with `decimals`.
    // No float math to avoid precision issues.
    let s = ui.trim();
    if s.is_empty() {
        return 0;
    }
    let neg = s.starts_with('-');
    let s = s.strip_prefix('-').unwrap_or(s);
    let mut parts = s.splitn(2, '.');
    let int_part = parts.next().unwrap_or("0");
    let frac_part = parts.next().unwrap_or("");

    let mut digits = String::new();
    digits.push_str(int_part);

    // pad/truncate fraction to `decimals`
    let mut frac = frac_part.to_string();
    if frac.len() > decimals as usize {
        frac.truncate(decimals as usize);
    } else {
        while frac.len() < decimals as usize {
            frac.push('0');
        }
    }
    digits.push_str(&frac);

    // remove leading zeros
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let v = digits.parse::<i128>().unwrap_or(0);
    if neg {
        -v
    } else {
        v
    }
}

fn scan_program_ids(message: &serde_json::Value, saw_pumpfun: &mut bool, saw_jupiter: &mut bool) {
    // message.instructions[].programId (jsonParsed) OR .programIdIndex (raw)
    if let Some(ixs) = message.get("instructions").and_then(|v| v.as_array()) {
        for ix in ixs {
            if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
                if pid == PUMPFUN_PROGRAM {
                    *saw_pumpfun = true;
                } else if pid == JUPITER_V6_PROGRAM {
                    *saw_jupiter = true;
                }
            }
        }
    }
}

fn scan_inner_program_ids(
    meta: &serde_json::Value,
    saw_pumpfun: &mut bool,
    saw_jupiter: &mut bool,
) {
    // meta.innerInstructions[].instructions[].programId
    if let Some(inner) = meta.get("innerInstructions").and_then(|v| v.as_array()) {
        for group in inner {
            if let Some(ixs) = group.get("instructions").and_then(|v| v.as_array()) {
                for ix in ixs {
                    if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
                        if pid == PUMPFUN_PROGRAM {
                            *saw_pumpfun = true;
                        } else if pid == JUPITER_V6_PROGRAM {
                            *saw_jupiter = true;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::source_wallet_sell_stats;
    use serde_json::json;

    #[test]
    fn detects_full_exit_when_post_balance_is_zero() {
        let meta = json!({
            "preTokenBalances": [
                {
                    "owner": "wallet_1",
                    "mint": "mint_1",
                    "uiTokenAmount": {
                        "amount": "456"
                    }
                }
            ],
            "postTokenBalances": []
        });
        let (is_full_exit, sold_pct) = source_wallet_sell_stats(&meta, "wallet_1", "mint_1");
        assert!(is_full_exit);
        assert_eq!(sold_pct, Some(100.0));
    }

    #[test]
    fn detects_partial_exit_when_post_balance_remains() {
        let meta = json!({
            "preTokenBalances": [
                {
                    "owner": "wallet_1",
                    "mint": "mint_1",
                    "uiTokenAmount": {
                        "amount": "200"
                    }
                }
            ],
            "postTokenBalances": [
                {
                    "owner": "wallet_1",
                    "mint": "mint_1",
                    "uiTokenAmount": {
                        "amount": "123"
                    }
                }
            ]
        });
        let (is_full_exit, sold_pct) = source_wallet_sell_stats(&meta, "wallet_1", "mint_1");
        assert!(!is_full_exit);
        assert_eq!(sold_pct, Some(38.5));
    }
}
