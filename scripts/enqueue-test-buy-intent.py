#!/usr/bin/env python3
"""
Enqueue a synthetic BUY TradeIntent into Redis (sb:q:trade_intents) so the bot
executes a real buy through the normal filters -> executor path.

No external deps. No redis-cli required.
"""

import json
import os
import socket
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, unquote
from urllib.request import urlopen, Request
from urllib.error import HTTPError


def _load_dotenv_if_present(dotenv_path: str) -> None:
    """
    Minimal .env loader (KEY=VALUE lines). Does not override existing os.environ.
    Keeps this script dependency-free.
    """
    try:
        with open(dotenv_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        return


def _default_wallet_from_alpha_wallets(repo_root: str) -> Optional[str]:
    """
    Best-effort: pick the first enabled wallet address from config/alpha_wallets.json
    so our manual test events look like real alpha-wallet activity.
    """
    try:
        path = os.environ.get("ALPHA_WALLETS_PATH") or os.path.join(repo_root, "config", "alpha_wallets.json")
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
        wallets = j.get("wallets") or []
        for w in wallets:
            if w.get("enabled") and w.get("address"):
                return str(w["address"]).strip()
    except Exception:
        return None
    return None


def _jupiter_preflight_quote(mint: str) -> tuple[bool, str]:
    """
    Try several auth header schemes against Jupiter and return (ok, details).
    """
    jup_api_key = os.environ.get("JUPITER_API_KEY", "").strip()
    amount_in = 150_000_000  # 0.15 SOL in lamports
    qurl = (
        "https://api.jup.ag/swap/v1/quote"
        "?inputMint=So11111111111111111111111111111111111111112"
        f"&outputMint={mint}&amount={amount_in}&slippageBps=2000"
    )

    attempts = []
    # Some providers accept only one of these; try them all to learn the requirement.
    attempts.append(("no_auth", {}))
    if jup_api_key:
        attempts.append(("x-api-key", {"x-api-key": jup_api_key}))
        attempts.append(("bearer", {"Authorization": f"Bearer {jup_api_key}"}))
        attempts.append(("both", {"x-api-key": jup_api_key, "Authorization": f"Bearer {jup_api_key}"}))

    results: list[str] = []
    for name, extra in attempts:
        headers = {"User-Agent": "solana_bot_test/1.0", **extra}
        req = Request(qurl, headers=headers)
        try:
            with urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            q = json.loads(raw)
            out_amt = int(q.get("outAmount") or 0)
            if out_amt > 0:
                return True, f"OK via={name} outAmount={out_amt}"
            results.append(f"FAIL via={name} outAmount=0 body={raw[:200]}")
        except HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            results.append(f"HTTP {getattr(e,'code',None)} via={name} body={body[:200] if body else '(empty)'}")
        except Exception as e:
            results.append(f"ERR via={name} {e}")

    return False, " | ".join(results) if results else "ERR no attempts"


def _resp_bulk(s: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(s), s)


def _resp_array(parts: list[bytes]) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    out.extend(_resp_bulk(p) for p in parts)
    return b"".join(out)


def _redis_cmd(redis_url: str, parts: list[bytes]) -> bytes:
    u = urlparse(redis_url)
    if u.scheme not in ("redis", ""):
        raise RuntimeError(f"unsupported REDIS_URL scheme: {u.scheme}")
    host = u.hostname or "127.0.0.1"
    port = u.port or 6379
    password = unquote(u.password) if u.password else None

    with socket.create_connection((host, port), timeout=3.0) as s:
        if password:
            s.sendall(_resp_array([b"AUTH", password.encode("utf-8")]))
            _ = s.recv(4096)

        s.sendall(_resp_array(parts))
        return s.recv(4096)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: enqueue-test-buy-intent.py <MINT> [WALLET_PUBKEY] [VENUE]")
        print("example: enqueue-test-buy-intent.py 6ogzHhzdrQr9Pgv6hZ2MNze7UrzBMAFyBBWUYp1Fhitx")
        return 2

    mint = sys.argv[1].strip()
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Auto-load core/.env if present (so you don't need to export vars manually).
    # This matches how core-app loads env via dotenvy.
    _load_dotenv_if_present(os.path.join(repo_root, "core", ".env"))

    if len(sys.argv) >= 3:
        wallet = sys.argv[2].strip()
        wallet_src = "argv"
    else:
        w = _default_wallet_from_alpha_wallets(repo_root)
        wallet = w or "manual_test_wallet"
        wallet_src = "alpha_wallets.json" if w else "fallback(manual_test_wallet)"
    venue = (sys.argv[3].strip() if len(sys.argv) >= 4 else "pumpfun")

    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    now = datetime.now(timezone.utc).isoformat()

    # Preflight: confirm Jupiter can quote WSOL->mint for 0.15 SOL.
    # This catches "not a mint", "no route", or API errors before we enqueue.
    print("JUPITER_API_KEY:", "set" if os.environ.get("JUPITER_API_KEY", "").strip() else "MISSING")
    print("wallet:", wallet, f"(src={wallet_src})")
    ok, details = _jupiter_preflight_quote(mint)
    print("Jupiter preflight:", details)
    if not ok:
        print("Aborting enqueue (no valid Jupiter quote).")
        return 3

    intent = {
        "signature": f"manual_test:{uuid.uuid4()}",
        "slot": 0,
        "wallet": wallet,
        "side": "buy",
        "mint": mint,
        "notional_sol": 0.15,
        "venue": venue,
        "observed_at": now,
        "classified_at": now,
        "amount_in_base_units": None,
        "created_at": now,
    }

    # Optional: emit a Telegram-visible alert so you know the trigger happened.
    alert = {
        "ts": now,
        "kind": "manual_test_buy_intent_enqueued",
        "message": f"MANUAL TEST: enqueued BUY intent\nmint: {mint}\nvenue: {venue}\nwallet: {wallet}",
    }

    r1 = _redis_cmd(redis_url, [b"LPUSH", b"sb:q:alerts", json.dumps(alert).encode("utf-8")])
    r2 = _redis_cmd(redis_url, [b"LPUSH", b"sb:q:trade_intents", json.dumps(intent).encode("utf-8")])

    print("enqueued alert reply:", r1)
    print("enqueued trade_intent reply:", r2)
    print("Done. Watch Telegram for bot_submitted / bot_confirmed / bot_fill_buy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

