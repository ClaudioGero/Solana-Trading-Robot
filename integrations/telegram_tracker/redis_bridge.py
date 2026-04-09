#!/usr/bin/env python3
"""
redis_bridge.py

Tiny, dependency-free Redis LPUSH helper for the Solana bot.
Used by the Telegram listener to enqueue TradeIntents into:
- sb:q:trade_intents
- sb:q:alerts (optional)
"""

from __future__ import annotations

import json
import socket
from typing import List
from urllib.parse import urlparse, unquote


def _resp_bulk(s: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(s), s)


def _resp_array(parts: List[bytes]) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    out.extend(_resp_bulk(p) for p in parts)
    return b"".join(out)


def _redis_cmd(redis_url: str, parts: List[bytes]) -> bytes:
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


def lpush_json(redis_url: str, key: str, payload: dict) -> bytes:
    return _redis_cmd(redis_url, [b"LPUSH", key.encode("utf-8"), json.dumps(payload).encode("utf-8")])


def enqueue_trade_intent(redis_url: str, intent: dict) -> bytes:
    return lpush_json(redis_url, "sb:q:trade_intents", intent)


def enqueue_alert(redis_url: str, alert: dict) -> bytes:
    return lpush_json(redis_url, "sb:q:alerts", alert)

