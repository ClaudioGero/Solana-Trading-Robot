#!/usr/bin/env python3
"""
No-deps Redis debug helper (no redis-cli).

Shows:
- PAUSE_BUYS / EMERGENCY_STOP flags
- queue lengths for wallet_events / trade_intents / exec_orders / alerts
"""

import os
import socket
from urllib.parse import urlparse, unquote


def _resp_bulk(s: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(s), s)


def _resp_array(parts: list[bytes]) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    out.extend(_resp_bulk(p) for p in parts)
    return b"".join(out)


def _redis_roundtrip(redis_url: str, parts: list[bytes]) -> bytes:
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


def _get(redis_url: str, key: str) -> bytes:
    return _redis_roundtrip(redis_url, [b"GET", key.encode()])


def _llen(redis_url: str, key: str) -> bytes:
    return _redis_roundtrip(redis_url, [b"LLEN", key.encode()])


def main() -> int:
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    print("REDIS_URL:", redis_url)

    print("GET sb:ctrl:pause_buys:", _get(redis_url, "sb:ctrl:pause_buys"))
    print("GET sb:ctrl:emergency_stop:", _get(redis_url, "sb:ctrl:emergency_stop"))

    for k in ["sb:q:wallet_events", "sb:q:trade_intents", "sb:q:exec_orders", "sb:q:alerts"]:
        print(f"LLEN {k}:", _llen(redis_url, k))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

