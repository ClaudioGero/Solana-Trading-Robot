#!/usr/bin/env python3
import json
import os
import sys
from datetime import datetime, timezone
import socket
from urllib.parse import urlparse, unquote


def _resp_bulk(s: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(s), s)


def _resp_array(parts: list[bytes]) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    out.extend(_resp_bulk(p) for p in parts)
    return b"".join(out)


def _redis_lpush_via_socket(redis_url: str, key: str, value: str) -> None:
    """
    Minimal Redis client for LPUSH via raw RESP over TCP.
    Supports redis://[:password@]host:port/db
    """
    u = urlparse(redis_url)
    if u.scheme not in ("redis", ""):
        raise RuntimeError(f"unsupported REDIS_URL scheme: {u.scheme}")
    host = u.hostname or "127.0.0.1"
    port = u.port or 6379
    password = unquote(u.password) if u.password else None

    with socket.create_connection((host, port), timeout=3.0) as s:
        # AUTH if password present
        if password:
            s.sendall(_resp_array([b"AUTH", password.encode("utf-8")]))
            _ = s.recv(4096)

        cmd = _resp_array([b"LPUSH", key.encode("utf-8"), value.encode("utf-8")])
        s.sendall(cmd)
        resp = s.recv(4096)
        if not resp.startswith(b":"):
            raise RuntimeError(f"unexpected redis reply: {resp!r}")


def main() -> int:
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    msg = " ".join(sys.argv[1:]).strip() or "TEST: bot alert pipeline is working"

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": "test",
        "message": msg,
    }

    _redis_lpush_via_socket(url, "sb:q:alerts", json.dumps(payload))
    print("enqueued test alert into sb:q:alerts (no redis-cli / no pip deps)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

