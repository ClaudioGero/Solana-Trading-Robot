#!/usr/bin/env bash
set -euo pipefail

echo "Checking prerequisites..."

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing: $1"
    exit 1
  fi
}

need node
need npm

if command -v docker >/dev/null 2>&1; then
  echo "docker: OK"
else
  echo "docker: missing (optional until you run infra)"
fi

if command -v rustc >/dev/null 2>&1 && command -v cargo >/dev/null 2>&1; then
  echo "rust/cargo: OK"
else
  echo "rust/cargo: missing (required for core/)"
fi

echo "OK"

#!/usr/bin/env bash
set -euo pipefail

say() { printf "%s\n" "$*"; }
ok() { say "✅ $*"; }
warn() { say "⚠️  $*"; }
die() { say "❌ $*"; exit 1; }

need_cmd() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    ok "found: $cmd ($(command -v "$cmd"))"
    return 0
  fi
  warn "missing: $cmd"
  return 1
}

say "== solana_bot prereqs check =="
say

missing_required=0
missing_optional=0

say "-- Required (dev) --"
need_cmd node || missing_required=1
need_cmd npm || missing_required=1
need_cmd cargo || missing_required=1

say
say "-- Optional (infra) --"
need_cmd docker || missing_optional=1

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    ok "docker compose works"
  else
    warn "docker compose not available (try updating Docker Desktop)"
    missing_optional=1
  fi
fi

say
say "-- Runtime endpoints (optional) --"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
DATABASE_URL="${DATABASE_URL:-postgres://bot:bot@127.0.0.1:5432/bot}"
say "REDIS_URL=${REDIS_URL}"
say "DATABASE_URL=${DATABASE_URL}"

if command -v docker >/dev/null 2>&1; then
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^infra-redis-1$'; then
    ok "redis container appears to be running (infra-redis-1)"
  else
    warn "redis container not detected (run: docker compose -f infra/docker-compose.yml up -d redis)"
  fi
fi

say
if [[ "$missing_required" -ne 0 ]]; then
  die "missing required tools. Install Node.js (20+) and Rust (stable) before continuing."
fi

if [[ "$missing_optional" -ne 0 ]]; then
  warn "optional tools missing. You can still work on Block 1 without Docker/Redis/Postgres."
fi

ok "prereqs look good for Block 1"

