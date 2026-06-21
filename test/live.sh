#!/usr/bin/env bash
# Live end-to-end tests: real `claude -p` (sonnet) -> dockerized proxy -> mock
# (deterministic faults) and, optionally, -> the real gateway.
#
# Why a clean HOME: Claude Code's settings.json `env` block OVERRIDES process
# env, so we isolate with `env -i HOME=<temp>` to force ANTHROPIC_BASE_URL at the
# proxy. Credentials for the real smoke come from the host's $ANTHROPIC_AUTH_TOKEN.
#
# Usage:  ./test/live.sh            # mock + (real if token present) + long-gen
#         DO_REAL=0 ./test/live.sh  # skip the real-gateway smoke
#         DO_LONG=0 ./test/live.sh  # skip the >300s long-generation case
set -uo pipefail

DIR=$(cd "$(dirname "$0")/.." && pwd)
PROXY="http://127.0.0.1:8789"
MOCK="http://127.0.0.1:9099"
MODEL="${MODEL:-sonnet}"
HOME_T="$(mktemp -d /tmp/cc-live-home.XXXX)"
REAL_TOKEN="${ANTHROPIC_AUTH_TOKEN:-}"
DO_REAL="${DO_REAL:-1}"; [ -z "$REAL_TOKEN" ] && DO_REAL=0
DO_LONG="${DO_LONG:-1}"
OUT=/tmp/cc_live_out; ERR=/tmp/cc_live_err
fails=0

c_pass(){ printf '  \033[32mPASS\033[0m  %-16s %s\n' "$1" "$2"; }
c_fail(){ printf '  \033[31mFAIL\033[0m  %-16s %s\n' "$1" "$2"; fails=$((fails+1)); }

run_claude(){ # $1 prompt  $2 timeout  $3 base  $4 token
  timeout "$2" env -i PATH="$PATH" HOME="$HOME_T" \
    ANTHROPIC_BASE_URL="$3" ANTHROPIC_AUTH_TOKEN="$4" \
    API_FORCE_IDLE_TIMEOUT=0 API_TIMEOUT_MS=600000 CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000 \
    DISABLE_AUTOUPDATER=1 CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
    claude -p "$1" --model "$MODEL" --dangerously-skip-permissions >"$OUT" 2>"$ERR"
}

compose(){ docker compose -f "$DIR/$1" "${@:2}"; }
wait_ready(){ for _ in $(seq 1 40); do curl -s -m2 -o /dev/null "$MOCK/__stats" 2>/dev/null && \
  curl -s -m2 -o /dev/null -X POST "$PROXY/v1/messages" -d '{}' 2>/dev/null && return 0; sleep 1; done; return 1; }

# --- mock-backed scenarios (deterministic, no real tokens) -------------------
mock_ok(){ # name  fault  min_attempts  timeout
  local nonce="N${RANDOM}${RANDOM}"
  curl -s "$MOCK/__reset" >/dev/null
  run_claude "Reply briefly. $nonce [[FAULT=$2]]" "${4:-120}" "$PROXY" "test-token"; local ec=$?
  local att; att=$(curl -s "$MOCK/__count?q=$nonce")
  if [ $ec -eq 0 ] && grep -q MOCK_OK "$OUT" && [ "${att:-0}" -ge "$3" ]; then
    c_pass "$1" "recovered after $att upstream attempt(s)"
  else
    c_fail "$1" "exit=$ec attempts=${att:-0} out=$(head -c70 "$OUT" | tr -d '\n')"
  fi
}

echo "==> Bringing up test stack (proxy -> mock)"
compose docker-compose.test.yml up -d --build >/tmp/cc_compose.log 2>&1
wait_ready || { echo "stack not ready; see /tmp/cc_compose.log"; exit 2; }

echo "==> Mock scenarios"
mock_ok "normal"            "normal"             1 60
mock_ok "eof-then-ok"       "eof-then-ok:2"      3 90
mock_ok "overload-then-ok"  "overload-then-ok:2" 3 90
mock_ok "malformed-then-ok" "malformed-then-ok:2" 3 90

# permanent: claude must FAIL fast (exactly 1 attempt, no retry storm)
nonce="N${RANDOM}${RANDOM}"; curl -s "$MOCK/__reset" >/dev/null
run_claude "Reply briefly. $nonce [[FAULT=permanent]]" 60 "$PROXY" "test-token"; ec=$?
att=$(curl -s "$MOCK/__count?q=$nonce")
if [ $ec -ne 0 ] && [ "${att:-9}" -le 1 ]; then c_pass "permanent-no-storm" "claude failed after ${att} attempt"
else c_fail "permanent-no-storm" "exit=$ec attempts=${att:-?} (want exit!=0, attempts<=1)"; fi

# long generation > 300s: with PROXY_KEEPALIVE_MS=600000 the proxy stays FULLY
# transactional for the whole turn (no commit), so the client must wait >310s for
# the first byte — exercised via CLAUDE_CODE_CONNECT_TIMEOUT_MS=660000 in run_claude.
if [ "$DO_LONG" = 1 ]; then
  echo "==> Long-generation (>300s, transactional window) — this takes ~5.5 min"
  nonce="N${RANDOM}${RANDOM}"; curl -s "$MOCK/__reset" >/dev/null
  run_claude "Reply briefly. $nonce [[FAULT=slow:310]]" 420 "$PROXY" "test-token"; ec=$?
  if [ $ec -eq 0 ] && grep -q MOCK_OK "$OUT"; then c_pass "long-gen-310s" "completed a >300s stream"
  else c_fail "long-gen-310s" "exit=$ec out=$(head -c70 "$OUT" | tr -d '\n')"; fi
fi

compose docker-compose.test.yml down >/dev/null 2>&1

# --- real-gateway smoke (one real sonnet call) -------------------------------
if [ "$DO_REAL" = 1 ]; then
  echo "==> Real-gateway smoke (proxy -> real gateway, sonnet)"
  PROXY_UPSTREAM_URL="${PROXY_UPSTREAM_URL:?set PROXY_UPSTREAM_URL to your real gateway for the real smoke}" \
    compose docker-compose.yml up -d --build >>/tmp/cc_compose.log 2>&1
  for _ in $(seq 1 40); do curl -s -m2 -o /dev/null -X POST "$PROXY/v1/messages" -d '{}' 2>/dev/null && break; sleep 1; done
  run_claude "Reply with exactly this token and nothing else: LIVEOK" 90 "$PROXY" "$REAL_TOKEN"; ec=$?
  if [ $ec -eq 0 ] && grep -q LIVEOK "$OUT"; then c_pass "real-sonnet" "real model replied through the proxy"
  else c_fail "real-sonnet" "exit=$ec out=$(head -c80 "$OUT" | tr -d '\n') err=$(tail -c120 "$ERR" | tr -d '\n')"; fi
  compose docker-compose.yml down >/dev/null 2>&1
fi

rm -rf "$HOME_T"
echo
if [ $fails -eq 0 ]; then echo "ALL LIVE TESTS PASSED"; else echo "$fails LIVE TEST(S) FAILED"; fi
exit $fails
