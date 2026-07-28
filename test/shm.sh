#!/bin/bash
# rvcache shm image-store smoke test (SPEC Milestone 3, first slice).
# A: INITIAL+UPDATE cached through -m map; _SNAP returns the merged image.
# B: a SECOND rv_cache process attached to the same map serves the image
#    the FIRST process cached (cross-process shm hit; no local entry).
# Map must exist (kv_server creates it): sysv:raikv.shm
set -u

HERE="$(cd "$(dirname "$0")/.." && pwd)"
BLD="$HERE/FC43_x86_64/bin"
SASS="$HERE/../sassrv/FC43_x86_64/bin"
PORT="${PORT:-7797}"
DNS="-d tcp:$PORT -s $PORT"
MAP="${MAP:-sysv:raikv.shm}"
TMP="$(mktemp -d)"
RVC="$BLD/rv_cache"
PUB="$SASS/rv_pub"
CLI="$SASS/rv_client"
SRV="$SASS/rv_server"

pass=0; fail=0
ok()   { echo "PASS: $1"; pass=$((pass+1)); }
no()   { echo "FAIL: $1"; fail=$((fail+1)); }

pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$TMP"
}
trap cleanup EXIT

echo "### rv_server on port $PORT"
"$SRV" -r "$PORT" >"$TMP/srv.log" 2>&1 &
pids+=($!)
sleep 1

echo; echo "### A: rv_cache -m $MAP, INITIAL+UPDATE, snap merged image"
"$RVC" $DNS -n "" -m "$MAP" >"$TMP/rvcA.log" 2>&1 &
RVCA=$!
pids+=($RVCA)
sleep 1
# seqno 0 => INITIAL(8), seqno 1 => UPDATE(1); merge in shm
"$PUB" $DNS -n "" -r -x -p 2 -k 1 '_TIC.SHMTEST.%d' >/dev/null 2>&1
sleep 2
timeout 6 "$CLI" $DNS -n "" -x -i -k 1 'SHMTEST.%d' >"$TMP/snapA.log" 2>&1
if grep -qiE "MSG_TYPE|SEQ_NO|BID|SHMTEST" "$TMP/snapA.log"; then
  ok "A: snapshot served from shm-backed cache"
else
  no "A: snapshot from shm-backed cache"
fi
if grep -qE "MSG_TYPE.*: +13" "$TMP/snapA.log"; then
  ok "A: image stamped MSG_TYPE=SNAPSHOT (13)"
else
  no "A: MSG_TYPE stamp missing/mismatched"
fi
# the UPDATE tick (seqno 1) must be merged over the INITIAL: rv_pub -p 2
# bumps SEQ_NO per publish, so the merged image carries SEQ_NO 1
if grep -qE "SEQ_NO.*: +1\b" "$TMP/snapA.log"; then
  ok "A: merged image carries the UPDATE's SEQ_NO (RMW in shm)"
else
  no "A: merged image SEQ_NO (merge-in-shm failed?)"
fi
echo "--- snapA head ---"; sed -n '1,20p' "$TMP/snapA.log"

echo; echo "### B: kill process A; a fresh rv_cache serves A's image from shm"
kill "$RVCA" 2>/dev/null
sleep 1
"$RVC" $DNS -n "" -m "$MAP" >"$TMP/rvcB.log" 2>&1 &
pids+=($!)
sleep 1
timeout 6 "$CLI" $DNS -n "" -x -i -k 1 'SHMTEST.%d' >"$TMP/snapB.log" 2>&1
if grep -qiE "SEQ_NO|BID|SHMTEST" "$TMP/snapB.log" &&
   ! grep -qiE "NOT_FOUND|TRANSIENT" "$TMP/snapB.log"; then
  ok "B: cross-process shm hit (image cached by A, served by B)"
else
  no "B: cross-process shm hit"
fi
echo "--- snapB head ---"; sed -n '1,20p' "$TMP/snapB.log"

echo
echo "==================================="
echo "RESULT: pass=$pass fail=$fail"
echo "==================================="
[ "$fail" -eq 0 ]
