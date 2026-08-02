#!/bin/bash
# rvcache OMM endpoint smoke tests (SPEC Milestone 4).
# A: provider side -- rvcache serves an OMM client (omm_client) from a
#    sass2 _TIC feed (solicited REFRESH from cache + UPDATE fan-out).
# B: feed side -- rvcache consumes omm_server's built-in test source
#    (RSF, service 100) and serves an rv_client the converted initial.
# Requires the RDM dictionary at $DICT (fname<->fid for conversions).
set -u

HERE="$(cd "$(dirname "$0")/.." && pwd)"
BLD="$HERE/FC43_x86_64/bin"
SASS="$HERE/../sassrv/FC43_x86_64/bin"
OMM="$HERE/../omm/FC43_x86_64/bin"
DICT="${DICT:-$HOME/rai/RaiCore/rmds-config}"
TMP="$(mktemp -d)"
RVC="$BLD/rv_cache"
PUB="$SASS/rv_pub"
CLI="$SASS/rv_client"
SRV="$SASS/rv_server"
OSRV="$OMM/omm_server"
OCLI="$OMM/omm_client"

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

for b in "$RVC" "$PUB" "$CLI" "$SRV" "$OSRV" "$OCLI"; do
  [ -x "$b" ] || { echo "missing binary: $b"; exit 2; }
done
[ -d "$DICT" ] || { echo "missing dictionary: $DICT"; exit 2; }

# =========================================================================
echo "### A: provider side (sass2 feed -> rvcache -> omm_client)"
PORTA=7811
"$SRV" -r $PORTA >"$TMP/srvA.log" 2>&1 &
pids+=($!)
sleep 1
# net 1 = sass2 feed, net 2 = sass2 sub, net 3 = omm provider on 14002,
# service named TEST so subject TEST.0 resolves against the directory
"$RVC" -d tcp:$PORTA -s $PORTA -n '' -p "$DICT" \
  -1 feed sass2 -2 sub sass2 -3 sub omm 14002 '' TEST \
  >"$TMP/rvcA.log" 2>&1 &
pids+=($!)
sleep 1
# INITIAL (seqno 0) + UPDATE (seqno 1) for TEST.REC.0 on the _TIC feed.
# OMM subjects are FEED.SECTOR.RIC shaped: the client resolves the
# service by the TEST.REC sector route, msg_key name carries the RIC
"$PUB" -d tcp:$PORTA -s $PORTA -n '' -r -x -p 2 -k 1 '_TIC.TEST.REC.%d' \
  >/dev/null 2>&1
sleep 2
timeout 8 "$OCLI" -d localhost -c "$DICT" -e TEST.REC.0 >"$TMP/ocliA.log" 2>&1
sleep 1
if grep -qiE "REFRESH|MSG_TYPE" "$TMP/ocliA.log" &&
   grep -qE "BID|ASK|SEQ_NO" "$TMP/ocliA.log"; then
  ok "A: omm_client got the solicited refresh from cache"
else
  no "A: omm_client refresh"
fi
if grep -qE "(SEQ_NO|seq_num).*: +1\b" "$TMP/ocliA.log"; then
  ok "A: refresh carries the merged UPDATE's seqno (cache hit, not raw)"
else
  no "A: refresh seqno merge"
fi
echo "--- omm_client A (head) ---"; sed -n '1,25p' "$TMP/ocliA.log"

# =========================================================================
echo; echo "### B: feed side (omm_server test source -> rvcache -> rv_client)"
PORTB=7812     # rvcache <-> rv_client leg
PORTB2=7813    # omm_server's own rv leg (isolated: no shortcut path)
OPORTB=14005
"$SRV" -r $PORTB  >"$TMP/srvB1.log" 2>&1 &
pids+=($!)
"$SRV" -r $PORTB2 >"$TMP/srvB2.log" 2>&1 &
pids+=($!)
sleep 1
"$OSRV" -o $OPORTB -c "$DICT" -t -d tcp:$PORTB2 -s $PORTB2 -n '' \
  >"$TMP/osrvB.log" 2>&1 &
pids+=($!)
sleep 1
"$RVC" -d tcp:$PORTB -s $PORTB -n '' -p "$DICT" \
  -1 feed omm 127.0.0.1:$OPORTB '' RSF \
  -2 sub sass2 \
  >"$TMP/rvcB.log" 2>&1 &
pids+=($!)
sleep 2
# interest on RSF.REC.SMOKE -> rvcache subscribes upstream -> test source
# refresh -> RWF->sass conversion -> cache -> initial broadcast/serve
timeout 10 "$CLI" -d tcp:$PORTB -s $PORTB -n '' -x RSF.REC.SMOKE \
  >"$TMP/cliB.log" 2>&1 &
CPID=$!
pids+=($CPID)
sleep 6
kill "$CPID" 2>/dev/null
if grep -qiE "BID|ASK|DSPLY" "$TMP/cliB.log"; then
  ok "B: rv_client got the converted image through the omm feed"
else
  no "B: rv_client image via omm feed"
fi
if grep -qE "MSG_TYPE" "$TMP/cliB.log"; then
  ok "B: sass header present (envelope class -> MSG_TYPE mapping)"
else
  no "B: sass header mapping"
fi
echo "--- rv_client B (head) ---"; sed -n '1,25p' "$TMP/cliB.log"
echo "--- rvcache B (tail) ---"; tail -5 "$TMP/rvcB.log"

echo
echo "==================================="
echo "RESULT: pass=$pass fail=$fail"
echo "==================================="
[ "$fail" -eq 0 ]
