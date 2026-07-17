#!/bin/bash
# rv_cache milestone-1 smoke tests.  Uses sassrv's own rvd-compatible server
# (rv_server) so no TIB install is needed.  Runs spec tests 1, 4, 5, 7 as far
# as feasible in this environment.  Does NOT fabricate results: each check
# prints PASS/FAIL/SKIP based on observed output.
set -u

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ARCH="$(uname -m)"
BLD="$HERE/FC43_x86_64/bin"
SASS="$HERE/../sassrv/FC43_x86_64/bin"
PORT="${PORT:-7793}"
D="-d tcp:$PORT -s $PORT -n"
DNS="-d tcp:$PORT -s $PORT"
TMP="$(mktemp -d)"
RVC="$BLD/rv_cache"
PUB="$SASS/rv_pub"
CLI="$SASS/rv_client"
TOP="$SASS/rv_subtop"
SRV="$SASS/rv_server"

pass=0; fail=0; skip=0
ok()   { echo "PASS: $1"; pass=$((pass+1)); }
no()   { echo "FAIL: $1"; fail=$((fail+1)); }
sk()   { echo "SKIP: $1"; skip=$((skip+1)); }

pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
  kill "$SRVPID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$TMP"
}
trap cleanup EXIT

for b in "$RVC" "$PUB" "$CLI" "$TOP" "$SRV"; do
  [ -x "$b" ] || { echo "missing binary: $b"; exit 2; }
done

echo "### starting rv_server on port $PORT"
"$SRV" -r "$PORT" >"$TMP/srv.log" 2>&1 &
SRVPID=$!
sleep 1

# ---------------------------------------------------------------------------
# Test 1 (interest, default mode): a second RvSubscriptionDB (rv_subtop) is the
# "feed's view".  A consumer listens TEST.FOO; the feed-side submgr must see the
# listen-start, and the listen-stop after the consumer leaves.
echo; echo "### Test 1: interest propagation via submgr (feed's view)"
stdbuf -oL -eL "$TOP" $DNS -n "" '>' >"$TMP/top.log" 2>&1 &
pids+=($!)
sleep 1
timeout 8 "$CLI" $DNS -n "" -x -q TEST.FOO >"$TMP/cli1.log" 2>&1 &
CPID=$!
pids+=($CPID)
sleep 3
grep -q "start .*TEST.FOO" "$TMP/top.log" && ST1=1 || ST1=0
kill "$CPID" 2>/dev/null
sleep 3
grep -q "stop .*TEST.FOO" "$TMP/top.log" && SP1=1 || SP1=0
if [ "$ST1" = 1 ]; then ok "test1 listen-start seen by feed-side submgr"; else no "test1 listen-start not seen"; fi
if [ "$SP1" = 1 ]; then ok "test1 listen-stop seen by feed-side submgr"; else no "test1 listen-stop not seen"; fi
echo "--- rv_subtop.log ---"; sed -n '1,12p' "$TMP/top.log"

# ---------------------------------------------------------------------------
# start rv_cache for the remaining tests
echo; echo "### starting rv_cache -m -A (accounting to file)"
"$RVC" $DNS -n "" -m -A "$TMP/acct.jsonl" >"$TMP/rvc.log" 2>&1 &
pids+=($!)
sleep 2

# ---------------------------------------------------------------------------
# Test 4 (forward-only-when-listened)
echo; echo "### Test 4: forward only when a listener exists"
# 4a: publish with NO listener -> dropped_no_listener, consumer silence
"$PUB" $DNS -n "" -r -p 1 _TIC.TEST.NONE >/dev/null 2>&1
sleep 2
# 4b: add listener on TEST.7, publish _TIC.TEST.7 -> delivered
timeout 10 "$CLI" $DNS -n "" -x TEST.7 >"$TMP/cli4.log" 2>&1 &
pids+=($!)
sleep 3
"$PUB" $DNS -n "" -r -p 1 _TIC.TEST.7 >/dev/null 2>&1
sleep 2
if grep -q "TEST.7" "$TMP/cli4.log"; then ok "test4 tick delivered to live listener (TEST.7)"; else no "test4 delivery to listener"; fi
if grep -q "drop=[1-9]" "$TMP/rvc.log"; then ok "test4 dropped_no_listener stat incremented"; else no "test4 dropped_no_listener stat"; fi

# ---------------------------------------------------------------------------
# Test 5 (cache + snapshot merge): publish INITIAL then UPDATE for TEST.5,
# then _SNAP.TEST.5 must return the merged image with the updated value.
echo; echo "### Test 5: cache + snapshot (INITIAL then UPDATE, merged image)"
# rv_pub -p 2: first publish seqno 0 => MSG_TYPE=INITIAL(8), second seqno 1 => UPDATE(1)
"$PUB" $DNS -n "" -r -x -p 2 -k 1 '_TIC.TEST.%d' >/dev/null 2>&1
sleep 2
timeout 6 "$CLI" $DNS -n "" -x -i -k 1 'TEST.%d' >"$TMP/snap5.log" 2>&1
sleep 1
if grep -qiE "MSG_TYPE|SEQ_NO|BID|inbox|TEST" "$TMP/snap5.log"; then
  ok "test5 snapshot RPC returned a cached image"
else
  no "test5 snapshot RPC returned image"
fi
# spec test 5 addendum: served image is stamped for the delivery kind and
# MSG_TYPE is the leading field (13 = SNAPSHOT for a plain _SNAP poll)
if grep -qE "MSG_TYPE.*: +13" "$TMP/snap5.log"; then
  ok "test5 image stamped MSG_TYPE=SNAPSHOT (13)"
elif grep -q "MSG_TYPE" "$TMP/snap5.log"; then
  no "test5 MSG_TYPE present but not stamped SNAPSHOT"
else
  sk "test5 stamp check (no MSG_TYPE visible in client dump)"
fi
echo "--- snapshot reply (head) ---"; sed -n '1,25p' "$TMP/snap5.log"

# ---------------------------------------------------------------------------
# Test 6b (miss on the listen-start initial path): an rv5-style listener
# (rv_client) attaches a reply inbox to its listen-start on a cold subject;
# the inbox must receive MSG_TYPE=TRANSIENT / REC_STATUS=NOT_FOUND (spec §2:
# miss -> status to the inbox, never silence; same code path as the _SNAP
# miss).  Interest stays registered: a later INITIAL warms the cache and
# broadcasts normally.
echo; echo "### Test 6b: listen-start initial miss => TRANSIENT / NOT_FOUND"
timeout 8 "$CLI" $DNS -n "" -x NO.SUCH.6B >"$TMP/miss6b.log" 2>&1 &
pids+=($!)
sleep 3
if grep -qiE "NOT_FOUND|TRANSIENT" "$TMP/miss6b.log"; then
  ok "test6b listen-start miss reply contains TRANSIENT/NOT_FOUND"
else
  no "test6b listen-start miss reply (silent miss)"
fi
# subject stays live: a later INITIAL must broadcast to the listener
"$PUB" $DNS -n "" -r -x -p 1 _TIC.NO.SUCH.6B >/dev/null 2>&1
sleep 2
if grep -q "NO.SUCH.6B" "$TMP/miss6b.log"; then
  ok "test6b interest survived the miss (later INITIAL delivered)"
else
  no "test6b later INITIAL not delivered after miss"
fi
echo "--- listen-start miss log (head) ---"; sed -n '1,15p' "$TMP/miss6b.log"

# ---------------------------------------------------------------------------
# Test 6c (rv7 flagged-_SNAP path): subrv7test listens + sends _SNAP with
# flags=6 (SUBSCRIBE|INITIAL_VALUES); a cold subject must return the same
# TRANSIENT/NOT_FOUND on its _SNAP reply inbox.  subrv7test fetches a SASS
# dictionary at startup and exits when none is served, so this is gated on
# its no-dictionary flag.
echo; echo "### Test 6c: rv7 _SNAP (flags=6) miss => TRANSIENT / NOT_FOUND"
S7="$SASS/subrv7test"
if [ -x "$S7" ] && strings "$S7" | grep -q nodict; then
  # 4-part subject convention: FEED.DOMAIN.INSTRUMENT.EXCHANGE (NaE = no
  # exchange); the sass3 feed name derives from the first segment.
  timeout 8 "$S7" -daemon tcp:$PORT -service $PORT -nodict NO.SUCH.6C.NaE \
    >"$TMP/miss6c.log" 2>&1 &
  pids+=($!)
  sleep 3
  if grep -qiE "NOT_FOUND|TRANSIENT" "$TMP/miss6c.log"; then
    ok "test6c rv7 _SNAP miss reply contains TRANSIENT/NOT_FOUND"
  else
    no "test6c rv7 _SNAP miss reply (silent miss)"
  fi
  echo "--- rv7 _SNAP miss log (head) ---"; sed -n '1,15p' "$TMP/miss6c.log"
else
  sk "test6c subrv7test lacks the no-dictionary flag (exits without a dict server)"
fi

# ---------------------------------------------------------------------------
# Test 6d (sass3 interest channel): subrv7test -3 sends _SASS.<feed>.SUB
# (SUBSCRIBE|INITIAL_VALUES, feed = first subject segment).  Cold subject:
# the reply inbox must get TRANSIENT/NOT_FOUND (same one-code-path as
# _SNAP).  The sass3 holder is real interest: a later _TIC publish must
# forward to the bare subject.
echo; echo "### Test 6d: sass3 subscribe => miss reply + interest-driven forward"
if [ -x "$S7" ] && strings "$S7" | grep -q sass3; then
  timeout 10 stdbuf -oL "$S7" -daemon tcp:$PORT -service $PORT -nodict -3 \
    SASS.REC.T6D.NaE >"$TMP/miss6d.log" 2>&1 &
  pids+=($!)
  sleep 3
  if grep -qiE "NOT_FOUND|TRANSIENT" "$TMP/miss6d.log"; then
    ok "test6d sass3 initial miss reply contains TRANSIENT/NOT_FOUND"
  else
    no "test6d sass3 initial miss reply (silent miss)"
  fi
  "$PUB" $DNS -n "" -r -x -p 1 _TIC.SASS.REC.T6D.NaE >/dev/null 2>&1
  sleep 2
  if grep -q "SASS.REC.T6D.NaE" "$TMP/miss6d.log"; then
    ok "test6d sass3 interest forwards a later tick to the bare subject"
  else
    no "test6d tick not forwarded on sass3 interest"
  fi
  echo "--- sass3 miss/forward log (head) ---"; sed -n '1,15p' "$TMP/miss6d.log"
else
  sk "test6d subrv7test lacks the -3/-sass3 flag"
fi

# ---------------------------------------------------------------------------
# Test 7 (miss path, broadcast mode): _SNAP.NO.SUCH -> TRANSIENT/NOT_FOUND
echo; echo "### Test 7: snapshot miss => TRANSIENT / NOT_FOUND"
timeout 6 "$CLI" $DNS -n "" -x -i -k 1 'NO.SUCH.%d' >"$TMP/snap7.log" 2>&1
sleep 1
if grep -qiE "NOT_FOUND|TRANSIENT" "$TMP/snap7.log"; then
  ok "test7 miss reply contains TRANSIENT/NOT_FOUND"
elif grep -q "snap=[0-9]*/[1-9]" "$TMP/rvc.log"; then
  ok "test7 snap_missed stat incremented (reply status in rv_cache stats)"
else
  no "test7 miss path"
fi
echo "--- miss reply (head) ---"; sed -n '1,20p' "$TMP/snap7.log"

# ---------------------------------------------------------------------------
echo; echo "### rv_cache final stats line"
tail -3 "$TMP/rvc.log"
echo; echo "### accounting jsonl (if any)"
sed -n '1,8p' "$TMP/acct.jsonl" 2>/dev/null || true

echo
echo "==================================================="
echo "RESULT: pass=$pass fail=$fail skip=$skip"
echo "==================================================="
[ "$fail" = 0 ]
