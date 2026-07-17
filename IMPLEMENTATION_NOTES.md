# rv_cache — Implementation Notes (decision log)

This log records every point where SPEC.md underdetermined an implementation
decision for **milestone 1** (default rvd-native mode: nets 1+2). Each entry
states what was ambiguous, what was chosen, why (referencing the cited
sources), and the spec sentence that would have removed the ambiguity. This is
a first-class deliverable measuring the spec's generatability.

Generated from SPEC.md against the reference trees `~/injinj/{sassrv,raimd,
raikv,rvcount}`. Build target: `FC43_x86_64/bin/rv_cache`, clean under
`g++ -Wall -Wextra` (same level as rvcount).

---

## 1. `CacheEntry` cannot embed `md::MDMsgMem` (RouteVec relocatability)
- **Ambiguous:** §3 gives `CacheEntry` with `md::MDMsgMem mem;` and
  `void * image;`, and also says to store it in a raikv `RouteVec<CacheEntry>`
  "same pattern submgr uses for sub_tab".
- **Chosen:** made `CacheEntry` a POD (trailing `hash/len/value[]` like
  submgr's `RvSubscription`) whose `image`/`image_len` is a `malloc`'d raw
  buffer, freed manually on replace/evict/stale. Dropped the embedded
  `MDMsgMem`.
- **Why:** `RouteHT`/`RouteVec` (raikv `route_ht.h`) relocate entries with raw
  `memmove` in `resize()`/`adjust()`/`split_ht()` and never run C++
  constructors/destructors (`inplace()` only sets `hash`/`len` then
  `memcpy`s `value`). A non-trivial `MDMsgMem` member (owns pointers, has a
  dtor) would be corrupted by relocation. submgr's own Data types
  (`RvSubscription`, `RvSessionEntry`) are strictly POD for exactly this
  reason.
- **Spec fix:** state that cache image bytes are heap-owned raw buffers
  (malloc/free) because `RouteVec` Data must be a trivially-relocatable POD;
  `MDMsgMem` in the struct sketch is illustrative, not literal.

## 2. Link libraries: `librvlib.a` does not exist; zlib needed
- **Ambiguous:** "Repo layout" says copy rvcount's GNUmakefile; rvcount's
  makefile links `$(sassrv_home)/.../librvlib.a` + `libsassrv.a`.
- **Chosen:** link `libsassrv.a` only (it now bundles `ev_rv`, `rv_host`,
  `ev_rv_client`, `submgr`, `ft`, `mc` — per sassrv/GNUmakefile
  `libsassrv_files`), plus `libraimd.a`, `libdecnumber.a`, `libraikv.a`, and
  added **`-lz`**.
- **Why:** sassrv no longer builds `librvlib.a`. `-lz` is required because
  `raimd/src/rv_msg.cpp` (`xml_to_string`) references `inflate*`; sassrv's own
  makefile has `dynlink_lib := -lpcre2-8 -lz` while rvcount's older copy did
  not.
- **Spec fix:** "link `-lsassrv -lraimd -ldecnumber -lraikv -lz`" (drop
  `-lrvlib`, add `-lz`).

## 3. Own subscription table structure & presence semantics — SUPERSEDED (2026-07-14 revision: submgr owns interest, see addendum)
- **Ambiguous:** §2 mandates rv_cache's *own* subscription table merging
  advisory + flagged-`_SNAP` + SASS3 interest with keepalive timers, but gives
  no concrete layout, and resolved-question-1 only says "read-only find".
- **Chosen:** `RouteVec<SubEntry>` keyed by subject; each `SubEntry` owns a
  heap `ArrayCount<Holder>`. A `Holder` is keyed by session-identity string and
  carries `host_id`, `ref_mono` (last keepalive), `start_mono`, `proto`,
  `query_flags`, `msgs`, `images`. **Presence** = any holder with
  `ref_mono + hold_secs >= cur_mono`. `host_id` taken directly from
  `RvSessionEntry::host_id`.
- **Why:** matches the submgr `RouteVec` pattern for the subject index while
  keeping variable-length per-subject holder sets off the relocatable Data
  (only a pointer lives in the entry). Session-string keying gives the
  per-(session-identity, subject) granularity §2 requires.
- **Spec fix:** specify the holder key (session identity) and the presence
  predicate (any unexpired holder), and that per-subject holder lists live off
  the RouteVec entry.

## 4. Default interest filter excludes `_`-prefixed subjects — where?
- **Ambiguous:** §2 says the `CacheSubListener` wildcard default is
  "everything except `_`-prefixed subjects", but submgr's `add_wildcard`
  filter mechanism (`do_wild_subscription`) can only *narrow* to a positive
  prefix (`_RV.INFO.LISTEN.START.<wild>`), it has no negative match.
- **Chosen:** subscribe submgr to everything (`start_subscriptions(all=true)`
  when no `-w`), then **drop `_`-prefixed subjects inside
  `on_listen_start`/`on_listen_stop`**. Without this, rv_cache's own
  `_TIC.>` (net 1) and `_SNAP.>`/advisory listens (net 2) show up as
  downstream "holders".
- **Why:** submgr is an event source; the `_`-exclusion is a listener-side
  policy. Confirmed empirically: before the guard, `_TIC.>` and `_SNAP.>`
  appeared as subscribed subjects.
- **Spec fix:** state the `_`-prefix exclusion is applied in the listener
  callback, not via a submgr subscription filter.

## 5. `_SNAP` routing: submgr `on_snapshot` is the single code path
- **Ambiguous:** §4 "two entry points, one code path": a raw `_SNAP.<subject>`
  publish with a reply, and `RvSubscriptionDB::on_snapshot()`.
- **Chosen:** route *all* `_SNAP` handling through submgr's `on_snapshot`
  callback. `RvSubscriptionDB::process_pub` already recognizes `_SNAP.>`,
  parses the `flags` field, strips the 6-char prefix, and fires
  `on_snapshot(sub, reply, flags)` (submgr.cpp IS_SNAP path).
- **Why:** avoids duplicating the `_SNAP.` prefix parse and gives one reply
  path. This does let submgr's `snapshot()` find-or-create an entry in *its*
  `sub_tab` per `_SNAP`, but that is low-volume (per-subject requests); the
  firehose bloat concern of resolved-question-1 is about the `_TIC.>` path,
  which uses rv_cache's own read-only table.
- **Spec fix:** say explicitly that the raw `_SNAP` publish is handled via the
  submgr `on_snapshot` callback (not a separate subject match in rv_cache).

## 6. Snapshot reply is the merged image verbatim (MSG_TYPE not restamped) — SUPERSEDED (2026-07-14 revision: in-place stamping, see addendum)
- **Ambiguous:** §4 "Reply payload: the cached image as-is" vs. the intuition
  that a cache serving an initial should send `MSG_TYPE=INITIAL`.
- **Chosen:** reply with the stored image bytes verbatim. After
  INITIAL→UPDATE merge, the stored image's `MSG_TYPE` reflects the last merged
  tick (UPDATE), because MSG_TYPE is just another field overwritten during
  merge. Verified by test 5 (reply shows the merged 3+ field image with the
  updated value).
- **Why:** "as-is" is taken literally; restamping would require rebuilding the
  SASS header on every reply.
- **Spec fix:** state whether snapshot replies must restamp `MSG_TYPE=INITIAL`
  or return the stored image unchanged.

## 7. Merge algorithm (field key, order, fallback)
- **Ambiguous:** §3 UPDATE/CORRECT "field-merge ... iterate update fields,
  overwrite/append into a rebuilt image (RvMsgWriter into fresh MDMsgMem, then
  swap)".
- **Chosen:** field **name** is the merge key. Pass 1 iterates the existing
  image in order, writing the update's value when the field is present in the
  update (`MDFieldIter::find`) else the retained value. Pass 2 appends
  update-only fields at the end. On any unpack failure or writer overflow,
  fall back to whole-image replace so the cache stays coherent.
- **Why:** preserves record field order (conventional for SASS records) and
  the "overwrite/append" wording; RVMSG is self-describing so `dict == NULL`.
- **Spec fix:** name the merge key (field name) and the ordering rule
  (retain existing order, append new fields).

## 8. `seq=stamp` — cache seqno tracked, outgoing field not rewritten
- **Ambiguous:** §3 `seq=stamp`: "cache maintains its own per-subject update
  seqno and stamps outgoing merged images with it (the `RWF_Cache_Seqno`
  pattern)".
- **Chosen:** maintain `own_seqno` per entry and use it as `last_seqno`;
  `observe` (gap/regress stats) and `strict` (drop non-increasing, 16-bit
  wrap-aware) are fully implemented. The in-place rewrite of the `SEQ_NO`
  field of the *outgoing* image bytes is **deferred** (best-effort stamping of
  the wire field left for a later cut).
- **Why:** observe is the default and the primary test path; full outgoing
  restamping needs an in-place field mutation of the published buffer that
  adds risk for little M1 value.
- **Spec fix:** clarify whether stamp must mutate the emitted `SEQ_NO` field or
  only track a cache-side monotonic counter.

## 9. Host-correlation sweep is present but degenerate in M1 — SUPERSEDED (2026-07-14 revision: submgr's own sweep machinery; sass3_db concern only)
- **Ambiguous:** topology section: a daemon-session `LISTEN.STOP` must expire
  every lease attributed to that host for the subject "at once".
- **Chosen:** `holder_remove` sweeps the exact session match **and** any holder
  with the same `host_id` when the stop is from a daemon (rv7) session. In M1
  only advisory holders exist, so the cross-dialect sweep is effectively a
  no-op beyond the matching session; it becomes meaningful with SASS3 /
  flagged-`_SNAP` holders in milestone 2.
- **Spec fix:** none needed for correctness; note that the sweep is only
  observable once non-advisory holders exist.

## 10. `-Q` value parsing
- **Ambiguous:** CLI lists `observe | strict | stamp` but not how unknown
  values are treated.
- **Chosen:** `strict`→strict, `stamp`→stamp, anything else (incl. `observe`
  and typos)→observe (the documented default).
- **Spec fix:** state the fallback for unrecognized `-Q` values.

## 11. Per-role `-1..-4` triple format
- **Ambiguous:** CLI shows `-1 d,n,s` etc. without empty-field semantics.
- **Chosen:** comma-separated `d,n,s`; any empty field falls back to the
  corresponding base `-d/-n/-s`. Stored via `strdup`.
- **Spec fix:** define the empty-field fallback rule explicitly.

## 12. Accounting log (`-A`) — subset emitted in M1
- **Ambiguous:** §6 defines a rich JSONL schema; the M1 task scope centers on
  §6's first paragraph (stats), not the full DACS log.
- **Chosen:** implemented JSONL for `subscribe` / `expire` / `snapshot` /
  `initial` with `ts, event, subject, host, host_id, session, protocol,
  query_flags` and (on close events) `reason, open_secs, msgs, images`.
  `user`/`app`/`pid` come from SASS3 / flagged-`_SNAP` accounting submessages
  (milestone 2) and are omitted for advisory holders. `resubscribe`/`sweep`/
  `host_stop` events are deferred to M2.
- **Spec fix:** none; scope was intentionally partial for M1. Noted so the M2
  author knows which events/fields remain.

## 13. Nets 3/4 (`-S`/`-F`) stubbed
- Per task scope, `-S` and `-F` are parsed but rejected at startup with a clear
  "not yet implemented (milestone 2)" error; `-P` (pending-initial timeout) is
  parsed but only meaningful in `-S` mode, so it is currently unused.

---

## Test results (test/basic.sh, against sassrv `rv_server`)
Spec tests exercised in this environment (no TIB install; sassrv's own
rvd-compatible `rv_server` used):

- **Test 1 (interest, default mode):** PASS — a second `RvSubscriptionDB`
  (`rv_subtop`, "the feed's view") sees `listen_start TEST.FOO` and, after the
  consumer exits, `listen_stop TEST.FOO`. (Harness note: `rv_subtop` only
  `fflush`es in top mode, so the script line-buffers it via `stdbuf -oL`.)
- **Test 4 (forward-only-when-listened):** PASS — publish with no listener
  bumps `dropped_no_listener`; with a live listener on `TEST.7` the tick is
  delivered on the bare subject and `ticks_forwarded` increments.
- **Test 5 (cache + snapshot merge):** PASS — INITIAL then UPDATE for
  `TEST.0`, then `_SNAP.TEST.0` returns the merged multi-field image carrying
  the updated value (`SEQ_NO=1`, all record fields present).
- **Test 7 (miss path, broadcast mode):** PASS — `_SNAP.NO.SUCH.0` replies
  `MSG_TYPE=TRANSIENT(9) / REC_STATUS=NOT_FOUND(17)` and `snap_missed`
  increments.

`test/basic.sh` final: `RESULT: pass=7 fail=0 skip=0` (after the 2026-07-14
revision below; adds the MSG_TYPE=SNAPSHOT stamp assertion to test 5).

Tests not scripted here (out of M1 scope or need extra tooling): 3b/7c/11/15/16
(SASS3, nets 3/4 — stubbed), 8 (wildcard modes), 9 (NOSUBSCRIBERS
desync-recovery variant needs advisory suppression), 10 (soak), 13/14 (rv5 vs
rv7 session matrix and flagged-`_SNAP` — needs the golang rv7 traffic gens /
a `snaprv7test`). The NOSUBSCRIBERS emit path and rv5 initial-on-listen
code paths are implemented and fire in the accounting log, but were not given
dedicated scripted assertions in this cut.

---

# 2026-07-14 revision — submgr-owned interest + in-place MSG_TYPE stamping

SPEC.md was rewritten (same date) and the code updated to match. What changed:

1. **Interest is submgr's, not rv_cache's (supersedes notes 3 and 9).** The
   `SubEntry`/`Holder` presence table, the per-holder keepalive/decay clock,
   and `decay_holders()` are deleted. The forwarding gate is a read-only
   `sub_db->sub_tab.find()` + `RvSubscription::refcnt != 0` (`sub_refcnt()`).
   Lifecycle events come from submgr: `refcnt==1` on start = subject live
   (`interest_opens`), `refcnt==0` on stop = last holder gone
   (`interest_closes` + NOSUBSCRIBERS). Host-correlation sweeps are submgr's
   session/host timeout machinery — rv_cache adds nothing on the RV side; the
   hold timer becomes a sass3_db concern only (`-D` parsed, currently unused).
   `Stop.is_listen_stop` discriminates `listen_stop` vs `host_stop` reasons in
   accounting.
2. **In-place MSG_TYPE stamping (supersedes note 6).**
   `CacheTab::set_image()` normalizes stored images: when a `MSG_TYPE` field
   exists it is forced to be the *first* field with a fixed-width integer type
   (rebuild to RVMSG with a uint16 lead field when needed; typeless images are
   never injected). Serving then stamps the delivery kind into the cached
   bytes via `MDFieldIter::update()` (same size/type, in place, no rebuild):
   `INITIAL` for listen-start-inbox pushes and INITIAL_VALUES-flagged `_SNAP`,
   `SNAPSHOT` for plain `_SNAP` polls. `RvFieldIter::update` memcpy's into the
   unpacked buffer, and `MDMsg::unpack` references `e.image` without copying,
   so the stamp lands directly in the cache image; every send re-stamps, the
   stored value is dead weight. Endianness/width follow the existing field's
   `MDReference`.
3. **`-i` removed.** The client controls initial-on-listen: attaching an inbox
   to the listen-start *is* the request (rv5-only ability); no cache option.
4. **Accounting user identity (extends note 12).** `acct_event` now takes the
   `RvSessionEntry` and emits `"user"` from `RvSessionEntry::user_id`
   (captured by submgr from `SESSION.START`'s `userid` field / subscription
   query replies) — verified in the harness: `"user":"rv_client"` on both
   `subscribe` and `snapshot` events. `on_snapshot` receives the session
   submgr resolves from the reply inbox. Close events carry subject-level
   `msgs`/`images` attribution (spec: attribution, not per-holder delivery).
5. **Known M1 gap (unchanged):** wildcard client listens (`TEST.>`) make a
   sub_tab entry under the wildcard subject; concrete ticks don't match it —
   can-wildcard forwarding remains unimplemented (TODO in `sub_refcnt`).
