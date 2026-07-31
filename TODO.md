# rvcache — TODO

Status snapshot 2026-07-23. Milestone 1 (rvd-native cache + interest-gated
forwarding), arbitrary net attachments, the sass3 downstream interest
channel, and the `_SASS.<feed>.PUB` envelope feed consumer are done and
tested (`test/basic.sh`). What follows is what's left.

## Finish the rename (rv_cache → rvcache)

Done: repo dir, SPEC.md, README.md, `rpm/rvcache.spec`, `.copr/Makefile`,
`deb/control` Package/Source, `include/rvcache/`. Still carrying the old
name:

- [ ] `GNUmakefile`: `rv_cache_files/objs/deps`, `$(bind)/rv_cache`,
      `dist_bins`, `install` — decide whether the installed binary becomes
      `rvcache` (rpm spec name says yes)
- [ ] `src/rv_cache.cpp`: help text, `rv_cache_net%u` session-user string;
      optionally rename the file itself to `src/rvcache.cpp`
- [ ] `include/rvcache/cache.h`: `__rv_cache__cache_h__` guard
- [ ] `test/basic.sh`: `RVC="$BLD/rv_cache"` + log labels
- [ ] `deb/control`: Homepage still `github.com/raitechnology/rv_cache`
- [ ] `IMPLEMENTATION_NOTES.md`: title + prose (historical log — lowest
      priority)

## Milestone 2 — SASS3 upstream client (`-S`)

*(2026-07-23: the placeholder `-S`/`-F`/`-D` flags and their Config
fields — `sass3_feed`, `sass3_name`, `hold_secs` — were removed from the
source; reintroduce `-S`/`-D` with the implementation. `-F` is dropped
for good: plain `sub,sass3` nets serve `_SASS.<name>.SUB` interest,
scoped by the per-net wildcard.)*

- [ ] Reintroduce `-S <feed>` + `-D <secs>`: `_SASS.<feed>.SUB` with
      `SUBSCRIBE|INITIAL_VALUES` on refcnt 0→1, `UNSUBSCRIBE_FLAG` on
      last-listener-gone, `RESUBSCRIBE` lease reasserts batched and spread
      across the `-D` window; consume acks on the reply inbox
- [ ] Pending-initial flow: `-P` is parsed but unused — TEMP_UNAVAIL(7)
      miss reply in interactive mode, pending table, broadcast-on-arrival,
      timeout stat (SPEC §4, test 7c). `-P` stays in the CLI: the pending
      table is generic to ALL interactive-feed types, not sass3-specific
      — sass3 (`-S`) is just the first upstream that can be asked
- [ ] Self-loop guard on `-S`: reject upstream feed name inside the
      downstream-served subject space when the sass3 attachments share
      network parameters

## Wiring gaps (milestone 1 leftovers)

- [x] ~~`-D hold_secs` parsed but never passed anywhere~~ — removed
      2026-07-23 along with `-S`/`-F`; the downstream lease window stays
      submgr-internal (480s) until milestone 2 makes it configurable
- [ ] Wildcard consumers (`can-wildcard`, SPEC §2): `fwd_mask` is
      exact-subject — a listener on `TEST.>` gates nothing today. Implement
      both policy meanings (tap vs. interest) on the tick path
- [ ] Close-event `msgs`/`images` in the accounting log use subject-level
      counters (`forward_count`/`snap_count`), not per-holder attribution —
      fix or note as an accepted M1 approximation in SPEC
- [ ] Per-holder usage-accounting dump on exit / on demand (SPEC §6) —
      only the `-A` JSONL stream exists

## Tests (basic.sh has 1, 4, 5, 6b, 6c, 6d, 6e, 6f, 7)

- [ ] 3b: `-S` SUB message assertions (blocked on milestone 2)
- [ ] 5b: MSG_TYPE lifecycle — VERIFY seed + merge, CLOSING merge with
      extra fields, DROP forward-then-evict
- [ ] 6: rv5 initial-on-listen **warm** path (`rv5_api_test` inbox listen →
      INITIAL arrives; 6b only covers the miss)
- [ ] 7b: TRANSIENT pass-through (forwarded, cache untouched)
- [ ] 7c: interactive-mode pending initial (blocked on milestone 2)
- [ ] 8: wildcard modes (blocked on can-wildcard implementation)
- [ ] 9: NOSUBSCRIBERS on last unsubscribe (two listeners, A then B)
- [ ] 10: churn/GC soak — 1k subjects × start/stop, RSS flat, no stuck
      refcnts
- [ ] 11: sass3 lease-lapse decay + mixed rv/sass3 interest on one subject
      (6d/6e cover subscribe/forward but not decay or mixing)
- [ ] 12: collapsed-network self-loop assertion
- [ ] 13: session-model matrix — rv5 + rv7 + sass3 same subject;
      `fanrv7test` same-host second-listener (no new advisory) case
- [ ] 14: flagged `_SNAP` **warm** path + no-`_SNAP`-lease assertion (6c
      only covers the miss)
- [ ] 15: ghost-window host-correlated lease expiry (kill without
      UNSUBSCRIBE → daemon LISTEN.STOP sweeps the lease)
- [ ] 16: accounting-log assertions over 11/14/15 (jq reconstruction)

## Milestone 3

- [x] raikv shared-memory KV image store (first slice, 2026-07-26):
      `CacheTab::init_shm()` attaches EvShm.map; images live as
      subject-keyed kv values (`ShmImageHdr{enc} + bytes`) via `EvKeyCtx`
      → `KeyCtx` acquire/resize/release; merge is a single-lock RMW
      (`shm_merge`: unpack old in place, `build_merge` into scratch,
      normalize, resize+copy); serves go through `get_image()` (shm
      copy-out to imgbuf, stamped per delivery) and `find_for_image()`
      mints local metadata entries so images cached by ANOTHER process
      are served (verified: `test/shm.sh` B).  Heap path unchanged when
      no `-m` (`test/basic.sh` unchanged).  Subscription tables stay
      process-private (SPEC §3).  Dictionary (`-p`/`dict_path`,
      `MDMsgDict`) loaded but unused so far as well.
      Left for later: eviction sweep of snap-minted imageless local
      entries; image_bytes stat is per-process approximate in shm mode;
      `EvKeyCtx` currently stack-primed per op — the prefetch batching
      (design notes below) queues these same objects.
      2026-07-27 update (after raimd 718af66): `ShmImageHdr` dropped —
      the value is bare image bytes and the encoding is the HashEntry
      type byte (`set_type((uint8_t)TYPE_ID)`, matcher-ftype convention,
      same slot raids uses for redis types); `MDMsg::unpack()` takes the
      byte as msg_enc hint.  Publish paths expand byte → 32-bit id via
      the matcher table (`enc_of_type_byte`) because `make_rv_msg` /
      `EvPublish.msg_enc` switch on full ids.  `build_merge` now gets its
      writer from `MDMsg::create_writer()` so merges preserve the cached
      codec (TIBMSG stays TIBMSG); RvMsgWriter fallback when a codec has
      no writer.  NOTE: `normalize_msg_type` still rebuilds via
      RvMsgWriter when MSG_TYPE isn't leading — that path (rare) still
      converts to RVMSG; could use create_writer + append_iter with a
      hand-built leading MSG_TYPE if codec preservation matters there.
- [ ] bloom-gated forward + batched merges (SPEC §"Milestone 3 design
      notes", 2026-07-26): per-net `BloomBits` front-end so the forward
      decision touches no shm; forward-first on hit (rvd filters false
      positives); merge path batches the recv drain with per-subject
      coalescing + `prefetch_array` two-phase pipeline. Gate prefetch by
      batch depth; keep the whole thing optional.
- [ ] RWF/OMM net type (SPEC §"RWF / OMM nets"): cached element = the
      field list (`msg + EvPublish.hdr_len`), type byte
      `(uint8_t)RWF_FIELD_LIST_TYPE_ID`; delivery kind from
      `RwfMsgPeek::get_msg_class()` (REFRESH⇒initial, UPDATE⇒merge,
      STATUS⇒transient); serve = build solicited REFRESH envelope
      around the cached field list (per-requester stream_id).
- [ ] **Milestone 4 feed side** (SPEC §"Milestone 4 design — OMM
      protocol endpoint", 2026-07-31): `-<idx> feed omm <host:port>
      <service>` net over `EvOmmClient` with an `OmmClientCB` (rvcache
      owns subscribe/unsubscribe; no RouteNotify self-attach).  Ready =
      on_connect after directory+dictionary; interest replay at ready,
      batched on reconnect.  submgr refcnt edges → subscribe/
      unsubscribe; `_SNAP` w/o interest → `send_snapshot()`.  Envelope
      class → tick-path decision; multi-part REFRESH until
      REFRESH_COMPLETE; STATUS CLOSED⇒evict, stale⇒forward status keep
      image.  Dict: `-p` ⇒ no_dictionary, else wire download; neither ⇒
      refuse to start.  Config keys: user, app_id, app_name,
      instance_id, token.  omm-fed subjects default route_after_merge
      (cross-codec fwd to RV nets).
- [ ] **Milestone 4 client side** (SPEC §"Client side: EvOmmListen net",
      2026-07-31, full spec): `-<idx> sub omm <listen> <service>` —
      EvOmmConn inherits stream tables / solicited gating / stream_id
      rewrite / fragmentation; rvcache adds (1) RouteNotify glue on the
      listener's sub_route (on_sub⇒interest_set, on_unsub⇒clear;
      snapshot requests serve-and-close, no interest), (2) the sass→RWF
      converter (strip sass hdr, RwfFieldListWriter::convert_msg
      skip_hdr, RwfMsgWriter envelope; convert ONCE, per-client stream
      stamping is EvOmmConn's job), (3) service-health wiring
      (feeds down ⇒ directory suspect; OmmSourceDB listener), (4) config
      plumbing.  MSG_TYPE→msg_class map + miss mapping (TRANSIENT⇒
      STATUS CLOSED_RECOVER, DROP⇒CLOSED) in the SPEC.  Dict required
      (fname→fid) — refuse to start the net without it.
- [ ] hand CacheTab the loaded `MDMsgDict` — `build_merge`/unpack
      currently pass NULL dict; RWF field-list iteration needs it (this
      is what the "-p loaded but unused" item was waiting for).
- [ ] the snapshot-drain invariant: snapshot serve applies the subject's
      pending deltas before reading the image — REQUIRED once
      forward-precedes-merge lands; loud comment at the serve site.
- [x] (raims-side, done 2026-07-28) `_INBOX` reply path pinning:
      `EvPublish.path_hint` (raikv) set in sassrv `ev_rv.cpp` —
      listen-start delivery records `crc(bare subject)` in a ring slot
      keyed by the reply inbox's trailing id (`update_reply_hint`,
      `&sub[29]` strips the LISTEN.START prefix); inbox publishes stamp
      the hint; raims `session.cpp` routes inbox with
      `hash_to_path(path_hint ?: subj_hash)`, primary fallback.  The
      initial rides the subject stream's path — the gru 4-wide skew is
      fixed by construction.  Verify with rv_client `initial_late` /
      `update_before` counters under replay load.
