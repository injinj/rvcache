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
- [x] **Milestone 4 feed side** (implemented 2026-08-02, test/omm.sh B):
      `-<idx> feed omm <host[:port]> [net] <service>` over `EvOmmClient`
      + `OmmClientCB` (rvcache owns subscribe/unsubscribe).  Ready =
      on_connect after directory+dictionary; interest replays at ready
      (`omm_feed_ready`).  submgr refcnt edges → total-fwd_mask 0<->
      nonzero transitions (`interest_edge_set/clear`) → upstream
      subscribe/unsubscribe.  Envelope class → `handle_tic` type
      override (REFRESH⇒INITIAL, STATUS closed⇒DROP / open⇒TRANSIENT +
      rwf_code_to_sass_rec_status, else rwf_to_sass_msg_type).
      **Deviation from spec:** RWF→sass RVMSG conversion at INGEST
      (RvMsgWriter + convert_msg(fields, skip_hdr)) reusing the whole
      existing tick path — field-list-native caching (type byte 0xca)
      remains the later optimization for omm-fed→omm-served.  Login
      attrs default (user "rv_cache") — NULL user = strlen crash in the
      login msg-key writer.  `no_dictionary` always set (local dict
      mandatory).  Feed loss = fail-fast like the rv nets (reconnect +
      batched replay deferred).  `send_snapshot()` for _SNAP-w/o-
      interest deferred (misses NOT_FOUND as before).
- [x] **Milestone 4 client side** (implemented 2026-08-02, test/omm.sh
      A): `-<idx> sub omm <[host:]port> [net] <service>` over
      `EvOmmListen`; service announced via the directory-map path
      (`announce_cache_service`: RwfMapWriter INFO+STATE filter lists →
      `update_source_map` — add_source() alone builds no sector
      routes).  `OmmSubNotify` (RouteNotify, src_type 'O' filter) →
      on_omm_sub/unsub → interest edges + solicited initial from cache
      (`omm_forward`) or STATUS suspect/open miss (`omm_send_status`;
      **deviation:** open+suspect instead of CLOSED_RECOVER — stream
      stays live, a later INITIAL refreshes, mirroring rv5 semantics).
      Update fan-out in handle_tic: one canonical RWF envelope per tick
      (`omm_forward`), EvOmmConn stamps per-client stream ids.
      Unmappable fields (no fid) are dropped by convert_msg.  Service
      health directory updates on feed loss: TODO.
- [ ] Milestone 4 leftovers: reconnect + batched interest replay on the
      omm feed (currently fail-fast); `send_snapshot()` for _SNAP
      without interest + pending-inbox replies (InboxReplyTab pattern);
      service-health directory updates (OmmSourceDB listener); field-
      list-native caching for omm-fed MARKET_PRICE subjects (Map domains
      are RWF-native already, see Milestone 5).
- [x] (2026-09-22) **Milestone 5 phase 1 — RWF Map caching (book
      domains).** `handle_rwf_map`: Map payloads cached as
      `RWF_MAP_TYPE_ID`, `CacheTab::build_merge_map` rebuilds by entry
      key (ADD replace / UPDATE fid-merge / DELETE / summary merge / set
      defs kept, set-encoded update entries re-encoded), multipart
      refresh via `image_partial`, raw forward of the feed envelope with
      SOLICITED cleared, solicited serve via `add_raw_container`.  raimd:
      `MAP_ENTRY_DEAD` iterator skip + ETA-correct perm gate,
      `RwfFieldListWriter::append_iter`.  Verified against a book rebuilt
      from the feed stream (`/tmp/bookcheck.py` style) and ETA Consumer.
- [x] (2026-09-23) **Milestone 5 phase 2 — in-place Map merge**
      (`src/map_merge.cpp`).  Tombstones (`MAP_ENTRY_DEAD`, live count in
      the header), UPDATE refit in place (key-prefix widening for 1 byte
      of slack, dead DELETE filler for 2+), tombstone + append on growth,
      compaction when dead > live and > 1 KB, writer-side `MapIndex`
      per `CacheEntry` (key hash → offset, validated by
      `CacheEntry::image_serial` = `KeyCtx::serial` in shm mode, rebuilt
      by prefix scan), serve strips (`live_image`).  Config `map_merge:
      inplace|rebuild`, `map_index_min` (1 = always index, 0 = scan).
      Merge time per update, `/tmp/mapbench.sh`, 50 ticks/s, 20 s each,
      all runs book-checked MATCH:

        keys / image     rebuild   inplace+scan   inplace+index
           4 /  250 B      5.3 us      5.8 us         3.5 us
           8 /  440 B      4.2        4.0            3.8
          62 /    3 KB      6.8        5.8            3.8
         399 /   20 KB     16.5       10.2            4.1
        2400 /  119 KB     75.6       31.7            4.2
        8400 /  418 KB    256.0      164.5            4.3

      The index is flat: ~4 us is the fixed cost (unpack the update,
      fid-merge one field list through the dictionary, encode, memcpy).
      There is no book size where it loses — below ~10 keys the three
      are within noise — so the default indexes every book (128 B min).
- [x] (2026-09-23) **shm mode exercised** (`map_name: sysv:rvbook.shm`,
      512 MB test map via `kv_server -m sysv:rvbook.shm -s 512`, removed
      with `-r` after): in-place+index 3.9 µs (399 keys) / 6.9 µs (8.4k
      keys, 418 KB), compactions on small books, rebuild mode — all
      MATCH.  Two bugs found: `shm_merge` handed `build_merge` the
      HashEntry type *byte*, so the Map dispatch never fired in shm
      (fixed: `enc_of_type_byte`); the index serial must be read right
      after `acquire()` (the sealed value) and before `value_update()`
      bumps it, else every merge rebuilt the index.  Cross-process serve
      verified: a second rv_cache (provider only, same map, no feed, no
      index) served the writer's book stripped of tombstones — matched
      the writer's client at the same seq (236 keys); `live_image` now
      strips unless THIS process's index is current and reports zero
      dead bytes, and takes the enc from `get_image` (a reader's
      `CacheEntry.image_enc` is 0).
- [x] (2026-09-23) **`test/map_bench`** — `CacheTab::merge` in a loop on a
      BookRoute-generated refresh + N deterministic updates, no network,
      hardware counters (perf_event_open) around the loop.  The live
      rv_cache numbers (3-4 µs) were 10× the isolated cost: a merge right
      after epoll wake-up runs cold (C-state, caches), and whole-process
      `perf stat` is swamped by the poll loop.  Per merge, 398-key book,
      pinned:  in-place+index **6.2k instr / 1.4k cycles / 0.25 µs**,
      in-place+scan 59k / 12.6k, rebuild 223k / 37k / 7 µs.  shm adds
      ~550 instructions (acquire / value_update / release).  The rest of
      the shm gap was `resize(copy)` copying the image on growth (4,120
      L1 misses per merge on a 600 KB book): fixed with **tail slack** —
      appends land in a trailing dead DELETE filler (a valid tombstone,
      1/8 of the image, 1 KB..32 KB, `map_grow_len`), so growth copies
      once per slack refill.  After: 600 KB book shm 3.3k cycles / 80 L1
      misses vs heap 1.8k / 25; 20 KB book 1.5k vs 1.4k cycles.
- [x] (2026-09-23) **shm key path slimmed** (Chris): per-op `EvKeyCtx`
      (128-bit rehash + placement of a full EvKeyCtx) replaced by
      `CacheTab::set_key`: the NUL-terminated subject is copied into a
      small `keybuf` KeyFragment (keylen = len + 1, the EvKeyCtx / raids
      string-key convention kept on purpose — the format is unchanged)
      and the hash is cached in `CacheEntry::khash1/2`.  shm overhead per
      merge went from ~550 to **~175 instructions / ~90 cycles** (398-key
      book: 6,387 vs 6,211 instr, 1,491 vs 1,399 cyc).  (A zero-copy
      variant reinterpreting the RouteVec trailer as the KeyFragment was
      tried and reverted: it drops the NUL.)
- [ ] Milestone 5 leftovers: `image_partial` lives in the heap entry only
      (a reader process could serve a half-received multipart book);
      an update carrying summary data takes the rebuild path (header
      rewrite); per-book index memory is unbounded by config (8 B/slot,
      2× live keys).
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
