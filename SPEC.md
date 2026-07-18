# rv_cache — RV subject cache with interest-driven tick forwarding

A sassrv + raimd caching test. Sits between a feed side (publishers that
send ticks on `_TIC.<subject>`) and a consumer side (RV clients that listen
on bare `<subject>`), maintaining a last-image cache per subject and
answering snapshot RPCs. Subscription state on the network is tracked with
`sassrv/src/submgr.cpp` (`RvSubscriptionDB`), which is what makes
interest-driven forwarding possible: ticks are only re-published for
subjects that currently have listeners, and interest is forwarded upstream
so the feed side can start/stop publishing per subject.

**Interest model — two modes:**

*Default (rvd-native, no extra protocol):* interest travels in the rvd's
own advisories, `_RV.INFO.SYSTEM.LISTEN.[START|STOP].<subject>`, managed by
`RvSubscriptionDB` — including the `_RV.INFO.SYSTEM.HOST.STATUS` keepalive,
which drives re-query of a host's sessions/subscriptions when its status
goes quiet (`RvHostEntry::check_query_needed`, 100s late →
`RV_HOST_QUERY`). A feed that wants to know what to publish runs its own
`RvSubscriptionDB` filtered to its subjects — clients speak nothing extra.
This is the recommended topology and the historical moral: **SASS3 existed
because the later Tibco APIs had no submgr facility to query open
subscriptions; sassrv's submgr restores it, making the extra protocol
unnecessary for new code.** rv_cache in default mode is purely cache +
interest-gated forwarder + snapshot service.

*Legacy-compat (`-S <feed>`, optional):* for feeds that speak SASS3 (old
APIs only — the protocol is undocumented; the authoritative source is
`~/rai/RaiCore/src/cache/sass3_svc.cpp`), rv_cache acts as a SASS3
subscriber upstream: batch-resubscribes interest to `_SASS.<feed>.SUB`
(`Sass3SubscribeConsumer::onMsg`) and consumes the `_SASS.<feed>.PUB`
broadcast envelope (`Sass3Svc::doFeed`) instead of raw `_TIC.>` ticks.
Wire format in the appendix below.

*Feed-type framing (raicache vocabulary):* the default `_TIC.>` mode is a
**broadcast-feed** — a passive listener on whatever the feed blasts. The
`-S` mode is the **interactive-feed** pattern — the client-side
subscription protocol is forwarded toward the feed network, which then
publishes only what was asked for. Interactive feeds are atypical inside
rvd networks; where they occur, the interactive side is usually an
exchange feed. rv_cache treats broadcast as the norm and interactive as
the compat/bridge case, matching that reality.

## Network topology — four RV attachments

Presume **four networks**. Each is its own `EvRvClient` session on the
shared `EvPoll`, with its own `(daemon, network, service)` parameters. The
parameters **may be identical** — collapsing any or all four onto one
physical rvd/network is a deployment choice, not a design assumption. The
code never assumes two roles share a session, and never requires them to
be distinct either.

| # | Network | rv_cache role | Traffic |
|---|---------|---------------|---------|
| 1 | **rv feed** | data consumer (upstream) | inbound `_TIC.>` ticks (broadcast-feed) |
| 2 | **rv sub** | data publisher (downstream) | outbound `<subject>` forwards, `_SNAP.>` RPC service, `_RV.INFO.SYSTEM.*` submgr interest tracking |
| 3 | **rv sass3 feed** | SASS3 *client* (upstream) | send `_SASS.<feed>.SUB`, consume `_SASS.<feed>.PUB` envelopes (`-S` mode) |
| 4 | **rv sass3 sub** | SASS3 *service* (downstream) | listen `_SASS.<name>.SUB` from legacy clients — **subscription maintenance only**; acks + initial images to reply inboxes. Data is NOT enveloped: subscribers listen on the bare `<subject>` itself |

Networks 1+2 are the rvd-native data path; 3+4 are the legacy-compat SASS3
path, each optional and independently enabled (`-S` enables 3, `-F` enables
4). The SASS3 side (nets 3+4) is **specified but not yet implemented** —
milestone 1 covers nets 1+2 only. Fully loaded, rv_cache is a **SASS3 concentrator/bridge**: legacy SASS3
feed upstream, legacy SASS3 clients downstream, native RV consumers and
feeds beside them on the same cache.

**Interest ownership — submgr refcnt on the RV side, leased refcnt in
sass3_db on the SASS3 side:** downstream interest is tracked by two
databases, one per protocol family. On network 2, `RvSubscriptionDB`
(`sub_db`) owns subscription life outright: advisories and session/host
queries drive `RvSubscription::refcnt` (one ref per session identity;
`ref7cnt` marks a daemon-session holder), and **`refcnt != 0` is the
forwarding gate** — no parallel rv_cache interest table, no RV-side decay
timer. The advisory protocol already provides the full lifecycle:
LISTEN.STOP derefs, session/host timeout sweeps deref, GC removes. On
network 4, a `Sass3SubscriptionDB` (`sass3_db`, mirror-shaped after
sub_db; **not yet implemented** — §5) owns SASS3 interest: the QueryFlags
of incoming `_SASS.<name>.SUB` messages manage the refcnt (SUBSCRIBE refs,
UNSUBSCRIBE derefs, RESUBSCRIBE renews the lease) and the **hold timer is
internalized in sass3_db** — SASS3 leases are the one place interest may
decay by clock, because legacy clients have no advisory channel. A tick is
forwarded on a downstream network when that network's database shows
refcnt > 0. A refcnt is still not a head-count — see the
session-granularity note below. Upstream, network 1 is a passive firehose
(no per-subject interest) and network 3 asserts the merged interest set
via the SASS3 lease cycle when enabled.

**Session granularity — why refcounts under-count:** the RV5 protocol
attaches each subscription to the client's own *session*, so per-session
listens are visible and honest. RV6+ shares one *daemon session* among all
clients on a host — the daemon emits LISTEN.START only **once** per
subject per host regardless of how many local listeners exist (and one
LISTEN.STOP when the last local listener goes). rv6+ clients therefore
under-count at the advisory level, and a second listener joining an
already-listened subject sees no fresh START (so no reply-inbox initial) —
it pulls its initial via `_SNAP` instead. Consequences: the subscription
tables count refs per (session identity, subject) — in sub_db a session
identity is an rv5 client session or an rv6+ daemon session (≈ a whole
host, flagged by `ref7cnt`); in sass3_db it is a SASS3 accounting
identity; NOSUBSCRIBERS / last-listener logic operates at that
granularity; and nothing may assume one ref = one listener. The
sassrv repo ships both client styles for testing — rv5-style (`rv_client`,
`rv5_api_test`) and rv7-style (`subrv7test`, `fanrv7test`, …).

*Discriminating the two:* an rv7 session/inbox contains the literal
**`.DAEMON.`** component — e.g. `_INBOX.0A0A0A0A.DAEMON.<session id>`,
where `0A0A0A0A` is the host id (IPv4 10.10.10.10 in hex). Any session or
inbox **without** `.DAEMON.` is rv5. The difference is negotiated at
connect and is a genuine protocol difference: an rv5 client *names its own
session* during initial setup; an rv7 client *receives its session from
the daemon*.

*The rv5 dialect is not just legacy:* the injinj NATS and redis servers
(`natsmd`, `raids`) were designed to **emit rv5-type protocol for their
pubsub** — meaning those servers can themselves be rv_cache clients, and
NATS subscribers / redis SUBSCRIBE clients transitively consume cached
subjects with no changes on their side. rv_cache treats them as ordinary
rv5 session holders (they name their own sessions); interest, initials,
host correlation, and accounting all apply unchanged. Deployment realism:
this is an option, not an expectation — people run their NATS and redis
servers as-is — but the design costs nothing here because it is just the
rv5 surface being used by another program.

**Host correlation — bounding the ghost window:** every client carries a
reference **host id, the IPv4 address in hex**: inbox names embed it
(`_INBOX.DAEMON.<iphex>`), HOST.STATUS carries it, LISTEN.START/STOP carry
it — entry points `RvHost::send_listen_start` / `send_listen_stop` /
`send_host_status` in `sassrv/src/rv_host.cpp`. Holders from all three
interest dialects therefore attribute to a host id: advisory holders from
their session, SASS3 holders from the accounting `H` field or sender
inbox, flagged-`_SNAP` holders from their reply inbox. On the RV side this is submgr's own
machinery — LISTEN.STOP derefs the subscription, session/host timeouts
sweep a dead host's sessions and their refs — rv_cache adds nothing. Where
host correlation earns its keep is bounding *SASS3 lease ghosting*: SASS3
subscribers hold their bare-subject *data* listen through the host's
daemon like everyone else, and a daemon-session **LISTEN.STOP is
authoritative — sent only when the host truly has no remaining subs on
that subject** — so on STOP for (host, subject), sass3_db immediately
expires every lease attributed to that host for that subject instead of
waiting out the hold timer; host death (HOST.STATUS gone quiet) sweeps the
same way. The hold timer remains only as the backstop for SASS3 holders
whose host correlation is unknown.

**One data plane:** SASS3 separates subscription maintenance from data
delivery — `_SASS.<feed>.SUB` carries only interest, and SASS3 subscribers
receive data by subscribing to the bare `<subject>` like any RV client.
The `_SASS.<name>.PUB` envelope is a **feed-side construct only** (what an
upstream SASS3 feed broadcasts to caches; rv_cache consumes it in `-S`
mode, never emits it downstream). A cache-accepted tick is therefore
published once per downstream network as a bare-subject publish, serving
native-RV and SASS3 subscribers alike — this control/data separation is
precisely what lets the SASS3 protocol coexist with the RV protocol on the
same network. When networks 2 and 4 are distinct, the publish goes to each
downstream network whose refcnt for the subject is > 0 (interest is
tracked per downstream attachment: sub_db's advisory-driven refcnt for
net 2, sass3_db's leased refcnt for net 4).

**Collapsed-parameter safety:** when all four attachments point at one
network, loop freedom comes from subject namespaces — inputs (`_TIC.>`,
`_SASS.<feed>.PUB`, `_SASS.<name>.SUB`) are disjoint from outputs
(`<subject>`, `_SASS.<feed>.SUB`) **provided the downstream advertised
name differs from the upstream feed name**; `<name> == <feed>` on a shared
network would make rv_cache consume its own SUB sends, so startup rejects
that combination when the sass3 attachments share parameters. (The bare
`<subject>` output namespace overlapping the consumer side is by design —
that is the shared data plane — and it never overlaps inputs, which are
all `_`-prefixed.)

In the diagram below, the left column is networks 1/3, the right column is
networks 2/4.

```
 feed side                      rv_cache                     consumer side
 ---------                      --------                     -------------
 publish _TIC.FOO ---------->  cache[FOO] merge/store
   (or _SASS.<feed>.PUB        refcnt(FOO) > 0 ? ----------> publish FOO
    envelope in -S mode)                                     (submgr: _RV.INFO.SYSTEM.
                                                              LISTEN.START/STOP + HOST.STATUS)
 [-S mode only]
 listen _SASS.<feed>.SUB <---  batch resubscribe (lease refresh)
   (reply inbox = ack)         + UNSUBSCRIBE on last listener
                               on_snapshot(FOO):
 (no feed round trip)          cache[FOO] image ----------> reply to _INBOX
                                                  <--------- publish _SNAP.FOO
                                                             (reply = _INBOX.<sess>.N)
```

## Subject protocol

**Subject shape convention:** production subjects are 4-part —
`FEED.DOMAIN.INSTRUMENT.EXCHANGE` (e.g. `IDN.REC.IBM.N`). The SASS3 feed
name is the **first segment**. `DOMAIN` is usually `REC` for level-2
record data — same concept as `MARKET_PRICE_DOMAIN` (6) in raimd
`omm_flags.h` (which of `REC` or the RDM domain naming came first is
lost to history; Reuters and Tibco were once one company, and both
vocabularies are commonly understood by people who work these systems).
`EXCHANGE` is `NaE` when there is no exchange (`NO.SUCH.6C.NaE`);
raicache accepts 3-segment subjects and presumes the implied `.NaE` —
but **as distinct cache keys**: no normalization, `NO.SUCH.6C` and
`NO.SUCH.6C.NaE` are different entries. rv_cache does the same —
subjects are opaque strings (raicache's default behavior too). **The
feed side enforces subject discipline**, not the cache: a Bloomberg
feed or a hand-written exchange feed brings its own naming, and that —
not cache configuration — is what shapes the cache's subject space.

| Subject                 | Direction        | Meaning |
|-------------------------|------------------|---------|
| `_TIC.<subject>`        | feed → rv_cache  | Tick/update payload for `<subject>` (RVMSG). Cached, and forwarded to `<subject>` when it has listeners. (SASS3 broadcast-feed role; a full deployment would use `_SASS.<source>.PUB`.) |
| `<subject>`             | rv_cache → consumers | Forwarded tick payload, unchanged (or post-merge image with `route-after-merge` semantics, `-M`). |
| `_SNAP.<subject>`       | consumer → rv_cache | Snapshot RPC. Optional QueryFlags-style `flags` field: absent/SNAPSHOT → image reply (default); SUBSCRIBE\|INITIAL_VALUES → image for a subscribing rv7 client — subscription *life* stays with the advisory refcnt (no `_SNAP` lease). submgr resolves the requester's session from the reply inbox; its `user_id` feeds accounting. |
| `_INBOX.…` (reply)      | rv_cache → consumer | Snapshot image reply (point-to-point). |
| `_SASS.<feed>.SUB`      | rv_cache → feed  | *`-S` mode only.* SASS3 batch subscription message (wire format below): subject list + QueryFlags, reply inbox for ack/images. Periodic RESUBSCRIBE reassert = lease refresh (`subsc-decay-time` in raicache terms); UNSUBSCRIBE_FLAG on last-listener-gone. |
| `_RV.INFO.SYSTEM.*`     | rvd → submgr     | Consumed internally by `RvSubscriptionDB` (LISTEN.START/STOP, SESSION.*, HOST.STATUS keepalive → re-query). Not rv_cache's own protocol — and in default mode, the ONLY interest channel: the feed side runs its own submgr. |

Interest state is restart-proof in both directions: a restarted feed
rebuilds from the next RESUBSCRIBE cycle (`-S` mode) or from its own
submgr host/session queries (default mode); a restarted rv_cache rebuilds
consumer interest from submgr's queries and reasserts upstream from the
refreshed `sub_tab`.

## Components

All within one process, single `EvPoll` — same runtime shape as the other
sassrv test programs.

### 1. Transports: four `EvRvClient`s

One client session per network role (see topology above), all on the one
`EvPoll`. Base `-d daemon`, `-n network`, `-s service` flags set the
default for all four; per-role overrides (`-1`..`-4`) replace the triple
for that attachment. Sessions connect lazily — an attachment whose role is
disabled (no `-S`, no `-F`) is simply not created.

Subscriptions per session:

- **net 1 (rv feed):** `_TIC.>` — the feed firehose
- **net 2 (rv sub):** `_SNAP.>` snapshot RPC; `RvSubscriptionDB` adds its
  `_RV.INFO.SYSTEM.*` subscriptions and inbox query subjects here when
  `start_subscriptions()` is called
- **net 3 (sass3 feed):** reply-inbox only (SUB acks / images); publishes
  `_SASS.<feed>.SUB`, consumes `_SASS.<feed>.PUB`
- **net 4 (sass3 sub):** `_SASS.<name>.SUB` — the downstream SASS3 service
  listener (control plane only); data for net-4-held interest is published
  as bare `<subject>` on this attachment

### 2. Interest tracking: `RvSubscriptionDB` + listener impl

```cpp
struct CacheSubListener : public RvSubscriptionListener {
  RvCache &cache;
  virtual void on_listen_start( Start &add ) noexcept; /* first refcnt=1 -> _TIC.START, reply inbox -> initial image */
  virtual void on_listen_stop ( Stop  &rem ) noexcept; /* refcnt=0 / is_orphan -> _TIC.STOP */
  virtual void on_snapshot    ( Snap  &snp ) noexcept; /* image -> snp.reply */
  virtual void on_sass3       ( Sass3 &sa3 ) noexcept; /* sass3 interest, net 2 (sassrv f67b8ad) */
};
```

- **Interest channels — sass2 and sass3 (sassrv `f67b8ad`, 2026-07-17):**
  `start_subscriptions( bool all, bool s2, bool s3 )` enables each
  interest method independently: `s2` = the classic `_RV.INFO.SYSTEM`
  advisory set (LISTEN.START/STOP, host/session queries, `_SNAP`), `s3` =
  the SASS3 wildcard interest channel (`IS_SASS` subscription →
  `on_sass3` callback with `S3_*` QueryFlags + accounting identity
  user/host/app/pid and a leased `RvSass3Entry`). Both enabled together
  WORKS, but produces **double notifications per subscription**, and
  clients may receive **double initial values**. sass3-aware clients know
  sass3 coexists on sass2 networks and **use sass3 even when only sass2
  exists** — so a sass3-enabled cache must expect sass3 interest from day
  one. rv_cache enables both channels; whether initials are deduped per
  subject+holder across the two channels or the double-initial is
  accepted is an implementation decision (production clients tolerate a
  duplicate initial). `S3_REFRESH` from a client asks for **another image
  to the reply inbox** (serve like SNAPSHOT/INITIAL_VALUES).
  Both-channels-on-one-submgr is the **collapsed** deployment (one net
  carries both interest styles, one shared refcnt per subject).
  **Separate sass2 and sass3 networks take separate submgr instances** —
  one configured `(all, s2=true, s3=false)`, the other
  `(all, false, true)` — with independent sub_tabs and refcnts; the
  forwarding gate then ORs the per-submgr refcnt checks (the same
  fall-through shape §5 describes for the net-4 sass3_db), and each
  submgr asserts and broadcasts initials on its own refcnt 0→1
  independently.
- **Asserted interest → broadcast an initial.** When rv_cache discovers
  interest it did not see arrive — a sass2 subscription-query reply
  (`Start.is_listen_start == false`, no inbox) or a sass3 RESUBSCRIBE
  renewing a holder submgr didn't know (`Sass3.is_asserted`, REFRESH
  OR'd in by submgr, not the client) — those listeners predate the
  cache (typical at rv_cache startup) and already believe they are
  subscribed. If the subject just went live (refcnt 1) and an image
  exists, **broadcast an INITIAL on the subject** so every such
  listener converges; nothing goes to any inbox. Cache still cold →
  broadcast nothing: the feed's next INITIAL broadcasts normally.

- Constructed with a wildcard filter (`add_wildcard()`); default is
  everything except `_`-prefixed subjects, `-w RSF.>` style flag to narrow.
- **Wildcard consumers** (a client listening `TEST.>`) follow the
  raicache policy pair, both meanings implemented:
  - `can-wildcard=false`: the wildcard is a **debug/monitoring tap** — the
    listener receives whatever matching traffic is *already flowing*
    (forwarded because concrete subscriptions exist), but the wildcard
    creates no subscriptions: no interest upstream, no forwarding of
    subjects nobody concretely asked for.
  - `can-wildcard=true` (default, matching raicache): matching subjects
    are forwarded — the wildcard itself is interest for its match space
    (consult via `is_matched()` / `sub_hash_count()` on the tick path; in
    `-S` mode the wildcard is forwarded upstream as-is and the feed
    decides what it means).
  - `wildcard-initial` stays **off** (and unimplemented in the first cut)
    for the structural reason it's uncommon in production: an inbox reply
    carries no subject identity, so the requester cannot tell which
    subject in the wildcard space an initial belongs to; and broadcasting
    the initials instead makes them visible to every consumer. Initials
    for wildcard listeners remain pull-only via `_SNAP.<subject>` once the
    subject is known from the update flow.
- **Inbound dispatch — `process_pub()` returns consumed/not-consumed:**
  every publish arriving on net 2 is offered to
  `sub_db.process_pub( pub )` first. It returns **true when the message
  matched one of the `_RV.INFO.*` / `_SNAP.>` / inbox-query
  subscriptions** (control traffic: consumed, listener callbacks fired)
  and false otherwise — false means ordinary data, handle it on the data
  path. Net 4 mirrors the contract: `sass3_db.process_pub( pub )` returns
  true when it consumed a `_SASS.<name>.SUB` control message.
- **Forwarding decision** on a `_TIC.<subject>` arrival: read-only
  `sub_tab.find()` in sub_db — forward when the entry exists with
  `refcnt != 0`. submgr controls the subscription's whole life (advisory
  START/STOP, session/host queries, timeout sweeps, GC); rv_cache keeps
  **no parallel interest table** on the RV side. The read-only find — not
  `snapshot()`, which find-or-creates — keeps the feed firehose from
  minting entries for never-listened subjects (see resolved question 1).
  When net 4 is enabled, a sub_db miss falls through to the same refcnt
  check against sass3_db.
- **Interest upstream (`-S` mode, SASS3 lease):** `on_listen_start` with
  refcnt 0→1 sends an immediate SUB message
  (`SUBSCRIBE|INITIAL_VALUES` flags, that subject in the `S` submessage);
  `on_listen_stop` dropping to 0 (or `is_orphan`) sends `UNSUBSCRIBE_FLAG`.
  Live subjects are reasserted with `RESUBSCRIBE_FLAG` on the hold-timer
  cycle (`-D`, default **480s — the customary 8-minute subscription hold
  timer**), and the reasserts are **spread across the window** the way
  existing SASS3 clients spread theirs: the live set is divided into
  batches issued evenly over the period, so every subject is refreshed
  once per window without a thundering reassert burst; feed acks to the
  reply inbox (`sendAck` path fires on RESUBSCRIBE without IMAGE_FLAGS).
  In default mode this whole component is absent.
- **Initial image on listen (rv5-only, no CLI flag):** `Start.reply`
  non-empty and cache has an image → publish the image (stamped
  `MSG_TYPE = INITIAL`, §3) to the reply inbox. **Miss → status to the
  inbox, never silence:** `Start.reply` non-empty and no cached image →
  TRANSIENT / `NOT_FOUND` in broadcast mode (TRANSIENT / `TEMP_UNAVAIL` +
  pending-initial in `-S` mode), the same one-code-path as the §4 `_SNAP`
  miss — the requester attached an inbox precisely to learn the subject's
  state. The same rule covers sass3 initial requests
  (`S3_SUBSCRIBE|S3_INITIAL_VALUES` / `S3_SNAPSHOT` on a cold subject).
  *Gap noted 2026-07-17: the snap/initial RPC path sent the status but
  the listen-start initial path did not — test 6b pins the fix.* **The client controls
  this**, not a cache option: attaching an inbox to the listen-start *is*
  the request for an initial, and a client that doesn't want one sends a
  bare listen or pulls via `_SNAP` — so there is nothing to configure.
  Only rv5 clients can attach an inbox to a listen-start; **the rv7 API
  cannot**, so rv7 clients obtain initials via `_SNAP.<subject>`
  (optionally flagged, §4) or via `_SASS.<feed>.SUB` with IMAGE_FLAGS.
  Even for rv5, the pushed initial serves only the first listener per
  session identity — the `_SNAP` pull path is the general mechanism, not
  a convenience.
- `process_events()` driven from a 1s timer (submgr expects its
  `cur_mono` ticks + gc), same cadence used for stats.

### 3. Subject cache (raimd)

Keyed by subject; raikv `RouteVec<CacheEntry>` (same pattern submgr uses
for `sub_tab`), entry holding:

```cpp
struct CacheEntry {
  uint32_t   subject_id;     /* hash/id parity with submgr optional */
  uint64_t   update_count,   /* ticks received */
             forward_count,  /* ticks re-published */
             snap_count;     /* snapshots served */
  uint64_t   last_update_ns;
  uint32_t   last_seqno;     /* SASS seqno when present */
  uint16_t   msg_type;       /* last MD_SASS msg type seen */
  md::MDMsgMem mem;          /* owns image bytes */
  void     * image;          /* latest image blob (RVMSG bytes) */
  size_t     image_len;
  uint16_t   len;            /* subject */
  char       value[ 2 ];
};
```

**Merge semantics** (raimd is the point of this half of the test):

- Payloads unpack via `MDMsg::unpack()` — expected `RVMSG` on the wire,
  but anything raimd recognizes (TibMsg, TibSassMsg) is handled the same.
- SASS `MSG_TYPE` field (raimd `sass.h`), when present, drives cache policy:
  - `INITIAL` → **replace** the image outright.
  - `UPDATE` / `CORRECT` → **field-merge** into the existing image:
    iterate update fields, overwrite/append into a rebuilt image
    (`RvMsgWriter` into fresh `MDMsgMem`, then swap). Merging is what makes
    snapshots correct when the feed publishes deltas.
  - `VERIFY` → treated as an **update** (merge) when an image is cached;
    when the subject is *not* cached, it **creates** the image (a verify
    carries a full record, so it can seed the cache).
  - `CLOSING` → an **update** (merge), not a lifecycle event: it is the
    data at the closing bell, and ticker plants often attach additional
    closing fields. The entry stays live.
  - `DROP` → instrument lifecycle end, **forwarded from the feed** — the
    normal case is an instrument that will no longer update (an expired
    bond, a delisted symbol). The cache **forwards it to current listeners
    like any message** (they need to know the instrument died) and
    **evicts** the entry. raicache's `send-drop` option governs the
    related-but-different case of how to answer *later* requests for a
    dropped subject (drop message vs. not-found).
  - `TRANSIENT` → **status notification, never cached**: does not create,
    merge into, or evict an image. Forwarded to listeners like any tick
    (status is information the subscriber wants), but the cache is
    untouched. Most common form: `REC_STATUS = NOT_FOUND` for a subject
    that is in the subscription space but absent from the cache. Both
    constants are already in raimd (`sass.h`: TRANSIENT msg type,
    NOT_FOUND=17 rec status).

**DROP emitted BY the cache (consumer network):** distinct from a DROP
arriving from the feed. When the last listener of a subject goes away, the
cache publishes `DROP` with `REC_STATUS = NOSUBSCRIBERS` (=9, in
`sass.h`) on the subject — raicache's `send-nosubscribers`, default yes.
This is not a courtesy message; it is the **interest-desync repair
signal**: a client that is still listening and receives NOSUBSCRIBERS now
knows the cache's interest view has lost it (a network break ate its
LISTEN.START or the HOST.STATUS keepalive) and forwarding has stopped — it
SHOULD reassert its subscription so a fresh advisory fires. In practice
most clients do nothing and recovery happens from the other side: submgr's
HOST.STATUS-driven re-query cycle rediscovers the listen and interest
resumes. Both repair paths coexist; rv_cache emits the signal by default
so proactive clients get the fast path.
- No `MSG_TYPE` field → configurable default: `--merge` (treat every tick
  as a delta) or replace mode (default; cheapest, correct for full-image
  feeds).
- **MSG_TYPE normalization — first field, fixed size, updated in place:**
  whenever the cache builds or rebuilds an image (INITIAL replace, field
  merge) and the source carries a `MSG_TYPE`, the writer emits it as the
  **first field** with a fixed-width integer type. Outgoing deliveries
  then stamp the type appropriate to the delivery — `INITIAL` for a
  pushed/requested initial, `SNAPSHOT` for a snapshot reply — directly
  into the stored bytes with `MDFieldIter::update()`, which replaces a
  field of the same size and type inline. Serving an image never rebuilds
  or copies the message; find-first-field + update is the whole cost.
  Images from typeless feeds (no `MSG_TYPE` anywhere) are served as-is —
  the field is normalized when present, never injected.
- `SEQ_NO` when present: tracked per subject with wrap-aware 16-bit
  arithmetic (production seqno is `unsigned short`). Handling is **feed
  policy, not a fixed design choice** — all of these are valid operating
  modes, selected per feed the way raicache's per-source attributes do it:
  - `seq=observe` (default): apply every update in arrival order; count
    `seq_regress` and `seq_gap` stats. Gaps are the integrity signal
    (missed updates → image suspect until next INITIAL/VERIFY).
  - `seq=strict`: drop non-increasing seqnos (duplicate/replay
    suppression for multi-path feeds).
  - `seq=stamp`: ignore feed seqno for ordering; cache maintains its own
    per-subject update seqno and stamps outgoing merged images with it
    (the `RWF_Cache_Seqno` pattern from production) — downstream sees a
    clean monotonic stream regardless of feed behavior.
  In rv_cache (single feed input) the policy is a CLI flag
  (`-Q observe|strict|stamp`); a multi-source build would hang it off
  per-source config like everything else in this list.

**Shared-memory storage — the raikv KV growth path (milestone 3):** the
private `RouteVec` table above is the first cut. The intended destination
is raikv's shared-memory KV — `HashTab::create_map()` / `attach_map()`
(`raikv/shm_ht.h`, `EvShm` wrapper) — a **shared-memory-across-process
model**: the image store lives in a named segment that several processes
attach and operate on concurrently. That is how raids already works
(`EvShmClient`), which is the point: an rv_cache can run **alongside a
ds_cache (redis cache)** and future protocol frontends, all serving *one*
image store — subject as key, image blob + meta (seqno, msg_type,
timestamps) as value — each process contributing its own protocol view.
The milestone-1 layout was shaped for this on purpose-by-accident:
`CacheEntry` is POD and the image is a raw byte blob with no pointer
graph, so it maps onto a ht value directly. What stays process-private is
the *subscription* table — interest is protocol-specific; only images are
shared. Concurrency comes from the ht's per-entry locking + versioned
reads (merge becomes read-modify-write under the entry lock). CLI sketch:
a `-k <map>` flag attaches a segment (raikv facility naming, e.g.
`sysv:raikv.shm`); absent → private heap table exactly as today.

### 4. Snapshot service

Two entry points, one code path:

- `_SNAP.<subject>` publish arriving with an RV reply → answer directly.
- `RvSubscriptionDB::on_snapshot()` callback (submgr also observes
  snapshot-style listens with `Snap.flags`) → same reply path.

**`_SNAP` QueryFlags — the rv7 initial path:** the `_SNAP.<subject>`
message body may carry a flags field mirroring the SASS3 `T` QueryFlags
(appendix): absent or `SNAPSHOT` → pure image RPC (default, backward
compatible); `SUBSCRIBE`/`INITIAL_VALUES` → deliver the image to a
requester that is subscribing. This is how rv7 clients
subscribe-with-initial, since the rv7 API cannot attach an inbox to a
listen-start the way rv5 can — but the *subscription itself* is not
`_SNAP` state: the client's bare-subject listen reaches sub_db through the
daemon's advisories, and that refcnt is what keeps ticks flowing. There is
no `_SNAP` lease table; `RESUBSCRIBE`/`UNSUBSCRIBE` flags are accepted and
logged for accounting, but interest life belongs to submgr.

*Wire format:* existing clients carry a single uint field literally named
**`flags`** — that stays the baseline (no magic, no envelope). An optional **`A`
accounting submessage** (`U` user, `H` host, `A` app, `P` pid — same shape
as SASS3 SUB's) is accepted and encouraged in new code: an old-school feed
replacement has to track who consumes what for the day the feed vendor
comes to collect — the **DACS** role in the RMDS world. When present, the
accounting identity becomes the holder identity and attributes usage; when
absent, holder identity falls back to the sender's session — which is no
longer anonymous: `on_snapshot` receives the `RvSessionEntry` submgr
resolves from the reply inbox, and `RvSessionEntry::user_id` (captured
from `SESSION.START`'s `userid` field, or the `user` field of a session's
subscriptions-query reply) attributes the request to a user with no
cooperation from the client at all. The reply inbox is the image/ack
delivery address either way.

Reply payload: the cached image, `MSG_TYPE` stamped for the delivery kind
(§3 normalization: `SNAPSHOT` for `_SNAP` replies and SNAPSHOT-flagged
requests, `INITIAL` for listen-start-inbox pushes and INITIAL_VALUES
requests — the receiver can tell a poll answer from a subscribe-image),
plus (in merge mode) rv_cache's own bookkeeping fields appended when `-v`
verbose-images is set (`_cache_seq`, `_cache_time`). The RPC always
completes; consumers distinguish miss from timeout, and the reply is
meaningful to any SASS-aware client, not just rv_cache's own tooling.

**Cold-cache misses — the pending-initial flow.** What a miss reply says
depends on what kind of source stands behind the cache:

- *Broadcast feed (default mode):* there is no one to ask — a miss is a
  miss. Reply **TRANSIENT / `NOT_FOUND` (17) immediately** (raicache's
  `bcast-nack` behavior).
- *Interactive feed (`-S` mode):* the subject may simply not have arrived
  yet — e.g. before the exchange opens. Reply **TRANSIENT /
  `TEMP_UNAVAIL` (7) immediately** to the reply inbox (the inbox is used
  for *status*), register the subject in a pending-initial table, and
  assert `SUBSCRIBE|INITIAL_VALUES` upstream. **When the initial arrives
  later it is broadcast on the subject** — not sent to the requester's
  inbox — so every listener acquires the image at once (consistent with
  the wildcard-initial reasoning: inbox replies carry no subject identity,
  broadcasts do). The pending entry clears on arrival, or on the
  configured pending timeout (`-P secs`, default 10): timeout just drops
  the pending entry and bumps a stat — the requester already has its
  status and interest remains asserted upstream, so a very-late initial
  still broadcasts normally.

### 5. SASS3 downstream service: `Sass3SubscriptionDB` (network 4, `-F name`) — not yet implemented

The mirror image of `-S` mode: rv_cache advertises itself as a SASS3 feed
named `<name>` for legacy clients. Structured as a second interest
database, `sass3_db`, deliberately shaped after sub_db: the same
`process_pub()` dispatch contract (returns true when it consumed a
`_SASS.<name>.SUB` control message, parsed per
`Sass3SubscribeConsumer::onMsg()` semantics — magic 23176, QueryFlags in
`T`, subjects as field names of `S`), the same `RouteVec` subscription
table, the same refcnt-gated forwarding check, the same listener-callback
surface feeding accounting. The differences are all consequences of SASS3
having no advisory channel:

- **QueryFlags manage the refcnt.** `SUBSCRIBE` refs the sender's holder
  on each subject listed in `S`; `UNSUBSCRIBE` derefs; `RESUBSCRIBE`
  renews the holder's lease without changing the count.
- **The hold timer is internalized here.** Every holder ref carries a
  lease stamped from the `-D` clock; a lease not renewed by RESUBSCRIBE
  within the window expires and derefs. This is the ONLY decay timer in
  rv_cache — the RV side needs none, because advisories provide explicit
  STOP. Nothing outside sass3_db sees or manages leases.
- SASS3 holders are identified by the `A` accounting submessage
  (user/host/app/pid); the `H` host attribution also enables
  host-correlated early expiry (see the ghost-window rule in the topology
  section) — the hold timer is the backstop, not the common case.
- `IMAGE_FLAGS` (SNAPSHOT/INITIAL_VALUES) → cached images to the reply
  inbox, `MSG_TYPE` stamped to match the flag (`SNAPSHOT` vs `INITIAL`,
  §3/§4); miss → TRANSIENT/NOT_FOUND, same one-code-path as `_SNAP`.
- Ack to reply inbox when RESUBSCRIBE without IMAGE_FLAGS (matching
  `sendAck`).
- **Data delivery is plain RV:** subscribers listen on the bare
  `<subject>`; rv_cache publishes accepted ticks there exactly as it does
  for native consumers. No PUB envelope is emitted — that envelope is the
  *feed-side* broadcast format (a role rv_cache would only take on when
  cascading cache-to-cache, out of scope for the first cut).

### 6. Stats

1s timer prints (or `-q` silences): subjects cached, images bytes,
ticks in/forwarded/dropped-no-listener, snaps served/missed, interest
opens/closes, submgr GC counters (`db.subscriptions.active/removed`, hosts,
sessions). Final totals on SIGINT exit, like the other sassrv tests.

**Usage accounting (DACS-shaped):** per holder identity (accounting
submessage when supplied, else session — whose `user_id` submgr captures
from `SESSION.START`/`userid` and subscription-query replies), track subjects held, images
served, subscription open/close times. This is the audit surface a feed
vendor's billing collection expects; in-memory counters with a
dump-on-exit / on-demand print, plus the structured log below — the point
is that the identity plumbing exists from day one, not that rv_cache does
billing.

**Structured accounting log (`-A file`, JSONL):** one JSON object per
subscription lifecycle event, written line-per-event (`-A -` for stdout).
This is the machine-readable DACS feed — everything the billing/entitlement
auditor needs to reconstruct who held what, when, and how they asked.

| Field | Type | Meaning |
|-------|------|---------|
| `ts` | string | ISO-8601 UTC, microseconds |
| `event` | string | `subscribe` \| `resubscribe` \| `unsubscribe` \| `expire` \| `sweep` \| `initial` \| `snapshot` \| `host_stop` |
| `subject` | string | the subject (omitted on `host_stop`) |
| `user` | string? | accounting `U`, else the session's `RvSessionEntry::user_id`, else null |
| `host` | string? | accounting `H`, else IPv4 decoded from host id |
| `host_id` | string | reference host id, IPv4 hex (the correlator) |
| `app` | string? | accounting `A`, else null |
| `pid` | uint? | accounting `P`, else null |
| `session` | string? | rv session string when known |
| `protocol` | string | `rv5` \| `rv7` \| `sass3` \| `snap` — which interest dialect the holder used |
| `query_flags` | uint | raw QueryFlags value as received (0 for advisory holders) |
| `flags` | [string] | decoded flag names (`SUBSCRIBE`, `INITIAL_VALUES`, …) |
| `reason` | string? | close-type events: `unsubscribe` \| `hold_timer` \| `listen_stop` \| `host_stop` |
| `open_secs` | number? | close-type events: subscription duration |
| `msgs` | uint? | close-type events: ticks published on the subject while held (attribution, not per-holder delivery) |
| `images` | uint? | close-type events: initials/snapshots served to this holder |

Examples:

```json
{"ts":"2026-07-11T22:57:03.123456Z","event":"subscribe","subject":"TEST.FOO","user":"chris","host":"dyna","host_id":"0a040412","app":"riskmon","pid":4411,"protocol":"sass3","query_flags":6,"flags":["SUBSCRIBE","INITIAL_VALUES"]}
{"ts":"2026-07-11T23:05:03.223311Z","event":"expire","subject":"TEST.FOO","user":"chris","host":"dyna","host_id":"0a040412","app":"riskmon","pid":4411,"protocol":"sass3","query_flags":0,"flags":[],"reason":"listen_stop","open_secs":480.1,"msgs":1912,"images":1}
{"ts":"2026-07-11T23:06:11.000042Z","event":"subscribe","subject":"TEST.BAR","user":null,"host":"10.4.4.16","host_id":"0a040410","app":null,"pid":null,"session":"0A040410.DAEMON.9F3A","protocol":"rv7","query_flags":0,"flags":[]}
```

Emission rules: `subscribe`/`unsubscribe`/`expire`/`sweep` always;
`initial`/`snapshot` per image served; `resubscribe` at most once per hold
window per (holder, subject) — volume scales with churn and window count,
never with tick rate. Advisory holders log `protocol` `rv5`/`rv7`,
discriminated by session shape: `.DAEMON.` in the session/inbox ⇒ rv7,
absent ⇒ rv5; `query_flags` 0. Rotation/shipping is out of scope — JSONL is
chosen exactly so jq/logrotate/collectors handle it downstream.

## CLI

```
rv_cache [-d daemon] [-n network] [-s service]   defaults for all 4 networks
         [-1 d,n,s]        rv feed override      (data in, _TIC.>)
         [-2 d,n,s]        rv sub override       (data out, _SNAP, submgr)
         [-3 d,n,s]        sass3 feed override   (-S upstream client)
         [-4 d,n,s]        sass3 sub override    (-F downstream service)
         [-w wildcard]     subject filter for interest tracking (repeatable)
         [-S feed]         enable SASS3 legacy-compat mode: resubscribe to
                           _SASS.<feed>.SUB, consume _SASS.<feed>.PUB envelopes
                           (default: off -- rvd-native, no upstream protocol)
         [-F name]         serve downstream SASS3 subscription maint on
                           _SASS.<name>.SUB (data flows on bare subjects;
                           default: off; <name> must differ from -S <feed> when
                           the sass3 attachments share network parameters)
         [-D secs]         SASS3 hold timer, sass3 nets only (internal to
                           sass3_db / the -S reassert cycle): upstream
                           reassert window + downstream lease decay (default
                           480 = 8 min, the customary SASS3 hold time;
                           reasserts spread across the window). No effect
                           without -S/-F: RV-side subscription life is
                           submgr's, advisory-driven, timerless
         [-m]              merge mode default for typeless ticks
         [-Q mode]         seqno policy: observe | strict | stamp (default observe)
         [-M]              route-after-merge: forward merged image, not raw tick
         [-x secs]         stale entry expiry (0 = never, default 0)
         [-P secs]         -S mode: pending-initial timeout (default 10)
         [-A file]         structured accounting log, JSONL (- for stdout;
                           default off)
         [-q]              quiet stats
         [-v]              verbose (submgr mout debug log to stdout)
```

## Repo layout

Mirror `rvcount` (standalone injinj project):

```
rv_cache/
  GNUmakefile          # link order: -lsassrv -lraimd -lraikv (+ static push/pop
                       # state per rvcount's lnk_lib), includes from
                       # $(sassrv_home)/include etc.
  src/rv_cache.cpp     # main: poll loop, EvRvClient wiring, dispatch
  src/cache_tab.cpp    # CacheEntry table + merge logic
  include/rvcache/cache.h
  test/                # scripted integration tests (below)
  rpm/rv_cache.spec    # BuildRequires: raikv-devel, raimd-devel, sassrv-devel
  deb/                 # control mirrors rpm deps
  .copr/Makefile       # same pattern as other repos (with the _ensure_git
                       # one-liner from the 2026-07-09 COPR fix)
```

## Test plan

Integration harness (shell script in `test/`), using sassrv's own rvd-compatible
server so no TIB install is needed:

1. Start sassrv rv server (or real rvd) on a private service/network.
2. Start `rv_cache -m`.
3. **Interest, default mode:** run a second `RvSubscriptionDB` instance as
   the "feed's view" (this is the recommended topology, so test it as
   such); start a consumer on `TEST.FOO`; assert the feed-side submgr sees
   the listen-start. Stop consumer; assert listen-stop after the advisory
   propagates.
3b. **Interest, `-S` mode:** listen on `_SASS.TIC.SUB`; assert the SUB
   message has magic 23176, `SUBSCRIBE|INITIAL_VALUES` in `T`, and
   `TEST.FOO` as a field name in `S`. Past decay/2, assert a RESUBSCRIBE
   batch arrives and ack it; stop the consumer, assert UNSUBSCRIBE.
4. **Forward-only-when-listened:** publish `_TIC.TEST.BAR` with no
   listener; assert consumer-side silence + `dropped_no_listener` stat.
   Add listener, publish again, assert delivery on `TEST.BAR`.
5. **Cache + snapshot:** publish INITIAL for `TEST.FOO` (3 fields), then
   UPDATE (1 field changed). `_SNAP.TEST.FOO` RPC → reply must contain the
   merged 3-field image with the updated value, `MSG_TYPE = SNAPSHOT` as
   the first field.
5b. **MSG_TYPE lifecycle:** (a) VERIFY on an uncached subject → image
   created, snapshot serves it; (b) VERIFY on a cached subject with one
   field differing → merged like an update, other fields retained;
   (c) CLOSING with extra closing-only fields → merged, entry still live,
   snapshot includes both regular and closing fields; (d) DROP with a live listener → listener
   receives the DROP (instrument-death notification), entry evicted,
   subsequent `_SNAP` gets the TRANSIENT/NOT_FOUND reply, `evicted` stat
   increments.
6. **Initial image on listen (rv5 path):** a new rv5-style listener
   (`rv5_api_test`) attaches a reply inbox to its listen-start on
   `TEST.FOO` and receives the image (`MSG_TYPE = INITIAL`) without
   publishing `_SNAP` — no cache-side option involved, the inbox on the
   START is the request. An rv7-style listener (`subrv7test`) on the same
   subject demonstrates the API gap: no inbox on its START, no pushed
   initial — it must pull via `_SNAP` (test 14).
6b. **Miss on the listen-start initial path:** an rv5-style listener
   attaches a reply inbox to its listen-start on cold `NO.SUCH` → the
   inbox receives MSG_TYPE=TRANSIENT / REC_STATUS=NOT_FOUND (broadcast
   mode; same one-code-path as the `_SNAP` miss, test 7). Interest stays
   registered (refcnt 1) and a later INITIAL warms the cache and
   broadcasts normally. **SASS3 variant:** sass3 interest with
   `S3_SUBSCRIBE|S3_INITIAL_VALUES` for a cold subject → the same
   TRANSIENT/NOT_FOUND on the sass3 reply inbox. *(Added 2026-07-17: the
   then-current version sent the status on the snap/initial RPC path but
   not on the listen-start initial path.)*
7. **Miss path (broadcast mode):** `_SNAP.NO.SUCH` → reply is
   MSG_TYPE=TRANSIENT with REC_STATUS=NOT_FOUND, `snap_missed` stat
   increments.
7c. **Miss path (interactive mode, pending initial):** in `-S` mode,
   request a cold subject → reply inbox gets TRANSIENT/TEMP_UNAVAIL
   immediately and SUBSCRIBE|INITIAL_VALUES goes upstream; deliver the
   initial from the feed side → it arrives as a **broadcast on the
   subject** (not to the inbox), cache is warm, next `_SNAP` serves it.
   Timeout variant: no upstream initial within `-P` → pending entry
   dropped, stat bumped; a later initial still broadcasts and warms the
   cache.
7b. **TRANSIENT pass-through:** publish `_TIC.TEST.FOO` TRANSIENT
   (NOT_FOUND) while `TEST.FOO` is cached and has a listener → listener
   receives it, cached image is UNCHANGED (subsequent snapshot returns the
   pre-TRANSIENT image), no eviction.
8. **Wildcard modes:** consumer listens `TEST.>` while a concrete
   listener exists on `TEST.FOO` and ticks flow for `TEST.FOO` and
   `TEST.BAR`. With `can-wildcard=false`: wildcard listener sees `TEST.FOO`
   traffic only (the already-flowing set), `TEST.BAR` is not forwarded,
   and no interest for `TEST.BAR` appears upstream. With
   `can-wildcard=true`: both forwarded, wildcard interest visible upstream
   in `-S` mode.
9. **NOSUBSCRIBERS on last unsubscribe:** consumer A and B listen
   `TEST.FOO`; stop A → no DROP; stop B → DROP with
   REC_STATUS=NOSUBSCRIBERS published on `TEST.FOO`, forwarding stops
   (`_TIC.TEST.FOO` publishes no longer appear on `TEST.FOO`).
   Desync-recovery variant (later, needs advisory suppression): a client
   still listening when NOSUBSCRIBERS arrives reasserts → fresh
   LISTEN.START → forwarding resumes; passive variant → submgr host-status
   re-query rediscovers the listen within the query interval.
10. **Churn/GC soak:** loop 1k subjects × start/stop with pub interleave;
   submgr GC counters stay sane, RSS flat (leak check), no stuck refcnts.
11. **SASS3 downstream service (`-F`):** legacy-style client sends
   `_SASS.<name>.SUB` with SUBSCRIBE|INITIAL_VALUES for `TEST.FOO` and
   listens on bare `TEST.FOO` → image arrives on its reply inbox,
   subsequent `_TIC.TEST.FOO` ticks arrive as plain `TEST.FOO` publishes
   (same messages a native RV listener would see — assert both client
   types receive identical payloads simultaneously: the coexistence
   property). Let the lease lapse without RESUBSCRIBE → interest decays,
   forwarding stops. Mixed interest: an RV listener and a SASS3 subscriber
   on the same subject — dropping one keeps forwarding alive for the
   other; refcnt hits 0 only when both are gone.
12. **Collapsed networks:** run all four attachments with identical
   `d,n,s` on one service (this is also how tests 1–11 run by default) —
   assert no self-loop: rv_cache never consumes its own forwards or
   envelopes; startup rejects `-S X -F X` when nets 3/4 share parameters.
13. **Session-model matrix:** same-subject interest from an rv5-style
   client (`rv5_api_test`/`rv_client`), an rv7-style client (`subrv7test`),
   and a SASS3 subscriber — assert one live subscription per session
   identity (rv5 session, daemon session, SASS3 identity) and forwarding
   until ALL are gone. Then two rv7 listeners on the SAME host
   (`fanrv7test`): the second join fires no new advisory (its initial must
   come via `_SNAP`), and the first exit fires no STOP — the subject stays
   live on the host's single daemon-session ref until the last local
   listener exits.
14. **Flagged `_SNAP` (rv7 initial path):** rv7 client listens on
   `TEST.FOO` (daemon advisory refs the subject in sub_db) and sends
   `_SNAP.TEST.FOO` with SUBSCRIBE|INITIAL_VALUES → image on the reply
   inbox, subsequent ticks arrive via the listen. Dropping the listen
   (advisory STOP → refcnt 0) stops forwarding — assert no `_SNAP` lease
   keeps it alive. The snapshot accounting event carries the session's
   `user_id`. Plain unflagged `_SNAP` still behaves exactly as test 5.
15. **Ghost-window bound via host correlation:** a SASS3 subscriber
   holds `TEST.FOO` with a fresh lease plus its
   bare-subject data listen; kill the client with no UNSUBSCRIBE. The
   daemon's LISTEN.STOP (host's last sub on the subject) must expire the
   lease immediately — forwarding stops within advisory latency, nowhere
   near the 480s backstop. Variant: two SASS3 clients on one host —
   killing one leaves forwarding alive (no STOP yet); killing both fires
   the STOP and sweeps both leases at once.
16. **Accounting log:** run tests 11/14/15 with `-A` — assert one
   `subscribe` per holder with correct `{user, host, app, pid, protocol,
   query_flags}`, `initial`/`snapshot` events for images, and close events
   carrying the right `reason` (`unsubscribe` vs `hold_timer` vs
   `listen_stop`), sane `open_secs`, and `msgs`/`images` totals; `jq` over
   the file reconstructs the full holder timeline.

Traffic generators: the golang `pubrv7test`/`subrv7test` from the May
session already speak the `_TIC.<subj>` convention — reuse them for 3/4;
add a tiny `snaprv7test` (RPC + wait reply) or use `rv7_test` C API bits.

## Appendix: SASS3 wire format (from `~/rai/RaiCore/src/cache/sass3_svc.cpp`)

RaiMsg fields, single-char names. Authoritative parse sites:
`Sass3SubscribeConsumer::onMsg()` (≈line 1805) and `Sass3Svc::doFeed()`
(≈line 2151).

**`_SASS.<feed>.SUB`** — subscription message (magic `23176`,
`SassConst::SASS3_SUB_MAGIC`):

| Field | Type | Meaning |
|-------|------|---------|
| `M`   | u16  | magic, 23176; message ignored without it |
| `T`   | uint | QueryFlags (below); default assumed RESUBSCRIBE |
| `A`   | msg  | accounting: `U` user, `H` host, `A` app, `P` pid → userId |
| `S`   | msg  | **subject list: the field NAMES are the subjects** (values unused) |

RV reply inbox on the message = ack / image delivery address. Ack is sent
when RESUBSCRIBE is set and IMAGE_FLAGS are not.

QueryFlags (`cache_if.h`): `SNAPSHOT 0x01`, `SUBSCRIBE 0x02`,
`INITIAL_VALUES 0x04`, `UNSUBSCRIBE 0x08`, `REFRESH 0x10`,
`RESUBSCRIBE 0x80`; `IMAGE_FLAGS = SNAPSHOT|INITIAL_VALUES`;
`USER_FLAGS_MASK 0xff` (higher bits internal: `PERMANENT 0x100`,
`NO_CACHE 0x400`, `DROP 0x1000`, …).

**`_SASS.<feed>.PUB`** — feed broadcast envelope (magic `23177`,
`SASS3_PUB_MAGIC`):

| Field | Type | Meaning |
|-------|------|---------|
| `M`   | u16  | magic, 23177 |
| `T`   | uint | SASS msg type (default UPDATE) |
| `S`   | uint | rec status (default STATUS_OK) |
| `I`   | uint | s3 indicator |
| `D`   | msg  | data payload submessage |
| `E`   | uint | expires |
| `A`/`G` | —  | acct/group (ignored by doFeed) |

rv_cache `-S` mode emits SUB messages byte-compatible with `onMsg()` and
parses PUB envelopes per `doFeed()`; default `_TIC.>` mode carries bare
RVMSG payloads with SASS `MSG_TYPE`/`SEQ_NO` fields inline instead.

## Production reference: raicache `source-sass3`

`~/injinj/cache.dtd` (≈ lines 1472–1500) documents the full production
policy surface this test is a subset of. Options adopted here (same
defaults where sensible): `bcast-nack` (yes → the miss/status reply),
`route-after-merge` (`-M` flag), `subsc-decay-time` (`-D`),
`can-wildcard` (yes). Options explicitly out of scope for the test:
conflation + bandwidth/message-rate caps, entitlement *enforcement*
(usage *accounting* per identity IS in scope — §4/§6), load-balance,
services infrastructure (`_SASS.SERVICES.LIST` dictionary requests), DQA
heartbeats (`_SASS.DQA.TIC.HB`). Listed so the test can grow toward the
real thing without renaming anything.

## Open questions — resolved

1. **Forward lookup without entry creation** *(re-resolved — submgr owns
   the table)*: the concern was the tick path consulting submgr's
   `sub_tab` via `RvSubscriptionDB::snapshot()`, which find-or-creates —
   the full feed firehose would mint entries for every never-listened
   subject and bloat the table to the feed universe. An earlier draft
   answered this with an rv_cache-owned "unified interest table" merging
   advisory + flagged-`_SNAP` + SASS3 holders under keepalive timers; that
   design is dropped. Resolution: the hot path does a **read-only
   `sub_tab.find()`** against sub_db and gates on
   `RvSubscription::refcnt != 0` — no entry creation, no parallel table;
   submgr's advisory lifecycle (STOP derefs, session/host sweeps, GC) IS
   the subscription's life. SASS3 interest never touches sub_db: it lives
   in the mirror-shaped sass3_db with its leased refcnt and internalized
   hold timer. submgr already grew what this needs:
   `process_pub()` returns consumed/not-consumed, `RvSessionEntry` carries
   `user_id`, and `_SNAP` resolves the requester's session for the
   `on_snapshot` callback.
2. **RESUBSCRIBE batching** *(resolved — match existing client behavior)*:
   SASS3 clients batch their interest and divide it across time; with the
   customary 8-minute hold timer, the live set is split into batches
   spread evenly over the window (each subject refreshed once per window).
   At that cadence no single SUB message needs to be anywhere near
   unbounded — batch size falls out of `live_subjects / batches_per_window`
   with a sane per-message cap, and the load profile matches what legacy
   feeds already tolerate from real clients.
```
