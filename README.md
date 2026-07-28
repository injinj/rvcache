# rvcache

RV subject cache with interest-driven tick forwarding.

rvcache sits between a feed side (publishers sending ticks on
`_TIC.<subject>`) and a consumer side (native RV listeners, rv7 clients,
SASS3 subscribers). It maintains a last-image cache per subject, answers
snapshot requests, and forwards ticks only for subjects that currently
have live interest. Interest is tracked from the rvd's own advisories
(`_RV.INFO.SYSTEM.LISTEN.START/STOP`) with sassrv's subscription manager,
so plain RV clients need no extra protocol, and interest is forwarded
upstream so the feed side can start/stop publishing per subject.

Built on [sassrv](https://github.com/raitechnology/sassrv) (RV client +
submgr interest tracking), [raimd](https://github.com/raitechnology/raimd)
(message codecs, SASS constants), and
[raikv](https://github.com/raitechnology/raikv) (hash tables, event poll).

See [SPEC.md](SPEC.md) for the design document and
[IMPLEMENTATION_NOTES.md](IMPLEMENTATION_NOTES.md) for milestone status.

## Features

- **Last-image cache** — every tick seen on the feed side is merged into a
  per-subject cached image, with MSG_TYPE normalization (INITIAL on first
  image, UPDATE afterward) stamped into the image.
- **Interest-gated forwarding** — ticks are re-published on the consumer
  side only while the subject has at least one live listener; interest
  comes from rvd advisories via `RvSubscriptionDB`, including host-status
  keepalive sweeps and session GC.
- **Snapshot service** — point-to-point snapshot replies served from the
  cache (`_SNAP.<subject>` request/reply), so late joiners get an image
  without waiting for the next tick.
- **Arbitrary network attachments** — any number of feed and sub nets
  (1..64), each an independent RV session with its own
  `(daemon, network, service)` and optional subject wildcard; they may
  be collapsed onto one rvd as a deployment choice. Declared on the
  command line (`-<idx> role proto ...`) or in a json/yaml config file
  (`-c`), which also carries the less-common options as long-name keys.
- **Sequence handling** — observe, strict, or stamp modes for SASS
  sequence numbers (`-Q`).
- **Accounting** — per-subject open/close/image events as JSON lines
  (`-A file`, `-` for stdout).
- **Shared-memory / dictionary hooks** — `-m map_name` opens a raikv shm
  segment for the image store (milestone 3 wires it), `-p path` loads a
  SASS dictionary (default `$cfile_path`).

## Build

Sibling checkouts of
[raikv](https://github.com/raitechnology/raikv),
[raimd](https://github.com/raitechnology/raimd),
[libdecnumber](https://github.com/raitechnology/libdecnumber), and
[sassrv](https://github.com/raitechnology/sassrv) are expected next to
this directory (the same layout the GNUmakefile probes with
`test_makefile`):

```console
$ ls ..
libdecnumber  raikv  raimd  rvcache  sassrv

$ make
```

Binaries land in the platform build directory (e.g. `FC43_x86_64/bin`).
A debug build is available with `make port_extra=-g`.

## Usage

```console
$ rvcache -h
rvcache [-d daemon] [-n network] [-s service] (defaults)
  [-<idx> role proto[ daemon[ network[ service[ wildcard]]]]] net
  [-p path]             = dictionary search path
  [-c file]             = json/yaml config (.yaml/.yml = yaml)
  [-m map_name]         = shm name to cache msgs
  [-r]                  = replace typeless msgs
  [-Q obs|strict|stamp] = message sequence policy
  [-M]                  = route-after-merge
  [-x secs]             = eviction expiry
  [-P secs]             = pending timeout (10)
  [-A file]             = accounting jsonl (- stdout)
  [-q]                  = quiet stats
  [-v]                  = verbose submgr log
```

- `-d`, `-n`, `-s` set the default daemon / network / service for all
  attachments.
- `-<idx>` declares a net attachment (idx 1..64): role `feed|sub`, proto
  `sass2|sass3|both`, then optional daemon / network / service /
  wildcard. Fields are **argv-separated** (network configs contain
  commas) and run to the next `-flag`; pass `''` to skip a middle field
  and keep the `-d/-n/-s` default. The optional wildcard scopes the
  attachment: on a sub net it filters interest tracking, on a feed net
  it narrows the upstream subscription (`_TIC.<wild>.>` /
  `_SASS.<wild>.PUB`). Default topology when no nets are declared:
  `-1 feed sass2 -2 sub both`.
- Only the important knobs are CLI flags. The `-c` json/yaml config
  file carries everything as long-name top-level keys — `daemon`,
  `network`, `service`, `nets` (array of `{index, role, proto, daemon,
  network, service, wildcard}`), `map_name`, `dict_path`,
  `replace_typeless_msgs`, `sequence_policy`, `route_after_merge`,
  `message_eviction_secs`, `pending_initial_secs`, `accounting_file`,
  `quiet`, `verbose` — see [cache.yaml](cache.yaml); explicit CLI flags
  override file values.
- Typeless ticks (no `MSG_TYPE` field) **merge by default**; `-r`
  switches to replace mode.
- `-Q` selects sequence-number handling: `obs` (observe, default),
  `strict` (drop out-of-order), or `stamp` (rewrite).
- `-x` evicts cached images not refreshed within the given seconds;
  `-P` bounds how long a snapshot request stays pending.

### Example

Cache between a feed rvd and a consumer rvd, forwarding only `RSF.>`
subjects, with accounting to a file:

```console
$ rvcache -1 sub sass2 tcp:7500 'eth0;227.5.0.0' 7500 'RSF.>' \
          -2 feed sass2 tcp:7600 'eth1;227.6.0.0' 7600 'RSF.>' \
          -c cache.yaml -m sysv:raikv.shm -A subscript.log
```

Consumers subscribe plain subjects on net 2; publishers send
`_TIC.<subject>` on net 1. A consumer subscribing `TEST.FOO` gets a
snapshot from the cache (or a pending reply until the first tick), then
live updates for as long as its subscription is open.

## Test

```console
$ test/basic.sh
```

Uses sassrv's rvd-compatible `rv_server`, so no TIB install is needed.

## License

[Apache-2.0](LICENSE)
