# rvcache

RV subject cache with interest-driven tick forwarding.

rv_cache sits between a feed side (publishers sending ticks on
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
- **Separate feed / sub networks** — the feed attachment and the consumer
  attachment are independent RV sessions, each with its own
  `(daemon, network, service)`; they may be collapsed onto one rvd as a
  deployment choice.
- **Sequence handling** — observe, strict, or stamp modes for SASS
  sequence numbers (`-Q`).
- **Accounting** — per-subject open/close/image events as JSON lines
  (`-A file`, `-` for stdout).

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
libdecnumber  raikv  raimd  rv_cache  sassrv

$ make
```

Binaries land in the platform build directory (e.g. `FC43_x86_64/bin`).
A debug build is available with `make port_extra=-g`.

## Usage

```console
$ rv_cache -h
rv_cache [-d daemon] [-n network] [-s service] (defaults for 4 nets)
  [-1 d,n,s] rv feed   [-2 d,n,s] rv sub
  [-3 d,n,s] sass3 feed [-4 d,n,s] sass3 sub
  [-w wild] interest filter (repeatable)
  [-S feed] SASS3 upstream (net 3)   [-F name] SASS3 downstream (net 4)
  [-D secs] sass3 hold timer (480; no effect without -S/-F)
  [-m] merge typeless   [-Q obs|strict|stamp]
  [-M] route-after-merge   [-x secs] stale expiry
  [-P secs] pending timeout (10)   [-A file] accounting jsonl (- stdout)
  [-q] quiet stats   [-v] verbose submgr log
```

- `-d`, `-n`, `-s` set the default daemon / network / service for all
  attachments (defaults `tcp:7500`, `""`, `7500`).
- `-1` / `-2` (and `-3` / `-4` for the SASS3 attachments) override the
  triple per network as `daemon,network,service`; any field may be left
  empty to keep the default.
- `-w` restricts interest tracking and forwarding to subjects under a
  wildcard; repeat for multiple filters.
- `-Q` selects sequence-number handling: `obs` (observe, default),
  `strict` (drop out-of-order), or `stamp` (rewrite).
- `-x` expires cached images not refreshed within the given seconds;
  `-P` bounds how long a snapshot request stays pending.

### Example

Cache between a feed rvd and a consumer rvd, forwarding only `TEST.>`
subjects, with accounting to stdout:

```console
$ rv_cache -1 tcp:7500,,7500 -2 tcp:7600,,7600 -w 'TEST.>' -A -
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
