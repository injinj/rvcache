# rv_cache

RV subject cache with interest-driven tick forwarding. Sits between a
feed side (publishers on `_TIC.<subject>`) and consumers (native RV
listeners, rv7 clients, SASS3 subscribers), caching images and forwarding
ticks only for subjects with live interest.

Built on [sassrv](https://github.com/injinj/sassrv) (RV client + submgr
interest tracking), [raimd](https://github.com/injinj/raimd) (message
codecs, SASS constants), and [raikv](https://github.com/injinj/raikv).

See [SPEC.md](SPEC.md) for the design document and
[IMPLEMENTATION_NOTES.md](IMPLEMENTATION_NOTES.md) for milestone status.

## Build

Sibling checkouts of raikv, raimd, libdecnumber, and sassrv are expected
next to this directory (same layout the GNUmakefile probes with
`test_makefile`):

```
make
```

## Test

```
test/basic.sh
```

Uses sassrv's rvd-compatible `rv_server`, so no TIB install is needed.
