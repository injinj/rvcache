#!/bin/bash
# deps.sh — clone (and optionally build) this repo's sibling dependencies.
#
# The dependency list comes from build_depends.mak (the same file the srpm
# target substitutes into rpm BuildRequires), so there is exactly one place
# that names the deps.  Repos are cloned as SIBLINGS of this checkout
# (../raikv, ../raimd, ...), which is the layout the GNUmakefile's
# -I../<dep>/include and ../<dep>/FC43_*/lib64 link paths expect.
#
#   ./deps.sh          clone missing siblings
#   ./deps.sh -b       clone missing siblings, then build all in dep order
#   ./deps.sh -n       dry run (show what would happen)
#
# Versions in build_depends.mak are rpm BuildRequires minimums; master of
# each dep normally satisfies them.  To pin exactly, check out matching
# tags in the sibling checkouts after cloning.
#
# Prefer packages?  The whole chain publishes on COPR:
#   dnf copr enable injinj/rel && dnf install rvcache
set -e

ORG="${ORG:-https://github.com/raitechnology}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PARENT="$(dirname "$HERE")"
DEPFILE="$HERE/build_depends.mak"

# global topological order (leaves first); filtered by what DEPFILE names
TOPO="libdecnumber raikv raimd h3 linecook rdbparser hdrhist openpgm
      sassrv natsmd raids omm raims"

build=0; dry=0
while getopts "bnh" o; do case "$o" in
  b) build=*** ;;
  n) dry=*** ;;
  *) sed -n '2,20p' "$0"; exit 0 ;;
esac; done

[ -f "$DEPFILE" ] || { echo "missing $DEPFILE" >&2; exit 1; }

# "raikv_dep := 1.42" -> "raikv"
deps="$(sed -n 's/^\([a-z0-9_]*\)_dep[[:space:]]*:=.*/\1/p' "$DEPFILE")"
[ -n "$deps" ] || { echo "no *_dep entries in $DEPFILE" >&2; exit 1; }

want() { echo "$deps" | grep -qx "$1"; }

ordered=""
for r in $TOPO; do want "$r" && ordered="$ordered $r"; done
# anything in DEPFILE not in TOPO goes last (new dep: extend TOPO)
for r in $deps; do
  case " $ordered " in *" $r "*) ;; *) ordered="$ordered $r"
    echo "warn: $r not in TOPO order list, building last" >&2 ;;
  esac
done

echo "deps (build order):$ordered"

for r in $ordered; do
  if [ ! -d "$PARENT/$r/.git" ]; then
    echo "clone: $ORG/$r -> $PARENT/$r"
    [ "$dry" = 1 ] || git clone "$ORG/$r" "$PARENT/$r"
  else
    echo "have:  $PARENT/$r"
  fi
done

if [ "$build" = 1 ]; then
  for r in $ordered; do
    echo "make:  $PARENT/$r"
    [ "$dry" = 1 ] || make -C "$PARENT/$r" -j"$(nproc)"
  done
  echo "make:  $HERE"
  [ "$dry" = 1 ] || make -C "$HERE" -j"$(nproc)"
fi
