#!/bin/sh

set -ex

SCRIPT=$0
SCRIPT_DIR=$(cd $(dirname "$SCRIPT") && pwd)
SRC_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
DEPS_DIR=$(cd "$SRC_DIR/.." && pwd)

case $(uname -s) in
FreeBSD) MAKE=gmake ;;
*)       MAKE=make ;;
esac

(
  cd "$SRC_DIR"
  $MAKE dep_ranch="cp /ranch" DEPS_DIR="$DEPS_DIR" tests
)
