#!/usr/bin/env bash
set -euo pipefail

# /usr/local/bin/tree $PWD
# /usr/local/bin/tree $TEST_SRCDIR/$TEST_WORKSPACE

if [ $1 = "-C" ]; then
    shift 2
fi

CMD=$1
shift

cd $TEST_SRCDIR/$TEST_WORKSPACE

exec env "$@" $CMD*
