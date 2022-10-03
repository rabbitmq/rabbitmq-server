#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

for f in $SCRIPT/suites/*.sh
do
  SUITE=$(basename -- "$f")
	echo "Running suite $SUITE ..."
  $f
done
