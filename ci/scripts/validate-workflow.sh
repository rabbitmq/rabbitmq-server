#!/bin/bash

set -euo pipefail

cd deps/${project}

trap 'catch $?' EXIT

catch() {
    rm expected_suites.txt actual_suites.txt
}

touch expected_suites.txt
for arg in "$@"; do
  echo "test/${arg}_SUITE.erl" >> expected_suites.txt
done
sort -o expected_suites.txt expected_suites.txt

touch actual_suites.txt
for f in test/*_SUITE.erl; do
  echo "$f" >> actual_suites.txt
done
sort -o actual_suites.txt actual_suites.txt

set -x
diff actual_suites.txt expected_suites.txt
