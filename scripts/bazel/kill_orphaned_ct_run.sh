#!/usr/bin/env bash
set -euo pipefail

pids=$(ps aux | grep -v awk | awk '/ct_run.*erl/ {print $2}')

set -x
kill $pids
