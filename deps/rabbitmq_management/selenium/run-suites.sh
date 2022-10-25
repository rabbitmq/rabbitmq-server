#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

OVERALL_TEST_RESULT=0
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

for f in $SCRIPT/suites/*.sh
do
  SUITE=$(basename -- "$f")
  echo "=== Running suite $SUITE ============================================"
  echo " "
  $f
  TEST_RESULT="$?"
  TEST_STATUS="${GREEN}Succeeded${NC}"
  if [ "$TEST_RESULT" -ne 0 ]
  then
    OVERALL_TEST_RESULT=$TEST_RESULT
    TEST_STATUS=" ${RED}Failed${NC}"
  fi
  echo -e "=== $TEST_STATUS $SUITE ==========================================="
  echo " "
done
exit $OVERALL_TEST_RESULT
