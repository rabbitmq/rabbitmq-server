#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

OVERALL_TEST_RESULT=0
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

SUCCESSFUL_SUITES=()
FAILED_SUITES=()

for f in $(find $SCRIPT/suites/ -name "${1:-*.sh}");
do
  SUITE=$(basename -- "$f")
  echo "=== Running suite $SUITE ============================================"
  echo " "
  ENV_MODES="docker" $f
  TEST_RESULT="$?"
  TEST_STATUS="${GREEN}Succeeded${NC}"
  if [ "$TEST_RESULT" -ne 0 ]
  then
    OVERALL_TEST_RESULT=$TEST_RESULT
    TEST_STATUS=" ${RED}Failed${NC}"
    FAILED_SUITES+=($SUITE)
  else
    SUCCESSFUL_SUITES+=($SUITE)
  fi
  echo -e "=== $TEST_STATUS $SUITE ==========================================="
  echo " "
done

echo "=== Summary ============================================"
if [ ${#SUCCESSFUL_SUITES[@]} -gt 0 ]; then echo -e " > ${GREEN}Successful suites ${NC}"; fi
for f in ${SUCCESSFUL_SUITES[@]}
do
  echo "   - $f"
done

if [ ${#FAILED_SUITES[@]} -gt 0 ]; then echo -e " > ${RED}Failed suites ${NC}"; fi
for f in ${FAILED_SUITES[@]}
do
  echo "   - $f"
done

exit $OVERALL_TEST_RESULT
