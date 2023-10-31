#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SUITE_FILE=${1:-"full-suite-management-ui"}
OVERALL_TEST_RESULT=0
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

SUCCESSFUL_SUITES=()
FAILED_SUITES=()

TOTAL_SUITES=`wc -l $SCRIPT/$SUITE_FILE | awk '{print $1}'`

while read SUITE
do
  echo -e "=== Running suite (${TOTAL_SUITES}/${GREEN}${#SUCCESSFUL_SUITES[@]}/${RED}${#FAILED_SUITES[@]}${NC}) $SUITE ============================================"
  echo " "
  ENV_MODES="docker" $SCRIPT/suites/$SUITE
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
done <<< "$(cat $SCRIPT/$SUITE_FILE | sort)"

echo -e "=== Summary (${TOTAL_SUITES}/${GREEN}${#SUCCESSFUL_SUITES[@]}/${RED}${#FAILED_SUITES[@]}${NC}) ============================================"
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

echo "Terminating with $OVERALL_TEST_RESULT"
exit $OVERALL_TEST_RESULT
