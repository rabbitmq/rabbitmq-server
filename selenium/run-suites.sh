#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SUITES_DIR="$SCRIPT/suites"

TARGET=${1:-"full-suite-management-ui"}
OVERALL_TEST_RESULT=0
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

SUCCESSFUL_SUITES=()
FAILED_SUITES=()

SEARCH_DIR=""
if [ -d "$SCRIPT/$TARGET" ]; then
  SEARCH_DIR="$SCRIPT/$TARGET"
elif [ -d "$SCRIPT/suites/$TARGET" ]; then
  SEARCH_DIR="$SCRIPT/suites/$TARGET"
elif [ -d "$TARGET" ]; then
  SEARCH_DIR="$TARGET"
fi

if [ -n "$SEARCH_DIR" ]; then
  SUITES_LIST=$(find "$SEARCH_DIR" -type f -name "*.sh" | sed "s|^${SUITES_DIR}/||" | sort -u)
else
  if [ -f "$SCRIPT/$TARGET" ]; then
    FILE_PATH="$SCRIPT/$TARGET"
  elif [ -f "$SCRIPT/suites/$TARGET" ]; then
    FILE_PATH="$SCRIPT/suites/$TARGET"
  elif [ -f "$TARGET" ]; then
    FILE_PATH="$TARGET"
  else
    echo "Error: Suite file or directory '$TARGET' not found"
    exit 1
  fi

  if [[ "$FILE_PATH" == *.sh ]]; then
    SUITES_LIST=$(echo "$FILE_PATH" | sed "s|^${SUITES_DIR}/||")
  else
    SUITES_LIST=$(grep -v "^[[:space:]]*#" "$FILE_PATH" | grep -v "^[[:space:]]*$" | sort -u)
  fi
fi

if [ -z "$SUITES_LIST" ]; then
  echo "Error: No test suites found for '$TARGET'"
  exit 1
fi

TOTAL_SUITES=$(echo "$SUITES_LIST" | wc -l | awk '{print $1}')

while read -r SUITE
do
  [ -z "$SUITE" ] && continue
  echo -e "=== Running suite (${TOTAL_SUITES}/${GREEN}${#SUCCESSFUL_SUITES[@]}/${RED}${#FAILED_SUITES[@]}${NC}) $SUITE ============================================"
  echo " "
  
  ENV_MODES="docker" $SCRIPT/suites/$SUITE
  TEST_RESULT="$?"
  TEST_STATUS="${GREEN}Succeeded${NC}"
  if [ "$TEST_RESULT" -ne 0 ]
  then
    OVERALL_TEST_RESULT=$TEST_RESULT
    TEST_STATUS=" ${RED}Failed${NC}"
    FAILED_SUITES+=("$SUITE")
  else
    SUCCESSFUL_SUITES+=("$SUITE")
  fi
  echo -e "=== $TEST_STATUS $SUITE ==========================================="
  echo " "
done <<< "$SUITES_LIST"

echo -e "=== Summary (${TOTAL_SUITES}/${GREEN}${#SUCCESSFUL_SUITES[@]}/${RED}${#FAILED_SUITES[@]}${NC}) ============================================"
if [ ${#SUCCESSFUL_SUITES[@]} -gt 0 ]; then echo -e " > ${GREEN}Successful suites ${NC}"; fi
for f in "${SUCCESSFUL_SUITES[@]}"
do
  echo "   - $f"
done

if [ ${#FAILED_SUITES[@]} -gt 0 ]; then echo -e " > ${RED}Failed suites ${NC}"; fi
for f in "${FAILED_SUITES[@]}"
do
  echo "   - $f"
done

echo "Terminating with $OVERALL_TEST_RESULT"
exit $OVERALL_TEST_RESULT
