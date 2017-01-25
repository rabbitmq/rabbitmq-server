#!/bin/sh

set -e

awk '
/^%changelog/ {
  in_changelog = 1;
  next;
}
{
  if (in_changelog) {
    print;
  }
}' "$@"
