#!/bin/sh
# vim:sw=2:et:

set -e

case $# in
  1)
    SPEC=$(dirname "$0")/../rabbitmq-server.spec
    PACKAGE_VERSION=$1
    ;;
  2)
    SPEC=$1
    PACKAGE_VERSION=$2
    ;;
esac

SCRIPT=$(basename "$0")
SCRIPTS_DIR=$(dirname "$0")

if test -z "$SPEC" -o ! -f "$SPEC" -o -z "$PACKAGE_VERSION"; then
    echo "Syntax: $SCRIPT [<spec file>] <rpm version>" 1>&2
    exit 64
fi

if "$SCRIPTS_DIR/parse-changelog.sh" "$SPEC" | \
 grep -E -q "^\*.+ ${PACKAGE_VERSION}-[^ ]+$"; then
  exit 0
fi

CHANGELOG_PKG_REV=1
CHANGELOG_EMAIL='info@rabbitmq.com'
CHANGELOG_COMMENT='New upstream release.'

awk "
/^Release:/ {
  if (!release_modified) {
    release = \$0;
    sub(/[0-9]+/, \"${CHANGELOG_PKG_REV}\", release);
    print release;
    release_modified = 1;
    next;
  }
}
/^%changelog/ {
  print;
  print \"* $(date +'%a %b %-d %Y') ${CHANGELOG_EMAIL} ${PACKAGE_VERSION}-${CHANGELOG_PKG_REV}\";
  print \"- ${CHANGELOG_COMMENT}\";
  print \"\";
  next;
}
{
  print;
}
" < "$SPEC" > "$SPEC.updated"

mv "$SPEC.updated" "$SPEC"
