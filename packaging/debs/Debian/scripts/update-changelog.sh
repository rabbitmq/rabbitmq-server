#!/bin/sh

set -e

PACKAGE_VERSION=$1

if test -z "$PACKAGE_VERSION"; then
    echo "Syntax: $(basename "$0") <debian version>" 1>&2
    exit 64
fi

PACKAGE_NAME=$(awk '/^Source:/ { print $2; }' < debian/control)
CHANGELOG_DATE=$(date -R)

if dpkg-parsechangelog --all | \
 grep -E -q "^ $PACKAGE_NAME \(${PACKAGE_VERSION}-[^)]\) "; then
  exit 0
fi

cat > debian/changelog.tmp <<EOF
${PACKAGE_NAME} (${PACKAGE_VERSION}-1) unstable; urgency=low

  * New Upstream Release.

 -- RabbitMQ Team <info@rabbitmq.com>  ${CHANGELOG_DATE}

EOF

cat debian/changelog >> debian/changelog.tmp
mv -f debian/changelog.tmp debian/changelog

echo
echo '--------------------------------------------------'
dpkg-parsechangelog
echo '--------------------------------------------------'
echo
