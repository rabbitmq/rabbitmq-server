#!/bin/sh

PACKAGE_VERSION=$1

PACKAGE_NAME=$(awk '/^Source:/ { print $2; }' < debian/control)
CHANGELOG_VERSION=$(dpkg-parsechangelog  | sed -n 's/^Version: \(.*\)-[^-]*$/\1/p')
CHANGELOG_DATE=$(date -R)

if [ "${CHANGELOG_VERSION}" != "${PACKAGE_VERSION}" ]; then
  cat > debian/changelog.tmp <<EOF
${PACKAGE_NAME} (${PACKAGE_VERSION}-1) unstable; urgency=low

  * New Upstream Release.

 -- RabbitMQ Team <info@rabbitmq.com>  ${CHANGELOG_DATE}

EOF

  cat debian/changelog >> debian/changelog.tmp
  mv -f debian/changelog.tmp debian/changelog
fi

echo
echo '--------------------------------------------------'
dpkg-parsechangelog
echo '--------------------------------------------------'
echo
