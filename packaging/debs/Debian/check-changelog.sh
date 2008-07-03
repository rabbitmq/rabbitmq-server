#!/bin/sh

PACKAGE_NAME=$1
cd $2

CHANGELOG_VERSION=$(dpkg-parsechangelog  | sed -n 's/^Version: \(.*\)-[^-]*$/\1/p')

if [ "${CHANGELOG_VERSION}" != "${VERSION}" ]; then
	if [ -n "${UNOFFICIAL_RELEASE}" ]; then
		echo "${PACKAGE_NAME} (${VERSION}-1) unstable; urgency=low" > debian/changelog.tmp
		echo >> debian/changelog.tmp
		echo "  * Unofficial release"  >> debian/changelog.tmp
		echo >> debian/changelog.tmp
		echo " -- Nobody <nobody@example.com>  $(date -R)" >> debian/changelog.tmp
		echo >> debian/changelog.tmp
		cat debian/changelog >> debian/changelog.tmp
		mv -f debian/changelog.tmp debian/changelog

		exit 0
	else
		echo
		echo There is no entry in debian/changelog for version ${VERSION}!
		echo Please create a changelog entry, or set the variable 
		echo UNOFFICIAL_RELEASE to automatically create one.
		echo

		exit 1
	fi
fi
