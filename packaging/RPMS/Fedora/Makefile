SOURCE_DIST_FILE ?= $(wildcard ../../../rabbitmq-server-*.tar.xz)

ifneq ($(filter-out clean,$(MAKECMDGOALS)),)
ifeq ($(SOURCE_DIST_FILE),)
$(error Cannot find source archive; please specify SOURCE_DIST_FILE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multiple source archives found; please specify SOURCE_DIST_FILE)
endif
endif

VERSION ?= $(patsubst rabbitmq-server-%.tar.xz,%,$(notdir $(SOURCE_DIST_FILE)))
ifeq ($(VERSION),)
$(error Cannot determine version; please specify VERSION)
endif

# $(RPM_VERSION) doesn't include the package revision: this one is only
# set in the .spec file.
RPM_VERSION = $(subst -,~,$(VERSION))
RPM_ORIG_TARBALL = rabbitmq-server-$(VERSION).tar.xz

TOP_DIR = $(shell pwd)
# Under debian we do not want to check build dependencies, since that
# only checks build-dependencies using rpms, not debs
DEFINES = --define 'upstream_version $(VERSION)' \
	  --define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' \
	  --define '_sysconfdir /etc' --define '_localstatedir /var'

RPM_OS ?= fedora
DO_SIGN ?= yes

ifeq "$(RPM_OS)" "sles11"
FUNCTION_LIBRARY=
REQUIRES=/sbin/chkconfig /sbin/service
OS_DEFINES=--define '_initrddir /etc/init.d' --define 'dist .sles11' --define 'suse_version 1012'
SPEC_DEFINES=--define 'group_tag Productivity/Networking/Other'
START_PROG=startproc
DO_SIGN=no
else ifeq "$(RPM_OS)" "opensuse"
FUNCTION_LIBRARY=
REQUIRES=/sbin/chkconfig /sbin/service
OS_DEFINES=--define '_unitdir /usr/lib/systemd/system' --define 'dist .suse' --define 'suse_version 1315'
SPEC_DEFINES=--define 'group_tag Productivity/Networking/Other'
START_PROG=startproc
else
FUNCTION_LIBRARY=\# Source function library.\n. /etc/init.d/functions
REQUIRES=chkconfig initscripts
OS_DEFINES=--define '_initrddir /etc/rc.d/init.d'
ifeq "$(RPM_OS)" "rhel6"
SPEC_DEFINES=--define 'group_tag Development/Libraries' --define 'dist .el6' --define 'rhel 6'
else ifeq "$(RPM_OS)" "rhel7"
SPEC_DEFINES=--define 'group_tag Development/Libraries' --define '_unitdir /usr/lib/systemd/system' --define 'dist .el7' --define 'rhel 7'
else ifeq "$(RPM_OS)" "rhel8"
SPEC_DEFINES=--define 'group_tag Development/Libraries' --define '_unitdir /usr/lib/systemd/system' --define 'dist .el8' --define 'rhel 8'
else
SPEC_DEFINES=--define 'group_tag Development/Libraries'
endif
START_PROG=daemon
endif

# Package signing.
#
# At least the key ID is mandatory ($(SIGNING_KEY)). If it's set, we
# call `rpm --addsign`, otherwise we do nothing.
#
# To maintain backward compatibility, the caller can also specify
# $(KEYSDIR) or $(GNUPG_PATH) and we set GNUPGHOME accordingly.

ifneq ($(KEYSDIR),)
	GNUPGHOME = $(KEYSDIR)/keyring/.gnupg
	export GNUPGHOME
endif
ifneq ($(GNUPG_PATH),)
	GNUPGHOME = $(GNUPG_PATH)/.gnupg
	export GNUPGHOME
endif

unexport DEPS_DIR
unexport ERL_LIBS
MAKEOVERRIDES =

.PHONY: all package clean

all: package
	@:

package: clean
# Log Erlang version.
	@echo
	@echo '--------------------------------------------------'
	@echo "Erlang and Elixir versions used to compile:"
	@elixir --version
	@echo '--------------------------------------------------'
	@echo
# If a signing key ID was specified, verify that the key is available
# before starting a possibly long build. At the same time, display some
# useful informations on the key so the caller can double-check if he
# wants.
ifneq ($(SIGNING_KEY),)
ifeq ($(DO_SIGN),yes)
	@echo
	@echo '--------------------------------------------------'
	@echo "The package will be signed with key $(SIGNING_KEY):"
	@gpg -K "$(SIGNING_KEY)"
	@echo '--------------------------------------------------'
	@echo
else
	@echo
	@echo '--------------------------------------------------'
	@echo "Packages of type '$(RPM_OS)' are not signed because"
	@echo "the signature can't be verified on the target OS"
	@echo '--------------------------------------------------'
	@echo
endif
endif
# Create the build tree and populate it.
	mkdir -p BUILD SOURCES SPECS SRPMS RPMS tmp
	cp $(SOURCE_DIST_FILE) SOURCES/$(RPM_ORIG_TARBALL)
	cp rabbitmq-server.spec SPECS/rabbitmq-server.spec.in
	cp rabbitmq-server.service SOURCES/rabbitmq-server.service
	cp rabbitmq-server.tmpfiles SOURCES/rabbitmq-server.tmpfiles
	sed \
		-e 's|^START_PROG=.*$$|START_PROG="$(START_PROG)"|' \
		-e 's|^@FUNCTION_LIBRARY@|$(FUNCTION_LIBRARY)|' \
		< rabbitmq-server.init \
		> SOURCES/rabbitmq-server.init
	cp rabbitmq-server.logrotate SOURCES/rabbitmq-server.logrotate
# Possibly update the changelog inside the spec (in the created build
# directory):
#   - if it contains an entry for the specified version, do nothing;
#   - otherwise, prepend a generated entry using "1" as the package
#     revision.
	scripts/update-changelog.sh SPECS/rabbitmq-server.spec.in "$(RPM_VERSION)"
	sed -e 's|%%VERSION%%|$(RPM_VERSION)|;s|%%REQUIRES%%|$(REQUIRES)|' \
		< SPECS/rabbitmq-server.spec.in \
		> SPECS/rabbitmq-server.spec
# Finally build the package!
	rpmbuild -ba --nodeps SPECS/rabbitmq-server.spec $(DEFINES) \
		$(OS_DEFINES) $(SPEC_DEFINES)
# Before we remove the source directory, copy the possibly updated
# debian/changelog to the original debian subdirectory, if the caller
# asks for it. He is then responsible for committing it.
ifeq ($(SAVE_CHANGELOG),yes)
	mv SPECS/rabbitmq-server.spec.in rabbitmq-server.spec
endif
	rm -rf SOURCES SPECS BUILD tmp
# If a PGP key is specified, sign source and binary packages. We start
# rpm(1) in a new session and redirect its stdin so that it doesn't
# prompt for a passphrase.
# https://ask.fedoraproject.org/en/question/56107/can-gpg-agent-be-used-when-signing-rpm-packages/?answer=76395#post-id-76395
ifeq ($(DO_SIGN),yes)
ifneq ($(SIGNING_KEY),)
	setsid \
		rpm --addsign \
		--define '_signature gpg' \
		--define '_gpg_name $(SIGNING_KEY)' \
		SRPMS/*-$(RPM_VERSION)*.rpm \
		RPMS/noarch/*-$(RPM_VERSION)*.rpm \
		< /dev/null
endif
endif
# If $(PACKAGES_DIR) is specified, move all package files to that
# location.
ifneq ($(PACKAGES_DIR),)
	mkdir -p "$(PACKAGES_DIR)"
	mv SRPMS/*-$(RPM_VERSION)*.rpm RPMS/noarch/*-$(RPM_VERSION)*.rpm \
		"$(PACKAGES_DIR)"
endif

clean:
	rm -rf SOURCES SPECS RPMS SRPMS BUILD tmp
