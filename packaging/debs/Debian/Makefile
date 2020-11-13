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

# $(DEBIAN_VERSION) doesn't include the package revision: this one is
# only set in debian/changelog.
DEBIAN_VERSION = $(subst -,~,$(VERSION))
DEBIAN_ORIG_TARBALL = rabbitmq-server_$(DEBIAN_VERSION).orig.tar.xz
UNPACKED_DIR = $(patsubst %.tar.xz,%,$(notdir $(SOURCE_DIST_FILE)))

DEB_HOST_ARCH = $(shell dpkg-architecture -qDEB_HOST_ARCH)
CHANGES_FILE = rabbitmq-server_$(DEBIAN_VERSION)-*_$(DEB_HOST_ARCH).changes

# Package signing.
#
# At least the key ID is mandatory ($(SIGNING_KEY)). If it's set, we
# enable signing in dpkg-build-package(1), otherwise we ask for an
# unsigned package.
#
# To maintain backward compatibility, the caller can also specify
# $(KEYSDIR) or $(GNUPG_PATH) and we set GNUPGHOME accordingly.

ifneq ($(SIGNING_KEY),)
	SIGNING_FLAG = -k$(SIGNING_KEY)
ifneq ($(KEYSDIR),)
	GNUPGHOME = $(KEYSDIR)/keyring/.gnupg
	export GNUPGHOME
endif
ifneq ($(GNUPG_PATH),)
	GNUPGHOME = $(GNUPG_PATH)/.gnupg
	export GNUPGHOME
endif
else
	SIGNING_FLAG = -us -uc
endif

unexport DEPS_DIR
unexport ERL_LIBS
MAKEOVERRIDES =

.PHONY: all package clean

all: package
	@:

package: clean
# If a signing key ID was specified, verify that the key is available
# before starting a possibly long build. At the same time, display some
# useful informations on the key so the caller can double-check if he
# wants.
ifneq ($(SIGNING_KEY),)
	@echo
	@echo '--------------------------------------------------'
	@echo "The package will be signed with key $(SIGNING_KEY):"
	@gpg -K "$(SIGNING_KEY)"
	@echo '--------------------------------------------------'
	@echo
endif
# Because we are creating a source package as well, Debian expects the
# source archive to have a specially formatted name. Copy the original
# archive to a correctly named file.
	cp -a "$(SOURCE_DIST_FILE)" "$(DEBIAN_ORIG_TARBALL)"
# Prepare the source directory: we extract the source archive and copy
# the debian/ subdirectory.
	xzcat "$(DEBIAN_ORIG_TARBALL)" | tar -xf -
	cp -a debian "$(UNPACKED_DIR)/debian"
ifeq ($(INSTALL_BUILD_DEPS),yes)
# Install build dependencies. To help us, we use mk-build-deps(1) from
# the devscripts package.
#
# We ignore errors from the first `dpkg -i` because the command will
# fail: dependencies are missing and dpkg(8) doesn't install them.
# That's why we have `apt-get install -f` following. To double-check
# everything went fine, we have the second `dpkg -i`.
	apt-get install -y --no-install-recommends devscripts equivs
	mk-build-deps debian/control
	-dpkg -i ./rabbitmq-server-build-deps_*_all.deb
	apt-get install -y -f -V --no-install-recommends
	dpkg -i ./rabbitmq-server-build-deps_*_all.deb
endif
# Log Erlang version.
	@echo
	@echo '--------------------------------------------------'
	@echo "Erlang and Elixir versions used to compile:"
	@elixir --version
	@echo '--------------------------------------------------'
	@echo
# Possibly update debian/changelog (in the created source directory):
#   - if it contains an entry for the specified version, do nothing;
#   - otherwise, prepend a generated entry using "1" as the package
#     revision.
	cd "$(UNPACKED_DIR)"; \
		../scripts/update-changelog.sh "$(DEBIAN_VERSION)"
# Finally build the package! We ask for both the source package and one
# or more binary packages.
	cd "$(UNPACKED_DIR)"; \
		dpkg-buildpackage -sa $(SIGNING_FLAG)
# Before we remove the source directory, copy the possibly updated
# debian/changelog to the original debian subdirectory, if the caller
# asks for it. He is then responsible for committing it.
ifeq ($(SAVE_CHANGELOG),yes)
	cp -a "$(UNPACKED_DIR)/debian/changelog" debian/changelog
endif
	rm -rf "$(UNPACKED_DIR)"
# If $(PACKAGES_DIR) is specified, move all package files to that
# location.
ifneq ($(PACKAGES_DIR),)
	mkdir -p "$(PACKAGES_DIR)"
	mv $$(./scripts/get-debian-package-files-list.sh $(CHANGES_FILE)) \
		"$(PACKAGES_DIR)"
endif

clean:
	rm -rf $(UNPACKED_DIR)
	rm -f $(DEBIAN_ORIG_TARBALL)
	./scripts/get-debian-package-files-list.sh $(CHANGES_FILE) | xargs rm -f
