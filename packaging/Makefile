# Platform detection.

ifeq ($(PLATFORM),)
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Linux)
PLATFORM = linux
else ifeq ($(UNAME_S),Darwin)
PLATFORM = darwin
else ifeq ($(UNAME_S),SunOS)
PLATFORM = solaris
else ifeq ($(UNAME_S),GNU)
PLATFORM = gnu
else ifeq ($(UNAME_S),FreeBSD)
PLATFORM = freebsd
else ifeq ($(UNAME_S),NetBSD)
PLATFORM = netbsd
else ifeq ($(UNAME_S),OpenBSD)
PLATFORM = openbsd
else ifeq ($(UNAME_S),DragonFly)
PLATFORM = dragonfly
else ifeq ($(shell uname -o),Msys)
PLATFORM = msys2
else
$(error Unable to detect platform. Please open a ticket with the output of uname -a.)
endif
endif

all: packages
	@:

# --------------------------------------------------------------------
# Packaging.
# --------------------------------------------------------------------

.PHONY: packages package-deb \
	package-rpm \
	package-rpm-redhat package-rpm-fedora \
	package-rpm-rhel6 package-rpm-rhel7 package-rpm-rhel8 \
	package-rpm-suse package-rpm-opensuse package-rpm-sles11 \
	package-windows \
	package-generic-unix

PACKAGES_DIR ?= ../PACKAGES
SOURCE_DIST_FILE ?= $(wildcard $(PACKAGES_DIR)/rabbitmq-server-*.tar.xz)

ifneq ($(filter-out clean,$(MAKECMDGOALS)),)
ifeq ($(SOURCE_DIST_FILE),)
$(error Cannot find source archive; please specify SOURCE_DIST_FILE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multiple source archives found; please specify SOURCE_DIST_FILE)
endif
ifeq ($(filter %.tar.xz %.txz,$(SOURCE_DIST_FILE)),)
$(error The source archive must a tar.xz archive)
endif
ifeq ($(wildcard $(SOURCE_DIST_FILE)),)
$(error The source archive must exist)
endif
endif

ifndef NO_CLEAN
DO_CLEAN := clean
endif

VARS = SOURCE_DIST_FILE="$(abspath $(SOURCE_DIST_FILE))" \
       PACKAGES_DIR="$(abspath $(PACKAGES_DIR))" \
       SIGNING_KEY="$(SIGNING_KEY)"

packages: package-deb package-rpm package-windows package-generic-unix
	@:

package-deb: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C debs/Debian $(VARS) all $(DO_CLEAN)

package-rpm: package-rpm-redhat package-rpm-suse
	@:

# FIXME: Why not package-rpm-fedora?
package-rpm-redhat: package-rpm-rhel6 package-rpm-rhel7 package-rpm-rhel8
	@:

package-rpm-fedora: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C RPMS/Fedora $(VARS) all $(DO_CLEAN)

package-rpm-rhel6: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C RPMS/Fedora $(VARS) RPM_OS=rhel6 all $(DO_CLEAN)

package-rpm-rhel7: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C RPMS/Fedora $(VARS) RPM_OS=rhel7 all $(DO_CLEAN)

package-rpm-rhel8: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C RPMS/Fedora $(VARS) RPM_OS=rhel8 all $(DO_CLEAN)

package-rpm-suse: package-rpm-opensuse package-rpm-sles11
	@:

package-rpm-opensuse: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C RPMS/Fedora $(VARS) RPM_OS=opensuse all $(DO_CLEAN)

package-rpm-sles11: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C RPMS/Fedora $(VARS) RPM_OS=sles11 all $(DO_CLEAN)

package-windows: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C windows $(VARS) all $(DO_CLEAN)
	$(verbose) $(MAKE) -C windows-exe $(VARS) all $(DO_CLEAN)

package-generic-unix: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C generic-unix $(VARS) all $(DO_CLEAN)

.PHONY: clean

clean:
	for subdir in debs/Debian RPMS/Fedora windows windows-exe generic-unix; do \
		$(MAKE) -C "$$subdir" clean; \
	done
