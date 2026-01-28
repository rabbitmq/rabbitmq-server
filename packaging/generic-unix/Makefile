SOURCE_DIST_FILE ?= $(wildcard ../../../rabbitmq-server-*.tar.xz)

ifneq ($(filter-out clean,$(MAKECMDGOALS)),)
ifeq ($(SOURCE_DIST_FILE),)
$(error Cannot find source archive; please specify SOURCE_DIST_FILE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multiple source archives found; please specify SOURCE_DIST_FILE)
endif

VERSION ?= $(patsubst rabbitmq-server-%.tar.xz,%,$(notdir $(SOURCE_DIST_FILE)))
ifeq ($(VERSION),)
$(error Cannot determine version; please specify VERSION)
endif
endif

SOURCE_DIR = rabbitmq-server-$(VERSION)
TARGET_DIR = rabbitmq_server-$(VERSION)
TARBALL_SUFFIX ?= generic-unix
TARGET_TARBALL = rabbitmq-server-$(TARBALL_SUFFIX)-$(VERSION)

unexport DEPS_DIR
unexport ERL_LIBS
MAKEOVERRIDES =

# See comment below about `$(HOME)` and Freedesktop.org variables.
XDG_DATA_HOME ?= $(HOME)/.local/share
XDG_CONFIG_HOME ?= $(HOME)/.config

all: dist
	@:

dist:
# Log Erlang version.
	@echo
	@echo '--------------------------------------------------'
	@echo "Erlang and Elixir versions used to compile:"
	@elixir --version
	@echo '--------------------------------------------------'
	@echo
	xzcat $(SOURCE_DIST_FILE) | tar -xf -

# web-manpages are not used by generic-unix but by `make release` in the
# Umbrella. Those manpages are copied to www.rabbitmq.com
#
# We explicitely set $HOME as a Make variable below because some package
# builders do that, as part of cleaning the build environment. It
# exercises our hack to convince mix(1) to work offline because that
# hack depends on `$HOME`. A Make variable on the command line takes
# precedence over variables declared in Makefiles, so our hack needs
# to consider this. We do the same with the Freedesktop.org-specified
# variables ($XDG_*_HOME).
	$(MAKE) -C $(SOURCE_DIR) \
		HOME="$(HOME)" \
		XDG_DATA_HOME="$(XDG_DATA_HOME)" \
		XDG_CONFIG_HOME="$(XDG_CONFIG_HOME)" \
		PREFIX= RMQ_ROOTDIR= \
		RMQ_ERLAPP_DIR=`pwd`/$(TARGET_DIR) \
		MANDIR=`pwd`/$(TARGET_DIR)/share/man \
		manpages web-manpages install install-man

	sed -e 's:^SYS_PREFIX=$$:SYS_PREFIX=\$${RABBITMQ_HOME}:' \
        $(TARGET_DIR)/sbin/rabbitmq-defaults >$(TARGET_DIR)/sbin/rabbitmq-defaults.tmp \
        && mv $(TARGET_DIR)/sbin/rabbitmq-defaults.tmp $(TARGET_DIR)/sbin/rabbitmq-defaults
	chmod 0755 $(TARGET_DIR)/sbin/rabbitmq-defaults

	mkdir -p $(TARGET_DIR)/etc/rabbitmq

	find $(TARGET_DIR) | LC_COLLATE=C sort > $(TARGET_TARBALL).manifest
	tar --no-recursion -T $(TARGET_TARBALL).manifest -cf - | \
	xz > $(CURDIR)/$(TARGET_TARBALL).tar.xz

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv $(TARGET_TARBALL).tar.xz "$(PACKAGES_DIR)"; \
	fi

clean: clean_partial
	rm -f rabbitmq-server-$(TARBALL_SUFFIX)-*.tar.xz

clean_partial:
	rm -rf rabbitmq-server-* rabbitmq_server-*
