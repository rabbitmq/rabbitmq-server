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
TARGET_ZIP = rabbitmq-server-windows-$(VERSION)

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

# We explicitely set $HOME as a Make variable below because some package
# builders do that, as part of cleaning the build environment. It
# exercises our hack to convince mix(1) to work offline because that
# hack depends on `$HOME`. A Make variable on the command line takes
# precedence over variables declared in Makefiles, so our hack needs
# to consider this. We do the same with the Freedesktop.org-specified
# variables ($XDG_*_HOME).
	$(MAKE) -C $(SOURCE_DIR) install-windows \
		DESTDIR=$(abspath $(TARGET_DIR)) \
		WINDOWS_PREFIX= \
		HOME="$(HOME)" \
		XDG_DATA_HOME="$(XDG_DATA_HOME)" \
		XDG_CONFIG_HOME="$(XDG_CONFIG_HOME)"

	cp -a README-etc $(TARGET_DIR)/etc/README.txt

	zip -q -r $(TARGET_ZIP).zip $(TARGET_DIR)

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv $(TARGET_ZIP).zip "$(PACKAGES_DIR)"; \
	fi

clean:
	rm -rf rabbitmq-server-* rabbitmq_server-*
