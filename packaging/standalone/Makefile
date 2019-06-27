SOURCE_DIST_FILE ?= $(wildcard ../../rabbitmq-server-*.tar.xz)

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

SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_TARBALL=rabbitmq-server-$(OS)-standalone-$(VERSION)
RLS_DIR=$(TARGET_DIR)/release/$(TARGET_DIR)

ERTS_VSN=$(shell erl -noshell -eval 'io:format("~s", [erlang:system_info(version)]), halt().')
ERTS_ROOT_DIR=$(shell erl -noshell -eval 'io:format("~s", [code:root_dir()]), halt().')
OTP_RELEASE=$(shell erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().')

# used to generate the erlang release
RABBITMQ_HOME=$(TARGET_DIR)
RABBITMQ_EBIN_ROOT=$(RABBITMQ_HOME)/ebin
RABBITMQ_PLUGINS_DIR=$(RABBITMQ_HOME)/plugins
RABBITMQ_PLUGINS_EXPAND_DIR=$(RABBITMQ_PLUGINS_DIR)/expand

RABBITMQ_DEFAULTS=$(TARGET_DIR)/sbin/rabbitmq-defaults
fix_defaults = sed -e $(1) $(RABBITMQ_DEFAULTS) > $(RABBITMQ_DEFAULTS).tmp \
	       && mv $(RABBITMQ_DEFAULTS).tmp $(RABBITMQ_DEFAULTS)

unexport DEPS_DIR
unexport ERL_LIBS
MAKEOVERRIDES =

# See comment below about `$(HOME)` and Freedesktop.org variables.
XDG_DATA_HOME ?= $(HOME)/.local/share
XDG_CONFIG_HOME ?= $(HOME)/.config

all: dist
	@:

dist:
	rm -rf $(SOURCE_DIR) $(TARGET_DIR)
	xzcat $(SOURCE_DIST_FILE) | tar -xf -

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
		RMQ_ERLAPP_DIR=$(abspath $(TARGET_DIR)) \
		MANDIR=$(abspath $(TARGET_DIR))/share/man \
		manpages install install-man

	mkdir -p $(TARGET_DIR)/etc/rabbitmq
	cp $(SOURCE_DIR)/deps/rabbit/docs/rabbitmq.config.example \
		$(TARGET_DIR)/etc/rabbitmq

## Here we set the RABBITMQ_HOME variable,
## then we make ERL_DIR point to our released erl
## and we add the paths to our released start_clean and start_sasl boot scripts
	$(call fix_defaults,'s:^SYS_PREFIX=$$:SYS_PREFIX=\$${RABBITMQ_HOME}:')
	$(call fix_defaults,'s:^ERL_DIR=$$:ERL_DIR=\$${RABBITMQ_HOME}/erts-$(ERTS_VSN)/bin/:')
	$(call fix_defaults,'s:start_clean$$:"\$${SYS_PREFIX}/releases/$(VERSION)/start_clean":')
	$(call fix_defaults,'s:start_sasl:"\$${SYS_PREFIX}/releases/$(VERSION)/start_sasl":')

	chmod 0755 $(RABBITMQ_DEFAULTS)

	mkdir -p $(TARGET_DIR)/etc/rabbitmq

	$(MAKE) generate_release

	mkdir -p $(RLS_DIR)
	tar -C $(RLS_DIR) -xzf $(RABBITMQ_HOME)/rabbit.tar.gz
	rm -rf $(RLS_DIR)/lib/rabbit-$(VERSION)/plugins/expand
	rm $(RLS_DIR)/releases/rabbit.rel

# add boot files
	cp $(ERTS_ROOT_DIR)/bin/start_clean.boot $(RLS_DIR)/releases/$(VERSION)
	cp $(ERTS_ROOT_DIR)/bin/start_sasl.boot $(RLS_DIR)/releases/$(VERSION)
	cp $(ERTS_ROOT_DIR)/bin/no_dot_erlang.boot $(RLS_DIR)/releases/$(VERSION)

# add OTP_VERSION file
	mkdir -p $(RLS_DIR)/releases/$(OTP_RELEASE)
	cp $(ERTS_ROOT_DIR)/releases/$(OTP_RELEASE)/OTP_VERSION $(RLS_DIR)/releases/$(OTP_RELEASE)/OTP_VERSION

# copy boot scripts to bin folder as Erlang does.
# Required by rabbit_nodes:ensure_epmd/0 or escript(1).
	mkdir -p $(RLS_DIR)/bin/
	cp $(RLS_DIR)/releases/$(VERSION)/*.boot $(RLS_DIR)/bin

# move rabbitmq files to top level folder
	mv $(RLS_DIR)/lib/rabbit-$(VERSION)/* $(RLS_DIR)

# remove empty lib/rabbit-$(VERSION) folder
	rm -rf $(RLS_DIR)/lib/rabbit-$(VERSION)

# fix Erlang ROOTDIR
	chmod +w $(RLS_DIR)/erts-$(ERTS_VSN)/bin/erl
	patch -o $(RLS_DIR)/erts-$(ERTS_VSN)/bin/erl $(RLS_DIR)/erts-$(ERTS_VSN)/bin/erl.src < erl.diff
	rm -f $(RLS_DIR)/erts-$(ERTS_VSN)/bin/erl.orig

	cd $(TARGET_DIR)/release && \
		find $(TARGET_DIR) | \
		LC_COLLATE=C sort > $(CURDIR)/manifest

	cd $(TARGET_DIR)/release && \
		tar -T $(CURDIR)/manifest --no-recursion -cf - | \
		xz > $(CURDIR)/$(TARGET_TARBALL).tar.xz

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv $(TARGET_TARBALL).tar.xz "$(PACKAGES_DIR)"; \
	fi

clean:
	rm -rf rabbitmq-server-* rabbitmq_server-* manifest src/rabbit_release.beam

.PHONY : generate_release
generate_release:
	erlc \
	    -I $(TARGET_DIR)/include/ -o src -Wall \
            -v +debug_info -Duse_proper_qc \
            -pa $(TARGET_DIR)/ebin/ src/rabbit_release.erl
	ERL_LIBS="$(TARGET_DIR)/plugins:$$ERL_LIBS" \
	erl \
	    -pa "$(RABBITMQ_EBIN_ROOT)" \
	    -pa src \
	    -noinput \
	    -hidden \
	    -s rabbit_release \
	    -extra "$(RABBITMQ_PLUGINS_DIR)" "$(RABBITMQ_PLUGINS_EXPAND_DIR)" "$(RABBITMQ_HOME)"
	test -f $(RABBITMQ_HOME)/rabbit.tar.gz
	rm src/rabbit_release.beam
