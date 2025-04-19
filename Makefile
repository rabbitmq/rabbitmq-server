PROJECT = rabbitmq_server_release
PROJECT_DESCRIPTION = RabbitMQ Server

# Propagate PROJECT_VERSION (from the command line or environment) to
# other components. If PROJECT_VERSION is unset, then an empty variable
# is propagated and the default version will fallback to the default
# value from rabbitmq-components.mk.
export RABBITMQ_VERSION := $(PROJECT_VERSION)

# Release artifacts are put in $(PACKAGES_DIR).
PACKAGES_DIR ?= $(abspath PACKAGES)

# List of plugins to include in a RabbitMQ release.
include plugins.mk

# An additional list of plugins to include in a RabbitMQ release,
# on top of the standard plugins.
#
# Note: When including NIFs in a release make sure to build
# them on the appropriate platform for the target environment.
# For example build on Linux when targeting Docker.
ADDITIONAL_PLUGINS ?=

DEPS = rabbit_common rabbit $(PLUGINS) $(ADDITIONAL_PLUGINS)

DEP_PLUGINS = rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk

DISABLE_DISTCLEAN = 1

ifeq ($(filter-out xref,$(MAKECMDGOALS)),)
XREF_SCOPE = app deps

# For Elixir protocols the right fix is to include the consolidated/
# folders in addition to ebin/. However this creates conflicts because
# some modules are duplicated. So instead we ignore warnings from
# protocols directly.
XREF_IGNORE = [ \
    {'Elixir.CSV.Encode',impl_for,1}, \
    {'Elixir.JSON.Decoder',impl_for,1}, \
    {'Elixir.JSON.Encoder',impl_for,1}, \
    {'Elixir.RabbitMQ.CLI.Core.DataCoercion',impl_for,1}]

# Include Elixir libraries in the Xref checks.
xref: ERL_LIBS := $(ERL_LIBS):$(CURDIR)/apps:$(CURDIR)/deps:$(dir $(shell elixir --eval ':io.format "~s~n", [:code.lib_dir :elixir ]'))
endif

include rabbitmq-components.mk

# Set PROJECT_VERSION, calculated in rabbitmq-components.mk,
# in stone now, because in this Makefile we will be using it
# multiple times (including for release file names and whatnot).
PROJECT_VERSION := $(PROJECT_VERSION)

# Fetch/build community plugins.
#
# To include community plugins in commands, use
# `make COMMUNITY_PLUGINS=1` or export the variable.
# They are not included otherwise. Note that only
# the top-level Makefile can do this.
#
# Note that the community plugins will be fetched using
# SSH and therefore may be subject to GH authentication.

ifdef COMMUNITY_PLUGINS
DEPS += $(RABBITMQ_COMMUNITY)
endif

include erlang.mk
include mk/github-actions.mk

# If PLUGINS was set when we use run-broker we want to
# fill in the enabled plugins list. PLUGINS is a more
# natural space-separated list.
ifdef PLUGINS
RABBITMQ_ENABLED_PLUGINS ?= $(call comma_list,$(PLUGINS))
endif

# --------------------------------------------------------------------
# Distribution - common variables and generic functions.
# --------------------------------------------------------------------

RSYNC ?= rsync
RSYNC_V_0 =
RSYNC_V_1 = -v
RSYNC_V_2 = -v
RSYNC_V = $(RSYNC_V_$(V))
BASE_RSYNC_FLAGS += -a $(RSYNC_V) \
	       --delete					\
	       --delete-excluded			\
	       --exclude '.sw?' --exclude '.*.sw?'	\
	       --exclude '*.beam'			\
	       --exclude '*.d'				\
	       --exclude '*.pyc'			\
	       --exclude '.git*'			\
	       --exclude '.hg*'				\
	       --exclude '.*.plt'			\
	       --exclude 'erlang_ls.config'		\
	       --exclude '$(notdir $(ERLANG_MK_TMP))'	\
	       --exclude '_build/'			\
	       --exclude '__pycache__/'			\
	       --exclude 'tools/'			\
	       --exclude 'ci/'				\
	       --exclude 'cover/'			\
	       --exclude 'deps/'			\
	       --exclude 'doc/'				\
	       --exclude 'docker/'			\
	       --exclude 'ebin/'			\
	       --exclude 'erl_crash.dump'		\
	       --exclude 'escript/'			\
	       --exclude 'MnesiaCore.*'			\
	       --exclude '$(notdir $(DEPS_DIR))/'	\
	       --exclude 'hexer*'			\
	       --exclude 'logs/'			\
	       --exclude 'PKG_*.md'			\
	       --exclude '/plugins/'			\
	       --include 'cli/plugins'			\
	       --exclude '$(notdir $(DIST_DIR))/'	\
	       --exclude '/$(notdir $(PACKAGES_DIR))/'	\
	       --exclude '/PACKAGES/'			\
	       --exclude '/amqp_client/doc/'		\
	       --exclude '/amqp_client/rebar.config'	\
	       --exclude '/cowboy/doc/'			\
	       --exclude '/cowboy/examples/'		\
	       --exclude '/rabbit/escript/'		\
	       --exclude '/rabbitmq_cli/escript/'	\
	       --exclude '/rabbitmq_mqtt/test/build/'	\
	       --exclude '/rabbitmq_mqtt/test/test_client/'\
	       --exclude '/rabbitmq_trust_store/examples/'\
	       --exclude '/ranch/doc/'			\
	       --exclude '/ranch/examples/'		\
	       --exclude '/sockjs/examples/'		\
	       --exclude '/workflow_sources/'

SOURCE_DIST_RSYNC_FLAGS += $(BASE_RSYNC_FLAGS)          \
	       --exclude 'packaging'			\
	       --exclude 'test'

# For source-bundle, explicitly include folders that are needed
# for tests to execute. These are added before excludes from
# the base flags so rsync honors the first match.
SOURCE_BUNDLE_RSYNC_FLAGS += \
	       --include 'rabbit_shovel_test/ebin'      \
	       --include 'rabbit_shovel_test/ebin/*'    \
	       --include 'rabbitmq_ct_helpers/tools'    \
	       --include 'rabbitmq_ct_helpers/tools/*'  \
               $(BASE_RSYNC_FLAGS)

TAR ?= tar
TAR_V_0 =
TAR_V_1 = -v
TAR_V_2 = -v
TAR_V = $(TAR_V_$(V))

GZIP ?= gzip
BZIP2 ?= bzip2
XZ ?= xz

ZIP ?= zip
ZIP_V_0 = -q
ZIP_V_1 =
ZIP_V_2 =
ZIP_V = $(ZIP_V_$(V))

ifeq ($(shell tar --version | grep -c "GNU tar"),0)
# Skip all flags if this is Darwin (a.k.a. macOS, a.k.a. OS X)
ifeq ($(shell uname | grep -c "Darwin"),0)
TAR_FLAGS_FOR_REPRODUCIBLE_BUILDS = --uid 0 \
				    --gid 0 \
				    --numeric-owner \
				    --no-acls \
				    --no-fflags \
				    --no-xattrs
endif
else
TAR_FLAGS_FOR_REPRODUCIBLE_BUILDS = --owner 0 \
				    --group 0 \
				    --numeric-owner
endif

DIST_SUFFIXES ?= tar.xz

# Function to create distribution targets
# Args: $(1) - Full distribution path
#       $(2) - RSYNC flags to use
define create_dist_target
$(1): $(ERLANG_MK_RECURSIVE_DEPS_LIST)
	$${verbose} mkdir -p $$(dir $$@)
	$${gen_verbose} $${RSYNC} $(2) ./ $$@/
	$${verbose} echo "$(PROJECT_DESCRIPTION) $(PROJECT_VERSION)" > $$@/git-revisions.txt
	$${verbose} echo "$(PROJECT) $$$$(git rev-parse HEAD) $$$$(git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)" >> $$@/git-revisions.txt
	$${verbose} echo "$$$$(TZ= git --no-pager log -n 1 --format='%cd' --date='format-local:%Y%m%d%H%M.%S')" > $$@.git-times.txt
	$${verbose} cat packaging/common/LICENSE.head > $$@/LICENSE
	$${verbose} mkdir -p $$@/deps/licensing
	$${verbose} set -e; for dep in $$$$(cat $(ERLANG_MK_RECURSIVE_DEPS_LIST) | LC_COLLATE=C sort); do \
		$${RSYNC} $(2) \
		 $$$$dep \
		 $$@/deps; \
		rm -f \
		 $$@/deps/rabbit_common/rebar.config \
		 $$@/deps/rabbit_common/rebar.lock; \
		if test -f $$@/deps/$$$$(basename $$$$dep)/erlang.mk && \
		   test "$$$$(wc -l $$@/deps/$$$$(basename $$$$dep)/erlang.mk | awk '{print $$$$1;}')" = "1" && \
		   grep -qs -E "^[[:blank:]]*include[[:blank:]]+(erlang\.mk|.*/erlang\.mk)$$$$" $$@/deps/$$$$(basename $$$$dep)/erlang.mk; then \
			echo "include ../../erlang.mk" > $$@/deps/$$$$(basename $$$$dep)/erlang.mk; \
		fi; \
		sed -E -i.bak "s|^[[:blank:]]*include[[:blank:]]+\.\./.*erlang.mk$$$$|include ../../erlang.mk|" \
		 $$@/deps/$$$$(basename $$$$dep)/Makefile && \
		rm $$@/deps/$$$$(basename $$$$dep)/Makefile.bak; \
		if test -f "$$$$dep/license_info"; then \
			cp "$$$$dep/license_info" "$$@/deps/licensing/license_info_$$$$(basename $$$$dep)"; \
			cat "$$$$dep/license_info" >> $$@/LICENSE; \
		fi; \
		find "$$$$dep" -maxdepth 1 -name 'LICENSE-*' -exec cp '{}' $$@/deps/licensing \; ; \
		(cd $$$$dep; \
		 echo "$$$$(basename "$$$$dep") $$$$(git rev-parse HEAD) $$$$(git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)") \
		 >> "$$@/git-revisions.txt"; \
		! test -d $$$$dep/.git || (cd $$$$dep; \
		 echo "$$$$(env TZ= git --no-pager log -n 1 --format='%cd' --date='format-local:%Y%m%d%H%M.%S')") \
		 >> "$$@.git-times.txt"; \
	done
	$${verbose} cat packaging/common/LICENSE.tail >> $$@/LICENSE
	$${verbose} find $$@/deps/licensing -name 'LICENSE-*' -exec cp '{}' $$@ \;
	$${verbose} rm -rf $$@/deps/licensing
	$${verbose} for file in $$$$(find $$@ -name '*.app.src'); do \
		sed -E -i.bak \
		  -e 's/[{]vsn[[:blank:]]*,[[:blank:]]*(""|"0.0.0")[[:blank:]]*}/{vsn, "$(PROJECT_VERSION)"}/' \
		  -e 's/[{]broker_version_requirements[[:blank:]]*,[[:blank:]]*\[\][[:blank:]]*}/{broker_version_requirements, ["$(PROJECT_VERSION)"]}/' \
		  $$$$file; \
		rm $$$$file.bak; \
	done
	$${verbose} echo "PLUGINS := $(PLUGINS)" > $$@/plugins.mk
	$${verbose} sort -r < "$$@.git-times.txt" | head -n 1 > "$$@.git-time.txt"
	$${verbose} find $$@ -print0 | xargs -0 touch -t "$$$$(cat $$@.git-time.txt)"
	$${verbose} rm "$$@.git-times.txt" "$$@.git-time.txt"

$(1).manifest: $(1)
	$${gen_verbose} cd $$(dir $$@) && \
		find $$(notdir $$<) | LC_COLLATE=C sort > $$@

$(1).tar.xz: $(1).manifest
	$${gen_verbose} cd $$(dir $$@) && \
		$${TAR} $${TAR_V} $${TAR_FLAGS_FOR_REPRODUCIBLE_BUILDS} --no-recursion -T $$(notdir $$<) -cf - | \
		$${XZ} > $$@

$(1).tar.gz: $(1).manifest
	$${gen_verbose} cd $$(dir $$@) && \
		$${TAR} $${TAR_V} $${TAR_FLAGS_FOR_REPRODUCIBLE_BUILDS} --no-recursion -T $$(notdir $$<) -cf - | \
		$${GZIP} --best > $$@

$(1).tar.bz2: $(1).manifest
	$${gen_verbose} cd $$(dir $$@) && \
		$${TAR} $${TAR_V} $${TAR_FLAGS_FOR_REPRODUCIBLE_BUILDS} --no-recursion -T $$(notdir $$<) -cf - | \
		$${BZIP2} > $$@

$(1).zip: $(1).manifest
	$${verbose} rm -f $$@
	$${gen_verbose} cd $$(dir $$@) && \
		$${ZIP} $${ZIP_V} --names-stdin $$@ < $$(notdir $$<)

endef

# Function to create clean targets
# Args: $(1) - Base name (e.g. SOURCE_DIST_BASE or BUNDLE_DIST_BASE)
define create_clean_targets
.PHONY: clean-$(1)

clean-$(1):
	$${gen_verbose} rm -rf -- $(1)-*

# Add each clean target to the clean:: rule
clean:: clean-$(1)
endef

# --------------------------------------------------------------------
# Distribution - public targets
# --------------------------------------------------------------------

SOURCE_DIST_BASE ?= rabbitmq-server
SOURCE_DIST ?= $(PACKAGES_DIR)/$(SOURCE_DIST_BASE)-$(PROJECT_VERSION)
SOURCE_DIST_FILES = $(addprefix $(SOURCE_DIST).,$(DIST_SUFFIXES))

.PHONY: source-dist
source-dist: $(SOURCE_DIST_FILES)
	@:

$(eval $(call create_dist_target,$(SOURCE_DIST),$(SOURCE_DIST_RSYNC_FLAGS)))

SOURCE_BUNDLE_BASE ?= rabbitmq-server-bundle
SOURCE_BUNDLE_DIST ?= $(PACKAGES_DIR)/$(SOURCE_BUNDLE_BASE)-$(PROJECT_VERSION)
SOURCE_BUNDLE_FILES = $(addprefix $(SOURCE_BUNDLE_DIST).,$(DIST_SUFFIXES))

.PHONY: source-bundle
source-bundle: $(SOURCE_BUNDLE_FILES)
	@:

$(eval $(call create_dist_target,$(SOURCE_BUNDLE_DIST),$(SOURCE_BUNDLE_RSYNC_FLAGS)))

# Create the clean targets for both distributions
$(eval $(call create_clean_targets,$(SOURCE_DIST_BASE)))
$(eval $(call create_clean_targets,$(SOURCE_BUNDLE_BASE)))

.PHONY: distclean-packages clean-unpacked-source-dist

distclean:: distclean-packages

distclean-packages:
	$(gen_verbose) rm -rf -- $(PACKAGES_DIR)

## If a dependency doesn't have a clean target - do not call it
clean-unpacked-source-dist:
	for d in deps/*; do \
		if test -f $$d/Makefile; then \
			(! make -n clean) || (make -C $$d clean || exit $$?); \
		fi; \
	done

clean-deps:
	git clean -xfffd deps

# --------------------------------------------------------------------
# Packaging.
# --------------------------------------------------------------------

.PHONY: packages package-deb \
	package-rpm package-rpm-fedora package-rpm-suse \
	package-windows \
	package-generic-unix \
	docker-image

# This variable is exported so sub-make instances know where to find the
# archive.
PACKAGES_SOURCE_DIST_FILE ?= $(firstword $(SOURCE_DIST_FILES))

RABBITMQ_PACKAGING_TARGETS = package-deb package-rpm \
package-rpm-redhat package-rpm-fedora package-rpm-rhel8 \
package-rpm-suse package-rpm-opensuse \
package-windows

ifneq ($(filter $(RABBITMQ_PACKAGING_TARGETS),$(MAKECMDGOALS)),)
ifeq ($(RABBITMQ_PACKAGING_REPO),)
$(error Cannot find rabbitmq-packaging repository dir; please clone from rabbitmq/rabbitmq-packaging and specify RABBITMQ_PACKAGING_REPO)
endif
endif

$(RABBITMQ_PACKAGING_TARGETS): $(PACKAGES_SOURCE_DIST_FILE)
	$(verbose) $(MAKE) -C $(RABBITMQ_PACKAGING_REPO) $@ \
		SOURCE_DIST_FILE=$(abspath $(PACKAGES_SOURCE_DIST_FILE))

package-generic-unix \
docker-image: $(PACKAGES_SOURCE_DIST_FILE)
	$(verbose) $(MAKE) -C packaging $@ \
		SOURCE_DIST_FILE=$(abspath $(PACKAGES_SOURCE_DIST_FILE))

packages: package-deb package-rpm package-windows package-generic-unix

# --------------------------------------------------------------------
# Installation.
# --------------------------------------------------------------------

.PHONY: manpages web-manpages distclean-manpages

manpages web-manpages distclean-manpages:
	$(MAKE) -C $(DEPS_DIR)/rabbit $@ DEPS_DIR=$(DEPS_DIR)

.PHONY: install install-erlapp install-scripts install-bin install-man \
	install-windows install-windows-erlapp install-windows-scripts \
	install-windows-docs

DESTDIR ?=

PREFIX ?= /usr/local
WINDOWS_PREFIX ?= rabbitmq-server-windows-$(PROJECT_VERSION)

MANDIR ?= $(PREFIX)/share/man
RMQ_ROOTDIR ?= $(PREFIX)/lib/erlang
RMQ_BINDIR ?= $(RMQ_ROOTDIR)/bin
RMQ_LIBDIR ?= $(RMQ_ROOTDIR)/lib
RMQ_ERLAPP_DIR ?= $(RMQ_LIBDIR)/rabbitmq_server-$(PROJECT_VERSION)
RMQ_AUTOCOMPLETE_DIR ?= $(RMQ_ROOTDIR)/autocomplete

SCRIPTS = rabbitmq-defaults \
	  rabbitmq-env \
	  rabbitmq-server \
	  rabbitmqctl \
	  rabbitmq-plugins \
	  rabbitmq-diagnostics \
	  rabbitmq-queues \
	  rabbitmq-upgrade \
	  rabbitmq-streams \
	  vmware-rabbitmq

AUTOCOMPLETE_SCRIPTS = bash_autocomplete.sh zsh_autocomplete.sh

WINDOWS_SCRIPTS = rabbitmq-defaults.bat \
		  rabbitmq-echopid.bat \
		  rabbitmq-env.bat \
		  rabbitmq-plugins.bat \
		  rabbitmq-diagnostics.bat \
		  rabbitmq-queues.bat \
		  rabbitmq-server.bat \
		  rabbitmq-service.bat \
		  rabbitmq-upgrade.bat \
		  rabbitmq-streams.bat \
		  vmware-rabbitmq.bat \
		  rabbitmqctl.bat

UNIX_TO_DOS ?= todos

inst_verbose_0 = @echo " INST  " $@;
inst_verbose = $(inst_verbose_$(V))

install: install-erlapp install-scripts

install-erlapp: dist
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_ERLAPP_DIR)
	$(inst_verbose) cp -r \
		LICENSE* \
		$(DEPS_DIR)/rabbit/INSTALL \
		$(DIST_DIR) \
		$(DESTDIR)$(RMQ_ERLAPP_DIR)
	$(verbose) echo "Put your EZs here and use rabbitmq-plugins to enable them." \
		> $(DESTDIR)$(RMQ_ERLAPP_DIR)/$(notdir $(DIST_DIR))/README

CLI_ESCRIPTS_DIR = escript

install-escripts:
	$(verbose) $(MAKE) -C $(DEPS_DIR)/rabbitmq_cli install \
		PREFIX="$(RMQ_ERLAPP_DIR)/$(CLI_ESCRIPTS_DIR)"

install-scripts: install-escripts
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_ERLAPP_DIR)/sbin
	$(inst_verbose) for script in $(SCRIPTS); do \
		cp "$(DEPS_DIR)/rabbit/scripts/$$script" \
			"$(DESTDIR)$(RMQ_ERLAPP_DIR)/sbin"; \
		chmod 0755 "$(DESTDIR)$(RMQ_ERLAPP_DIR)/sbin/$$script"; \
	done

# FIXME: We do symlinks to scripts in $(RMQ_ERLAPP_DIR))/sbin but this
# code assumes a certain hierarchy to make relative symlinks.
install-bin: install-scripts install-autocomplete-scripts
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_BINDIR)
	$(inst_verbose) for script in $(SCRIPTS); do \
		test -e $(DESTDIR)$(RMQ_BINDIR)/$$script || \
			ln -sf ../lib/$(notdir $(RMQ_ERLAPP_DIR))/sbin/$$script \
			 $(DESTDIR)$(RMQ_BINDIR)/$$script; \
	done

install-autocomplete-scripts:
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_AUTOCOMPLETE_DIR)
	$(inst_verbose) for script in $(AUTOCOMPLETE_SCRIPTS); do \
		cp "scripts/$$script" \
			"$(DESTDIR)$(RMQ_AUTOCOMPLETE_DIR)" && \
		chmod 0755 "$(DESTDIR)$(RMQ_AUTOCOMPLETE_DIR)/$$script"; \
	done

install-man: manpages
	$(inst_verbose) sections=$$(ls -1 $(DEPS_DIR)/rabbit/docs/*.[1-9] \
		| sed -E 's/.*\.([1-9])$$/\1/' | uniq | sort); \
	for section in $$sections; do \
		mkdir -p $(DESTDIR)$(MANDIR)/man$$section; \
		for manpage in $(DEPS_DIR)/rabbit/docs/*.$$section; do \
			gzip < $$manpage \
			 > $(DESTDIR)$(MANDIR)/man$$section/$$(basename $$manpage).gz; \
		done; \
	done

install-windows: install-windows-erlapp install-windows-scripts install-windows-docs

install-windows-erlapp: dist
	$(verbose) mkdir -p $(DESTDIR)$(WINDOWS_PREFIX)
	$(inst_verbose) cp -r \
		LICENSE* \
		$(DEPS_DIR)/rabbit/INSTALL \
		$(DIST_DIR) \
		$(DESTDIR)$(WINDOWS_PREFIX)
	$(verbose) echo "Put your EZs here and use rabbitmq-plugins.bat to enable them." \
		> $(DESTDIR)$(WINDOWS_PREFIX)/$(notdir $(DIST_DIR))/README.txt
	$(verbose) $(UNIX_TO_DOS) $(DESTDIR)$(WINDOWS_PREFIX)/plugins/README.txt

install-windows-escripts:
	$(verbose) $(MAKE) -C $(DEPS_DIR)/rabbitmq_cli install \
		PREFIX="$(WINDOWS_PREFIX)/$(CLI_ESCRIPTS_DIR)"

install-windows-scripts: install-windows-escripts
	$(verbose) mkdir -p $(DESTDIR)$(WINDOWS_PREFIX)/sbin
	$(inst_verbose) for script in $(WINDOWS_SCRIPTS); do \
		cp "$(DEPS_DIR)/rabbit/scripts/$$script" \
			"$(DESTDIR)$(WINDOWS_PREFIX)/sbin"; \
		chmod 0755 "$(DESTDIR)$(WINDOWS_PREFIX)/sbin/$$script"; \
	done

install-windows-docs: install-windows-erlapp
	$(verbose) mkdir -p $(DESTDIR)$(WINDOWS_PREFIX)/etc
	$(inst_verbose) man $(DEPS_DIR)/rabbit/docs/rabbitmq-service.8 > tmp-readme-service.txt
	$(verbose) col -bx < ./tmp-readme-service.txt > $(DESTDIR)$(WINDOWS_PREFIX)/readme-service.txt
	$(verbose) rm -f ./tmp-readme-service.txt
	$(verbose) for file in \
	 $(DESTDIR)$(WINDOWS_PREFIX)/readme-service.txt \
	 $(DESTDIR)$(WINDOWS_PREFIX)/LICENSE* \
	 $(DESTDIR)$(WINDOWS_PREFIX)/INSTALL; do \
		$(UNIX_TO_DOS) "$$file"; \
		case "$$file" in \
		*.txt) ;; \
		*.example) ;; \
		*) mv "$$file" "$$file.txt" ;; \
		esac; \
	done

INTERNAL_DEPS := \
	   amqp10_client \
	   amqp10_common \
	   amqp_client \
	   oauth2_client \
	   rabbit_common \
	   rabbitmq_ct_client_helpers \
	   rabbitmq_ct_helpers \
	   rabbitmq_stream_common \
	   trust_store_http

TIER1_PLUGINS := \
	   rabbitmq_amqp_client \
	   rabbitmq_amqp1_0 \
	   rabbitmq_auth_backend_cache \
	   rabbitmq_auth_backend_http \
	   rabbitmq_auth_backend_internal_loopback \
	   rabbitmq_auth_backend_ldap \
	   rabbitmq_auth_backend_oauth2 \
	   rabbitmq_auth_mechanism_ssl \
	   rabbitmq_aws \
	   rabbitmq_consistent_hash_exchange \
	   rabbitmq_event_exchange \
	   rabbitmq_federation \
	   rabbitmq_federation_management \
	   rabbitmq_federation_prometheus \
	   rabbitmq_jms_topic_exchange \
	   rabbitmq_management \
	   rabbitmq_management_agent \
	   rabbitmq_mqtt \
	   rabbitmq_peer_discovery_aws \
	   rabbitmq_peer_discovery_common \
	   rabbitmq_peer_discovery_consul \
	   rabbitmq_peer_discovery_etcd \
	   rabbitmq_peer_discovery_k8s \
	   rabbitmq_prelaunch \
	   rabbitmq_prometheus \
	   rabbitmq_random_exchange \
	   rabbitmq_recent_history_exchange \
	   rabbitmq_sharding \
	   rabbitmq_shovel \
	   rabbitmq_shovel_management \
	   rabbitmq_shovel_prometheus \
	   rabbitmq_stomp \
	   rabbitmq_stream \
	   rabbitmq_stream_management \
	   rabbitmq_top \
	   rabbitmq_tracing \
	   rabbitmq_trust_store \
	   rabbitmq_web_dispatch \
	   rabbitmq_web_mqtt \
	   rabbitmq_web_mqtt_examples \
	   rabbitmq_web_stomp \
	   rabbitmq_web_stomp_examples

YTT ?= ytt

actions-workflows: .github/workflows/test.yaml .github/workflows/test-mixed-versions.yaml

.PHONY: .github/workflows/test.yaml .github/workflows/test-mixed-versions.yaml

.github/workflows/test.yaml: .github/workflows/templates/test.template.yaml
	$(gen_verbose) $(YTT) \
		--file $< \
		--data-value-yaml internal_deps=[$(subst $(space),$(comma),$(foreach s,$(INTERNAL_DEPS),"$s"))] \
		--data-value-yaml tier1_plugins=[$(subst $(space),$(comma),$(foreach s,$(TIER1_PLUGINS),"$s"))] \
		| sed 's/^true:/on:/' \
		| sed 's/pull_request: null/pull_request:/'> $@

.github/workflows/test-mixed-versions.yaml: .github/workflows/templates/test-mixed-versions.template.yaml
	$(gen_verbose) $(YTT) \
		--file $< \
		--data-value-yaml internal_deps=[$(subst $(space),$(comma),$(foreach s,$(INTERNAL_DEPS),"$s"))] \
		--data-value-yaml tier1_plugins=[$(subst $(space),$(comma),$(foreach s,$(TIER1_PLUGINS),"$s"))] \
		| sed 's/^true:/on:/' \
		| sed 's/pull_request: null/pull_request:/'> $@
