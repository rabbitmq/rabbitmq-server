PROJECT = rabbitmq_server_release
PROJECT_DESCRIPTION = RabbitMQ Server

# Propagate PROJECT_VERSION (from the command line or environment) to
# other components. If PROJECT_VERSION is unset, then an empty variable
# is propagated and the default version will fallback to the default
# value from rabbitmq-components.mk.
export RABBITMQ_VERSION = $(PROJECT_VERSION)

# Release artifacts are put in $(PACKAGES_DIR).
PACKAGES_DIR ?= $(abspath PACKAGES)

# List of plugins to include in a RabbitMQ release.
include plugins.mk

DEPS = rabbit_common rabbit $(PLUGINS)

DEP_PLUGINS = rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
include mk/github-actions.mk
include mk/topic-branches.mk

# --------------------------------------------------------------------
# Distribution.
# --------------------------------------------------------------------

.PHONY: source-dist clean-source-dist

SOURCE_DIST_BASE ?= rabbitmq-server
SOURCE_DIST_SUFFIXES ?= tar.xz
SOURCE_DIST ?= $(PACKAGES_DIR)/$(SOURCE_DIST_BASE)-$(PROJECT_VERSION)

# The first source distribution file is used by packages: if the archive
# type changes, you must update all packages' Makefile.
SOURCE_DIST_FILES = $(addprefix $(SOURCE_DIST).,$(SOURCE_DIST_SUFFIXES))

.PHONY: $(SOURCE_DIST_FILES)

source-dist: $(SOURCE_DIST_FILES)
	@:

RSYNC ?= rsync
RSYNC_V_0 =
RSYNC_V_1 = -v
RSYNC_V_2 = -v
RSYNC_V = $(RSYNC_V_$(V))
RSYNC_FLAGS += -a $(RSYNC_V)		\
	       --exclude '.sw?' --exclude '.*.sw?'	\
	       --exclude '*.beam'			\
	       --exclude '*.d'				\
	       --exclude '*.pyc'			\
	       --exclude '.git*'			\
	       --exclude '.hg*'				\
	       --exclude '.travis.yml'			\
	       --exclude '.*.plt'			\
	       --exclude '$(notdir $(ERLANG_MK_TMP))'	\
	       --exclude '_build/'			\
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
	       --exclude 'packaging'			\
	       --exclude '/plugins/'			\
	       --include 'cli/plugins'			\
	       --exclude '$(notdir $(DIST_DIR))/'	\
	       --exclude 'test'				\
	       --exclude 'xrefr'			\
	       --exclude '/$(notdir $(PACKAGES_DIR))/'	\
	       --exclude '/PACKAGES/'			\
	       --exclude '/amqp_client/doc/'		\
	       --exclude '/amqp_client/rebar.config'	\
	       --exclude '/cowboy/doc/'			\
	       --exclude '/cowboy/examples/'		\
	       --exclude '/rabbit/escript/'		\
	       --exclude '/rabbitmq_amqp1_0/test/swiftmq/build/'\
	       --exclude '/rabbitmq_amqp1_0/test/swiftmq/swiftmq*'\
	       --exclude '/rabbitmq_cli/escript/'	\
	       --exclude '/rabbitmq_mqtt/test/build/'	\
	       --exclude '/rabbitmq_mqtt/test/test_client/'\
	       --exclude '/ranch/doc/'			\
	       --exclude '/ranch/examples/'		\
	       --exclude '/sockjs/examples/'		\
	       --delete					\
	       --delete-excluded

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

.PHONY: $(SOURCE_DIST)
.PHONY: clean-source-dist distclean-packages clean-unpacked-source-dist

$(SOURCE_DIST): $(ERLANG_MK_RECURSIVE_DEPS_LIST)
	$(verbose) mkdir -p $(dir $@)
	$(gen_verbose) $(RSYNC) $(RSYNC_FLAGS) ./ $@/
	$(verbose) echo "$(PROJECT_DESCRIPTION) $(PROJECT_VERSION)" > $@/git-revisions.txt
	$(verbose) echo "$(PROJECT) $$(git rev-parse HEAD) $$(git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)" >> $@/git-revisions.txt
	$(verbose) cat packaging/common/LICENSE.head > $@/LICENSE
	$(verbose) mkdir -p $@/deps/licensing
	$(verbose) set -e; for dep in $$(cat $(ERLANG_MK_RECURSIVE_DEPS_LIST) | LC_COLLATE=C sort); do \
		$(RSYNC) $(RSYNC_FLAGS) \
		 $$dep \
		 $@/deps; \
		rm -f \
		 $@/deps/rabbit_common/rebar.config \
		 $@/deps/rabbit_common/rebar.lock; \
		if test -f $@/deps/$$(basename $$dep)/erlang.mk && \
		   test "$$(wc -l $@/deps/$$(basename $$dep)/erlang.mk | awk '{print $$1;}')" = "1" && \
		   grep -qs -E "^[[:blank:]]*include[[:blank:]]+(erlang\.mk|.*/erlang\.mk)$$" $@/deps/$$(basename $$dep)/erlang.mk; then \
			echo "include ../../erlang.mk" > $@/deps/$$(basename $$dep)/erlang.mk; \
		fi; \
		sed -E -i.bak "s|^[[:blank:]]*include[[:blank:]]+\.\./.*erlang.mk$$|include ../../erlang.mk|" \
		 $@/deps/$$(basename $$dep)/Makefile && \
		rm $@/deps/$$(basename $$dep)/Makefile.bak; \
		mix_exs=$@/deps/$$(basename $$dep)/mix.exs; \
		if test -f $$mix_exs; then \
			(cd $$(dirname "$$mix_exs") && \
			 env DEPS_DIR=$@/deps HOME=$@/deps MIX_ENV=prod FILL_HEX_CACHE=yes mix local.hex --force && \
			 env DEPS_DIR=$@/deps HOME=$@/deps MIX_ENV=prod FILL_HEX_CACHE=yes mix deps.get --only prod && \
			 cp $(CURDIR)/mk/rabbitmq-mix.mk . && \
			 rm -rf _build deps); \
		fi; \
		if test -f "$$dep/license_info"; then \
			cp "$$dep/license_info" "$@/deps/licensing/license_info_$$(basename "$$dep")"; \
			cat "$$dep/license_info" >> $@/LICENSE; \
		fi; \
		find "$$dep" -maxdepth 1 -name 'LICENSE-*' -exec cp '{}' $@/deps/licensing \; ; \
		(cd $$dep; \
		 echo "$$(basename "$$dep") $$(git rev-parse HEAD) $$(git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)") \
		 >> $@/git-revisions.txt; \
	done
	$(verbose) cat packaging/common/LICENSE.tail >> $@/LICENSE
	$(verbose) find $@/deps/licensing -name 'LICENSE-*' -exec cp '{}' $@ \;
	$(verbose) for file in $$(find $@ -name '*.app.src'); do \
		sed -E -i.bak \
		  -e 's/[{]vsn[[:blank:]]*,[[:blank:]]*(""|"0.0.0")[[:blank:]]*}/{vsn, "$(PROJECT_VERSION)"}/' \
		  -e 's/[{]broker_version_requirements[[:blank:]]*,[[:blank:]]*\[\][[:blank:]]*}/{broker_version_requirements, ["$(PROJECT_VERSION)"]}/' \
		  $$file; \
		rm $$file.bak; \
	done
	$(verbose) echo "PLUGINS := $(PLUGINS)" > $@/plugins.mk

$(SOURCE_DIST).manifest: $(SOURCE_DIST)
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		find $(notdir $(SOURCE_DIST)) | LC_COLLATE=C sort > $@

# TODO: Fix file timestamps to have reproducible source archives.
# $(verbose) find $@ -not -name 'git-revisions.txt' -print0 | xargs -0 touch -r $@/git-revisions.txt

$(SOURCE_DIST).tar.gz: $(SOURCE_DIST).manifest
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		$(TAR) $(TAR_V) --no-recursion -T $(SOURCE_DIST).manifest -cf - | \
		$(GZIP) --best > $@

$(SOURCE_DIST).tar.bz2: $(SOURCE_DIST).manifest
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		$(TAR) $(TAR_V) --no-recursion -T $(SOURCE_DIST).manifest -cf - | \
		$(BZIP2) > $@

$(SOURCE_DIST).tar.xz: $(SOURCE_DIST).manifest
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		$(TAR) $(TAR_V) --no-recursion -T $(SOURCE_DIST).manifest -cf - | \
		$(XZ) > $@

$(SOURCE_DIST).zip: $(SOURCE_DIST).manifest
	$(verbose) rm -f $@
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		$(ZIP) $(ZIP_V) --names-stdin $@ < $(SOURCE_DIST).manifest

clean:: clean-source-dist

clean-source-dist:
	$(gen_verbose) rm -rf -- $(SOURCE_DIST_BASE)-*

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

# --------------------------------------------------------------------
# Packaging.
# --------------------------------------------------------------------

.PHONY: packages package-deb \
	package-rpm package-rpm-fedora package-rpm-suse \
	package-windows package-standalone-macosx \
	package-standalone-linux-x86_64 \
	package-standalone-freebsd-x86_64 \
	package-generic-unix

# This variable is exported so sub-make instances know where to find the
# archive.
PACKAGES_SOURCE_DIST_FILE ?= $(firstword $(SOURCE_DIST_FILES))

packages package-deb package-rpm \
package-rpm-redhat package-rpm-fedora package-rpm-rhel6 package-rpm-rhel7 \
package-rpm-rhel8 package-rpm-suse package-rpm-opensuse package-rpm-sles11 \
package-windows package-standalone-macosx \
package-standalone-linux-x86_64 \
package-standalone-freebsd-x86_64 \
package-generic-unix: $(PACKAGES_SOURCE_DIST_FILE)
	$(verbose) $(MAKE) -C packaging $@ \
		SOURCE_DIST_FILE=$(abspath $(PACKAGES_SOURCE_DIST_FILE))

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
	  cuttlefish

AUTOCOMPLETE_SCRIPTS = bash_autocomplete.sh zsh_autocomplete.sh

WINDOWS_SCRIPTS = rabbitmq-defaults.bat \
		  rabbitmq-echopid.bat \
		  rabbitmq-env.bat \
		  rabbitmq-plugins.bat \
		  rabbitmq-diagnostics.bat \
		  rabbitmq-server.bat \
		  rabbitmq-service.bat \
		  rabbitmqctl.bat \
		  cuttlefish

UNIX_TO_DOS ?= todos

inst_verbose_0 = @echo " INST  " $@;
inst_verbose = $(inst_verbose_$(V))

install: install-erlapp install-scripts

install-erlapp: dist
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_ERLAPP_DIR)
	$(inst_verbose) cp -r \
		LICENSE* \
		$(DEPS_DIR)/rabbit/ebin \
		$(DEPS_DIR)/rabbit/priv \
		$(DEPS_DIR)/rabbit/INSTALL \
		$(DIST_DIR) \
		$(DESTDIR)$(RMQ_ERLAPP_DIR)
	$(verbose) echo "Put your EZs here and use rabbitmq-plugins to enable them." \
		> $(DESTDIR)$(RMQ_ERLAPP_DIR)/$(notdir $(DIST_DIR))/README

	@# FIXME: Why do we copy headers?
	$(verbose) cp -r \
		$(DEPS_DIR)/rabbit/include \
		$(DESTDIR)$(RMQ_ERLAPP_DIR)
	@# rabbitmq-common provides headers too: copy them to
	@# rabbitmq_server/include.
	$(verbose) cp -r \
		$(DEPS_DIR)/rabbit_common/include \
		$(DESTDIR)$(RMQ_ERLAPP_DIR)

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
		$(DEPS_DIR)/rabbit/ebin \
		$(DEPS_DIR)/rabbit/priv \
		$(DEPS_DIR)/rabbit/INSTALL \
		$(DIST_DIR) \
		$(DESTDIR)$(WINDOWS_PREFIX)
	$(verbose) echo "Put your EZs here and use rabbitmq-plugins.bat to enable them." \
		> $(DESTDIR)$(WINDOWS_PREFIX)/$(notdir $(DIST_DIR))/README.txt
	$(verbose) $(UNIX_TO_DOS) $(DESTDIR)$(WINDOWS_PREFIX)/plugins/README.txt

	@# FIXME: Why do we copy headers?
	$(verbose) cp -r \
		$(DEPS_DIR)/rabbit/include \
		$(DESTDIR)$(WINDOWS_PREFIX)
	@# rabbitmq-common provides headers too: copy them to
	@# rabbitmq_server/include.
	$(verbose) cp -r \
		$(DEPS_DIR)/rabbit_common/include \
		$(DESTDIR)$(WINDOWS_PREFIX)

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
	$(verbose) cp $(DEPS_DIR)/rabbit/docs/rabbitmq.config.example \
		$(DESTDIR)$(WINDOWS_PREFIX)/etc
	$(verbose) for file in \
	 $(DESTDIR)$(WINDOWS_PREFIX)/readme-service.txt \
	 $(DESTDIR)$(WINDOWS_PREFIX)/LICENSE* \
	 $(DESTDIR)$(WINDOWS_PREFIX)/INSTALL \
	 $(DESTDIR)$(WINDOWS_PREFIX)/etc/rabbitmq.config.example; do \
		$(UNIX_TO_DOS) "$$file"; \
		case "$$file" in \
		*.txt) ;; \
		*.example) ;; \
		*) mv "$$file" "$$file.txt" ;; \
		esac; \
	done
