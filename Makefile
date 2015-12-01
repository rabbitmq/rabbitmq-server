PROJECT = rabbit
VERSION ?= $(call get_app_version,src/$(PROJECT).app.src)

# Release artifacts are put in $(PACKAGES_DIR).
PACKAGES_DIR ?= $(abspath PACKAGES)

DEPS = ranch $(PLUGINS)

define usage_xml_to_erl
$(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, src/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

DOCS_DIR     = docs
MANPAGES     = $(patsubst %.xml, %, $(wildcard $(DOCS_DIR)/*.[0-9].xml))
WEB_MANPAGES = $(patsubst %.xml, %.man.xml, $(wildcard $(DOCS_DIR)/*.[0-9].xml) $(DOCS_DIR)/rabbitmq-service.xml $(DOCS_DIR)/rabbitmq-echopid.xml)
USAGES_XML   = $(DOCS_DIR)/rabbitmqctl.1.xml $(DOCS_DIR)/rabbitmq-plugins.1.xml
USAGES_ERL   = $(foreach XML, $(USAGES_XML), $(call usage_xml_to_erl, $(XML)))

EXTRA_SOURCES += $(USAGES_ERL)

.DEFAULT_GOAL = all
$(PROJECT).d:: $(EXTRA_SOURCES)

DEP_PLUGINS = rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-dist.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk

# List of plugins to include in a RabbitMQ release.
DISTRIBUTED_DEPS := rabbitmq_amqp1_0 \
		    rabbitmq_auth_backend_ldap \
		    rabbitmq_auth_mechanism_ssl \
		    rabbitmq_consistent_hash_exchange \
		    rabbitmq_federation \
		    rabbitmq_federation_management \
		    rabbitmq_management \
		    rabbitmq_management_agent \
		    rabbitmq_management_visualiser \
		    rabbitmq_mqtt \
		    rabbitmq_shovel \
		    rabbitmq_shovel_management \
		    rabbitmq_stomp \
		    rabbitmq_tracing \
		    rabbitmq_web_dispatch \
		    rabbitmq_web_stomp \
		    rabbitmq_web_stomp_examples

ifneq ($(IS_DEP),1)
ifneq ($(filter source-dist packages package-%,$(MAKECMDGOALS)),)
DEPS += $(DISTRIBUTED_DEPS)
endif
ifneq ($(wildcard git-revisions.txt),)
DEPS += $(DISTRIBUTED_DEPS)
endif
endif

include erlang.mk

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

RMQ_ERLC_OPTS += -I $(DEPS_DIR)/rabbit_common/include

ifdef INSTRUMENT_FOR_QC
RMQ_ERLC_OPTS += -DINSTR_MOD=gm_qc
else
RMQ_ERLC_OPTS += -DINSTR_MOD=gm
endif

ifdef CREDIT_FLOW_TRACING
RMQ_ERLC_OPTS += -DCREDIT_FLOW_TRACING=true
endif

ERTS_VER := $(shell erl -version 2>&1 | sed -E 's/.* version //')
USE_SPECS_MIN_ERTS_VER = 5.11
ifeq ($(call compare_version,$(ERTS_VER),$(USE_SPECS_MIN_ERTS_VER),>=),true)
RMQ_ERLC_OPTS += -Duse_specs
endif

ifndef USE_PROPER_QC
# PropEr needs to be installed for property checking
# http://proper.softlab.ntua.gr/
USE_PROPER_QC := $(shell $(ERL) -eval 'io:format({module, proper} =:= code:ensure_loaded(proper)), halt().')
RMQ_ERLC_OPTS += $(if $(filter true,$(USE_PROPER_QC)),-Duse_proper_qc)
endif

ERLC_OPTS += $(RMQ_ERLC_OPTS)

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)

# --------------------------------------------------------------------
# Tests.
# --------------------------------------------------------------------

TARGETS_IN_RABBITMQ_TEST = $(patsubst %,%-in-rabbitmq_test,\
			   tests full unit lite conformance16 lazy-vq-tests)

.PHONY: $(TARGETS_IN_RABBITMQ_TEST)

TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)

tests:: tests-in-rabbitmq_test

$(TARGETS_IN_RABBITMQ_TEST): $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
    test-build $(DEPS_DIR)/rabbitmq_test
	$(MAKE) -C $(DEPS_DIR)/rabbitmq_test \
		IS_DEP=1 \
		RABBITMQ_BROKER_DIR=$(RABBITMQ_BROKER_DIR) \
		$(patsubst %-in-rabbitmq_test,%,$@)

# --------------------------------------------------------------------
# Documentation.
# --------------------------------------------------------------------

# xmlto can not read from standard input, so we mess with a tmp file.
%: %.xml $(DOCS_DIR)/examples-to-end.xsl
	$(gen_verbose) xmlto --version | \
	    grep -E '^xmlto version 0\.0\.([0-9]|1[1-8])$$' >/dev/null || \
	    opt='--stringparam man.indent.verbatims=0' ; \
	xsltproc --novalid $(DOCS_DIR)/examples-to-end.xsl $< > $<.tmp && \
	(xmlto -o $(DOCS_DIR) $$opt man $< 2>&1 | (grep -qv '^Note: Writing' || :)) && \
	rm $<.tmp

# Use tmp files rather than a pipeline so that we get meaningful errors
# Do not fold the cp into previous line, it's there to stop the file being
# generated but empty if we fail
define usage_dep
$(call usage_xml_to_erl, $(1)):: $(1) $(DOCS_DIR)/usage.xsl
	$$(gen_verbose) xsltproc --novalid --stringparam modulename "`basename $$@ .erl`" \
	    $(DOCS_DIR)/usage.xsl $$< > $$@.tmp && \
	sed -e 's/"/\\"/g' -e 's/%QUOTE%/"/g' $$@.tmp > $$@.tmp2 && \
	fold -s $$@.tmp2 > $$@.tmp3 && \
	mv $$@.tmp3 $$@ && \
	rm $$@.tmp $$@.tmp2
endef

$(foreach XML,$(USAGES_XML),$(eval $(call usage_dep, $(XML))))

# We rename the file before xmlto sees it since xmlto will use the name of
# the file to make internal links.
%.man.xml: %.xml $(DOCS_DIR)/html-to-website-xml.xsl
	$(gen_verbose) cp $< `basename $< .xml`.xml && \
	    xmlto xhtml-nochunks `basename $< .xml`.xml ; \
	rm `basename $< .xml`.xml && \
	cat `basename $< .xml`.html | \
	    xsltproc --novalid $(DOCS_DIR)/remove-namespaces.xsl - | \
	      xsltproc --novalid --stringparam original `basename $<` $(DOCS_DIR)/html-to-website-xml.xsl - | \
	      xmllint --format - > $@ && \
	rm `basename $< .xml`.html

.PHONY: manpages web-manpages distclean-manpages

docs:: manpages web-manpages

manpages: $(MANPAGES)
	@:

web-manpages: $(WEB_MANPAGES)
	@:

distclean:: distclean-manpages

distclean-manpages::
	$(gen_verbose) rm -f $(MANPAGES) $(WEB_MANPAGES)

# --------------------------------------------------------------------
# Distribution.
# --------------------------------------------------------------------

.PHONY: source-dist clean-source-dist

SOURCE_DIST_BASE ?= rabbitmq-server
SOURCE_DIST_SUFFIXES ?= tar.xz zip
SOURCE_DIST ?= $(PACKAGES_DIR)/$(SOURCE_DIST_BASE)-$(VERSION)

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
	       --exclude '*.pyc'			\
	       --exclude '.git*'			\
	       --exclude '.hg*'				\
	       --exclude '.travis.yml'			\
	       --exclude '.*.plt'			\
	       --exclude '$(notdir $(ERLANG_MK_TMP))'	\
	       --exclude 'ebin'				\
	       --exclude 'packaging'			\
	       --exclude 'erl_crash.dump'		\
	       --exclude 'MnesiaCore.*'			\
	       --exclude 'cover/'			\
	       --exclude 'deps/'			\
	       --exclude '$(notdir $(DEPS_DIR))/'	\
	       --exclude 'plugins/'			\
	       --exclude '$(notdir $(DIST_DIR))/'	\
	       --exclude '/$(notdir $(PACKAGES_DIR))/'	\
	       --exclude '/cowboy/doc/'			\
	       --exclude '/cowboy/examples/'		\
	       --exclude '/rabbitmq_amqp1_0/test/swiftmq/build/'\
	       --exclude '/rabbitmq_amqp1_0/test/swiftmq/swiftmq*'\
	       --exclude '/rabbitmq_mqtt/test/build/'	\
	       --exclude '/rabbitmq_mqtt/test/test_client/'\
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

$(SOURCE_DIST): $(ERLANG_MK_RECURSIVE_DEPS_LIST)
	$(verbose) mkdir -p $(dir $@)
	$(gen_verbose) $(RSYNC) $(RSYNC_FLAGS) ./ $@/
	$(verbose) sed -E -i.bak \
		-e 's/[{]vsn[[:blank:]]*,[^}]+}/{vsn, "$(VERSION)"}/' \
		$@/src/$(PROJECT).app.src && \
		rm $@/src/$(PROJECT).app.src.bak
	$(verbose) cat packaging/common/LICENSE.head > $@/LICENSE
	$(verbose) mkdir -p $@/deps/licensing
	$(verbose) for dep in $$(cat $(ERLANG_MK_RECURSIVE_DEPS_LIST) | grep -v '/$(PROJECT)$$' | LC_COLLATE=C sort); do \
		$(RSYNC) $(RSYNC_FLAGS) \
		 $$dep \
		 $@/deps; \
		if test -f $@/deps/$$(basename $$dep)/erlang.mk && \
		   test "$$(wc -l $@/deps/$$(basename $$dep)/erlang.mk | awk '{print $$1;}')" = "1" && \
		   grep -qs -E "^[[:blank:]]*include[[:blank:]]+(erlang\.mk|.*/erlang\.mk)$$" $@/deps/$$(basename $$dep)/erlang.mk; then \
			echo "include ../../erlang.mk" > $@/deps/$$(basename $$dep)/erlang.mk; \
		fi; \
		if test -f "$$dep/license_info"; then \
			cp "$$dep/license_info" "$@/deps/licensing/license_info_$$(basename "$$dep")"; \
			cat "$$dep/license_info" >> $@/LICENSE; \
		fi; \
		find "$$dep" -maxdepth 1 -name 'LICENSE-*' -exec cp '{}' $@/deps/licensing \; ; \
	done
	$(verbose) cat packaging/common/LICENSE.tail >> $@/LICENSE
	$(verbose) find $@/deps/licensing -name 'LICENSE-*' -exec cp '{}' $@ \;
	$(verbose) for file in $$(find $@ -name '*.app.src'); do \
		sed -E -i.bak -e 's/[{]vsn[[:blank:]]*,[[:blank:]]*""[[:blank:]]*}/{vsn, "$(VERSION)"}/' $$file; \
		rm $$file.bak; \
	done
	$(verbose) echo "$(PROJECT) $$(git rev-parse HEAD) $$(git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)" > $@/git-revisions.txt
	$(verbose) for dep in $$(cat $(ERLANG_MK_RECURSIVE_DEPS_LIST)); do \
		(cd $$dep; echo "$$(basename "$$dep") $$(git rev-parse HEAD) $$(git describe --tags --exact-match 2>/dev/null || git symbolic-ref -q --short HEAD)") >> $@/git-revisions.txt; \
	done

# TODO: Fix file timestamps to have reproducible source archives.
# $(verbose) find $@ -not -name 'git-revisions.txt' -print0 | xargs -0 touch -r $@/git-revisions.txt

$(SOURCE_DIST).tar.gz: $(SOURCE_DIST)
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		find $(notdir $(SOURCE_DIST)) -print0 | LC_COLLATE=C sort -z | \
		xargs -0 $(TAR) $(TAR_V) --no-recursion -cf - | \
		$(GZIP) --best > $@

$(SOURCE_DIST).tar.bz2: $(SOURCE_DIST)
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		find $(notdir $(SOURCE_DIST)) -print0 | LC_COLLATE=C sort -z | \
		xargs -0 $(TAR) $(TAR_V) --no-recursion -cf - | \
		$(BZIP2) > $@

$(SOURCE_DIST).tar.xz: $(SOURCE_DIST)
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		find $(notdir $(SOURCE_DIST)) -print0 | LC_COLLATE=C sort -z | \
		xargs -0 $(TAR) $(TAR_V) --no-recursion -cf - | \
		$(XZ) > $@

$(SOURCE_DIST).zip: $(SOURCE_DIST)
	$(verbose) rm -f $@
	$(gen_verbose) cd $(dir $(SOURCE_DIST)) && \
		find $(notdir $(SOURCE_DIST)) -print0 | LC_COLLATE=C sort -z | \
		xargs -0 $(ZIP) $(ZIP_V) $@

clean:: clean-source-dist

clean-source-dist:
	$(gen_verbose) rm -rf -- $(SOURCE_DIST_BASE)-*

# --------------------------------------------------------------------
# Installation.
# --------------------------------------------------------------------

.PHONY: install install-erlapp install-scripts install-bin install-man
.PHONY: install-windows install-windows-erlapp install-windows-scripts install-windows-docs

DESTDIR ?=

PREFIX ?= /usr/local
WINDOWS_PREFIX ?= rabbitmq-server-windows-$(VERSION)

MANDIR ?= $(PREFIX)/share/man
RMQ_ROOTDIR ?= $(PREFIX)/lib/erlang
RMQ_BINDIR ?= $(RMQ_ROOTDIR)/bin
RMQ_LIBDIR ?= $(RMQ_ROOTDIR)/lib
RMQ_ERLAPP_DIR ?= $(RMQ_LIBDIR)/rabbitmq_server-$(VERSION)

SCRIPTS = rabbitmq-defaults \
	  rabbitmq-env \
	  rabbitmq-server \
	  rabbitmqctl \
	  rabbitmq-plugins

WINDOWS_SCRIPTS = rabbitmq-defaults.bat \
		  rabbitmq-echopid.bat \
		  rabbitmq-env.bat \
		  rabbitmq-plugins.bat \
		  rabbitmq-server.bat \
		  rabbitmq-service.bat \
		  rabbitmqctl.bat

UNIX_TO_DOS ?= todos

inst_verbose_0 = @echo " INST  " $@;
inst_verbose = $(inst_verbose_$(V))

install: install-erlapp install-scripts

install-erlapp: dist
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_ERLAPP_DIR)
	$(inst_verbose) cp -r include ebin plugins LICENSE* INSTALL \
		$(DESTDIR)$(RMQ_ERLAPP_DIR)
	$(verbose) echo "Put your EZs here and use rabbitmq-plugins to enable them." \
		> $(DESTDIR)$(RMQ_ERLAPP_DIR)/plugins/README

	@# rabbitmq-common provides headers too: copy them to
	@# rabbitmq_server/include.
	$(verbose) cp -r $(DEPS_DIR)/rabbit_common/include $(DESTDIR)$(RMQ_ERLAPP_DIR)

install-scripts:
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_ERLAPP_DIR)/sbin
	$(inst_verbose) for script in $(SCRIPTS); do \
		cp "scripts/$$script" "$(DESTDIR)$(RMQ_ERLAPP_DIR)/sbin"; \
		chmod 0755 "$(DESTDIR)$(RMQ_ERLAPP_DIR)/sbin/$$script"; \
	done

# FIXME: We do symlinks to scripts in $(RMQ_ERLAPP_DIR))/sbin but this
# code assumes a certain hierarchy to make relative symlinks.
install-bin: install-scripts
	$(verbose) mkdir -p $(DESTDIR)$(RMQ_BINDIR)
	$(inst_verbose) for script in $(SCRIPTS); do \
		test -e $(DESTDIR)$(RMQ_BINDIR)/$$script || \
			ln -sf ../lib/$(notdir $(RMQ_ERLAPP_DIR))/sbin/$$script \
			 $(DESTDIR)$(RMQ_BINDIR)/$$script; \
	done

install-man: manpages
	$(inst_verbose) sections=$$(ls -1 docs/*.[1-9] \
		| sed -E 's/.*\.([1-9])$$/\1/' | uniq | sort); \
	for section in $$sections; do \
                mkdir -p $(DESTDIR)$(MANDIR)/man$$section; \
                for manpage in $(DOCS_DIR)/*.$$section; do \
                        gzip < $$manpage \
			 > $(DESTDIR)$(MANDIR)/man$$section/$$(basename $$manpage).gz; \
                done; \
        done

install-windows: install-windows-erlapp install-windows-scripts install-windows-docs

install-windows-erlapp: dist
	$(verbose) mkdir -p $(DESTDIR)$(WINDOWS_PREFIX)
	$(inst_verbose) cp -r include ebin plugins LICENSE* INSTALL \
		$(DESTDIR)$(WINDOWS_PREFIX)
	$(verbose) echo "Put your EZs here and use rabbitmq-plugins.bat to enable them." \
		> $(DESTDIR)$(WINDOWS_PREFIX)/plugins/README.txt
	$(verbose) $(UNIX_TO_DOS) $(DESTDIR)$(WINDOWS_PREFIX)/plugins/README.txt

# rabbitmq-common provides headers too: copy them to
# rabbitmq_server/include.
	$(verbose) cp -r $(DEPS_DIR)/rabbit_common/include $(DESTDIR)$(WINDOWS_PREFIX)

install-windows-scripts:
	$(verbose) mkdir -p $(DESTDIR)$(WINDOWS_PREFIX)/sbin
	$(inst_verbose) for script in $(WINDOWS_SCRIPTS); do \
		cp "scripts/$$script" "$(DESTDIR)$(WINDOWS_PREFIX)/sbin"; \
		chmod 0755 "$(DESTDIR)$(WINDOWS_PREFIX)/sbin/$$script"; \
	done

install-windows-docs: install-windows-erlapp
	$(verbose) mkdir -p $(DESTDIR)$(WINDOWS_PREFIX)/etc
	$(inst_verbose) xmlto -o . xhtml-nochunks docs/rabbitmq-service.xml
	$(verbose) elinks -dump -no-references -no-numbering rabbitmq-service.html \
		> $(DESTDIR)$(WINDOWS_PREFIX)/readme-service.txt
	$(verbose) rm rabbitmq-service.html
	$(verbose) cp docs/rabbitmq.config.example $(DESTDIR)$(WINDOWS_PREFIX)/etc
	$(verbose) for file in $(DESTDIR)$(WINDOWS_PREFIX)/readme-service.txt \
	 $(DESTDIR)$(WINDOWS_PREFIX)/LICENSE* $(DESTDIR)$(WINDOWS_PREFIX)/INSTALL \
	 $(DESTDIR)$(WINDOWS_PREFIX)/etc/rabbitmq.config.example; do \
		$(UNIX_TO_DOS) "$$file"; \
		case "$$file" in \
		*.txt) ;; \
		*.example) ;; \
		*) mv "$$file" "$$file.txt" ;; \
		esac; \
	done

# --------------------------------------------------------------------
# Packaging.
# --------------------------------------------------------------------

.PHONY: packages package-deb \
	package-rpm package-rpm-fedora package-rpm-suse \
	package-windows package-standalone-macosx \
	package-generic-unix

# This variable is exported so sub-make instances know where to find the
# archive.
PACKAGES_SOURCE_DIST_FILE ?= $(firstword $(SOURCE_DIST_FILES))

packages package-deb package-rpm package-rpm-fedora \
package-rpm-suse package-windows package-standalone-macosx \
package-generic-unix: $(PACKAGES_SOURCE_DIST_FILE)
	$(verbose) $(MAKE) -C packaging $@ \
		SOURCE_DIST_FILE=$(abspath $(PACKAGES_SOURCE_DIST_FILE))
