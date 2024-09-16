.PHONY: dist test-dist do-dist cli-scripts cli-escripts clean-dist

DIST_DIR = plugins
CLI_SCRIPTS_DIR = sbin
CLI_ESCRIPTS_DIR = escript
MIX = echo y | mix

# Set $(DIST_AS_EZS) to a non-empty value to enable the packaging of
# plugins as .ez archives.
ifeq ($(USE_RABBIT_BOOT_SCRIPT),)
DIST_AS_EZS ?=
else
DIST_AS_EZS =
endif

dist_verbose_0 = @echo " DIST  " $@;
dist_verbose_2 = set -x;
dist_verbose = $(dist_verbose_$(V))

# We take the version of an Erlang application from the .app file. This
# macro is called like this:
#
#   $(call get_app_version,/path/to/name.app.src)

ifeq ($(PLATFORM),msys2)
core_unix_path = $(shell cygpath $1)
else
core_unix_path = $1
endif

define get_app_version
$(shell awk '
/{ *vsn *, *"/ {
	vsn=$$0;
	sub(/.*{ *vsn, *"/, "", vsn);
	sub(/".*/, "", vsn);
	print vsn;
	exit;
}' $(1))
endef

# Define the target to create an .ez plugin archive for an
# Erlang.mk-based project. This macro is called like this:
#
#   $(call do_ez_target_erlangmk,app_name,app_version,app_dir)

define do_ez_target_erlangmk
dist_$(1)_ez_dir = $$(if $(2),$(DIST_DIR)/$(1)-$(2), \
	$$(if $$(VERSION),$(DIST_DIR)/$(1)-$$(VERSION),$(DIST_DIR)/$(1)))
ifeq ($(DIST_AS_EZS),)
dist_$(1)_ez = $$(dist_$(1)_ez_dir)
else
dist_$(1)_ez = $$(dist_$(1)_ez_dir).ez
endif

$$(dist_$(1)_ez): APP     = $(1)
$$(dist_$(1)_ez): VSN     = $(2)
$$(dist_$(1)_ez): SRC_DIR = $(3)
$$(dist_$(1)_ez): EZ_DIR  = $$(abspath $$(dist_$(1)_ez_dir))
$$(dist_$(1)_ez): EZ      = $$(dist_$(1)_ez)
$$(dist_$(1)_ez): $$(if $$(wildcard $(3)/ebin $(3)/include $(3)/priv),\
	$$(filter-out %/dep_built %/ebin/test,$$(call core_find,$$(wildcard $(3)/ebin $(3)/include $(3)/priv),*)),)

# If the application's Makefile defines a `list-dist-deps` target, we
# use it to populate the dependencies list. This is useful when the
# application has also a `prepare-dist` target to modify the created
# tree before we make an archive out of it.

ifeq ($$(shell test -f $(3)/rabbitmq-components.mk \
	&& grep -q '^list-dist-deps::' $(3)/Makefile && echo yes),yes)
$$(dist_$(1)_ez): $$(patsubst %,$(3)/%, \
	$$(shell $(MAKE) --no-print-directory -C $(3) list-dist-deps \
	APP=$(1) VSN=$(2) EZ_DIR=$$(abspath $$(dist_$(1)_ez_dir))))
endif

ERLANGMK_DIST_APPS += $(1)

ERLANGMK_DIST_EZS += $$(dist_$(1)_ez)

endef

# Real entry point: it tests the existence of an .app file to determine
# if it is an Erlang application (and therefore if it should be provided
# as an .ez plugin archive) and calls do_ez_target_erlangmk. If instead
# it finds a Mix configuration file, it is skipped, as the only elixir
# applications in the directory are used by rabbitmq_cli and compiled
# with it.
#
#   $(call ez_target,path_to_app)

define ez_target
dist_$(1)_appdir  = $(2)
dist_$(1)_appfile = $$(dist_$(1)_appdir)/ebin/$(1).app
dist_$(1)_mixfile = $$(dist_$(1)_appdir)/mix.exs

$$(if $$(shell test -f $$(dist_$(1)_appfile) && echo OK), \
  $$(eval $$(call do_ez_target_erlangmk,$(1),$$(call get_app_version,$$(dist_$(1)_appfile)),$$(dist_$(1)_appdir))))

endef

ifneq ($(filter do-dist,$(MAKECMDGOALS)),)
# The following code is evaluated only when running "make do-dist",
# otherwise it would trigger an infinite loop, as this code calls "make
# list-dist-deps" (see do_ez_target_erlangmk).
ifdef DIST_PLUGINS_LIST
# Now, try to create an .ez target for the top-level project and all
# dependencies.

ifeq ($(wildcard $(DIST_PLUGINS_LIST)),)
$(error DIST_PLUGINS_LIST ($(DIST_PLUGINS_LIST)) is missing)
endif

$(eval $(foreach path, \
  $(sort $(shell cat $(DIST_PLUGINS_LIST))) $(CURDIR), \
  $(call ez_target,$(if $(filter $(path),$(CURDIR)),$(PROJECT),$(notdir $(path))),$(path))))
endif
endif

# The actual recipe to create the .ez plugin archive. Some variables
# are defined in the do_ez_target_erlangmk and do_ez_target_mix macros
# above. All .ez archives are also listed in this do_ez_target_erlangmk
# and do_ez_target_mix macros.

RSYNC ?= rsync
RSYNC_V_0 =
RSYNC_V_1 = -v
RSYNC_V = $(RSYNC_V_$(V))

ZIP ?= zip
ZIP_V_0 = -q
ZIP_V_1 =
ZIP_V = $(ZIP_V_$(V))

$(ERLANGMK_DIST_EZS):
	$(verbose) rm -rf $(EZ_DIR) $(EZ)
	$(verbose) mkdir -p $(EZ_DIR)
	$(eval SRC_DIR_UNIX := $(call core_unix_path,$(SRC_DIR)))
	$(eval EZ_DIR_UNIX := $(call core_unix_path,$(EZ_DIR)))
	$(dist_verbose) cp -a $(SRC_DIR_UNIX)/ebin $(wildcard $(SRC_DIR_UNIX)/include) $(wildcard $(SRC_DIR_UNIX)/priv) $(EZ_DIR_UNIX)/
	$(verbose) rm -f $(EZ_DIR_UNIX)/ebin/dep_built $(EZ_DIR_UNIX)/ebin/test
	@# Give a chance to the application to make any modification it
	@# wants to the tree before we make an archive.
ifneq ($(RABBITMQ_COMPONENTS),)
ifneq ($(filter $(PROJECT),$(RABBITMQ_COMPONENTS)),)
	$(verbose) ! (grep -q '^prepare-dist::' $(SRC_DIR)/Makefile) || \
		$(MAKE) --no-print-directory -C $(SRC_DIR) prepare-dist \
		APP=$(APP) VSN=$(VSN) EZ_DIR=$(EZ_DIR)
endif
endif
ifneq ($(DIST_AS_EZS),)
	$(verbose) (cd $(DIST_DIR) && \
		find "$(basename $(notdir $@))" | LC_COLLATE=C sort \
		> "$(basename $(notdir $@)).manifest" && \
		$(ZIP) $(ZIP_V) --names-stdin "$(notdir $@)" \
		< "$(basename $(notdir $@)).manifest")
	$(verbose) rm -rf $(EZ_DIR) $(EZ_DIR).manifest
endif

# We need to recurse because the top-level make instance is evaluated
# before dependencies are downloaded.

MAYBE_APPS_LIST = $(if $(shell test -f $(ERLANG_MK_TMP)/apps.log && echo OK), \
		  $(ERLANG_MK_TMP)/apps.log)
DIST_LOCK = $(DIST_DIR).lock

dist:: $(ERLANG_MK_RECURSIVE_DEPS_LIST) all
	$(gen_verbose) \
	if command -v flock >/dev/null; then \
		flock $(DIST_LOCK) \
		sh -c '$(MAKE) do-dist \
		DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_DEPS_LIST) \
		$(MAYBE_APPS_LIST)"'; \
	elif command -v lockf >/dev/null; then \
		lockf $(DIST_LOCK) \
		sh -c '$(MAKE) do-dist \
		DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_DEPS_LIST) \
		$(MAYBE_APPS_LIST)"'; \
	else \
		$(MAKE) do-dist \
		DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_DEPS_LIST) \
		$(MAYBE_APPS_LIST)"; \
	fi

test-dist:: export TEST_DIR=NON-EXISTENT
test-dist:: $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) test-build
	$(gen_verbose) \
	if command -v flock >/dev/null; then \
		flock $(DIST_LOCK) \
		sh -c '$(MAKE) do-dist \
		DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
		$(MAYBE_APPS_LIST)"'; \
	elif command -v lockf >/dev/null; then \
		lockf $(DIST_LOCK) \
		sh -c '$(MAKE) do-dist \
		DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
		$(MAYBE_APPS_LIST)"'; \
	else \
		$(MAKE) do-dist \
		DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
		$(MAYBE_APPS_LIST)"; \
	fi

DIST_EZS = $(ERLANGMK_DIST_EZS) $(MIX_DIST_EZS)

do-dist:: $(DIST_EZS)
	$(verbose) unwanted='$(filter-out $(DIST_EZS) $(EXTRA_DIST_EZS), \
		$(wildcard $(DIST_DIR)/*))'; \
	test -z "$$unwanted" || (echo " RM     $$unwanted" && rm -rf $$unwanted)

CLI_SCRIPTS_LOCK = $(CLI_SCRIPTS_DIR).lock
CLI_ESCRIPTS_LOCK = $(CLI_ESCRIPTS_DIR).lock

ifeq ($(MAKELEVEL),0)
ifneq ($(filter-out rabbit_common amqp10_common rabbitmq_stream_common,$(PROJECT)),)
# These do not depend on 'rabbit' as DEPS but may as TEST_DEPS.
ifneq ($(filter-out amqp_client amqp10_client rabbitmq_amqp_client rabbitmq_ct_helpers,$(PROJECT)),)
app:: install-cli
endif
test-build:: install-cli
endif
endif

install-cli: install-cli-scripts install-cli-escripts
	@:

install-cli-scripts:
	$(gen_verbose) \
	set -e; \
	test -d "$(DEPS_DIR)/rabbit/scripts"; \
	if command -v flock >/dev/null; then \
		flock $(CLI_SCRIPTS_LOCK) \
		sh -e -c 'mkdir -p "$(CLI_SCRIPTS_DIR)" && \
			cp -a $(DEPS_DIR)/rabbit/scripts/* $(CLI_SCRIPTS_DIR)/'; \
	elif command -v lockf >/dev/null; then \
		lockf $(CLI_SCRIPTS_LOCK) \
		sh -e -c 'mkdir -p "$(CLI_SCRIPTS_DIR)" && \
			cp -a $(DEPS_DIR)/rabbit/scripts/* $(CLI_SCRIPTS_DIR)/'; \
	else \
		mkdir -p "$(CLI_SCRIPTS_DIR)" && \
			cp -a $(DEPS_DIR)/rabbit/scripts/* $(CLI_SCRIPTS_DIR)/; \
	fi

install-cli-escripts:
	$(gen_verbose) \
	if command -v flock >/dev/null; then \
		flock $(CLI_ESCRIPTS_LOCK) \
		sh -c 'mkdir -p "$(CLI_ESCRIPTS_DIR)" && \
		$(MAKE) -C "$(DEPS_DIR)/rabbitmq_cli" install \
			PREFIX="$(abspath $(CLI_ESCRIPTS_DIR))" \
			DESTDIR='; \
	elif command -v lockf >/dev/null; then \
		lockf $(CLI_ESCRIPTS_LOCK) \
		sh -c 'mkdir -p "$(CLI_ESCRIPTS_DIR)" && \
		$(MAKE) -C "$(DEPS_DIR)/rabbitmq_cli" install \
			PREFIX="$(abspath $(CLI_ESCRIPTS_DIR))" \
			DESTDIR='; \
	else \
		mkdir -p "$(CLI_ESCRIPTS_DIR)" && \
		$(MAKE) -C "$(DEPS_DIR)/rabbitmq_cli" install \
			PREFIX="$(abspath $(CLI_ESCRIPTS_DIR))" \
			DESTDIR= ; \
	fi

clean-dist::
	$(gen_verbose) rm -rf \
		"$(DIST_DIR)" \
		"$(CLI_SCRIPTS_DIR)" \
		"$(CLI_ESCRIPTS_DIR)"

clean:: clean-dist
