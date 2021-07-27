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

MIX_ARCHIVES ?= $(HOME)/.mix/archives

MIX_TASK_ARCHIVE_DEPS_VERSION = 0.5.0
mix_task_archive_deps = $(MIX_ARCHIVES)/mix_task_archive_deps-$(MIX_TASK_ARCHIVE_DEPS_VERSION)

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

define get_mix_project_version
$(shell cd $(1) && \
	$(MIX) do deps.get, deps.compile, compile >/dev/null && \
	$(MIX) run --no-start -e "IO.puts(Mix.Project.config[:version])")
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

# Define the target to create an .ez plugin archive for a Mix-based
# project. This macro is called like this:
#
#   $(call do_ez_target_mix,app_name,app_version,app_dir)

define get_mix_project_dep_ezs
$(shell cd $(1) && \
	$(MIX) do deps.get, deps.compile, compile >/dev/null && \
	$(MIX) archive.build.all.list -e -o $(DIST_DIR) --skip "rabbit $(ERLANGMK_DIST_APPS)")
endef

define do_ez_target_mix
dist_$(1)_ez_dir = $$(if $(2),$(DIST_DIR)/$(1)-$(2), \
	$$(if $$(VERSION),$(DIST_DIR)/$(1)-$$(VERSION),$(DIST_DIR)/$(1)))
dist_$(1)_ez = $$(dist_$(1)_ez_dir).ez

$$(dist_$(1)_ez): APP     = $(1)
$$(dist_$(1)_ez): VSN     = $(2)
$$(dist_$(1)_ez): SRC_DIR = $(3)
$$(dist_$(1)_ez): EZ_DIR  = $$(abspath $$(dist_$(1)_ez_dir))
$$(dist_$(1)_ez): EZ      = $$(dist_$(1)_ez)
$$(dist_$(1)_ez): $$(if $$(wildcard _build/dev/lib/$(1)/ebin $(3)/priv),\
	$$(filter-out %/dep_built,$$(call core_find,$$(wildcard _build/dev/lib/$(1)/ebin $(3)/priv),*)),)

MIX_DIST_EZS += $$(dist_$(1)_ez)
EXTRA_DIST_EZS += $$(call get_mix_project_dep_ezs,$(3))

endef

# Real entry point: it tests the existence of an .app file to determine
# if it is an Erlang application (and therefore if it should be provided
# as an .ez plugin archive) and calls do_ez_target_erlangmk. If instead
# it finds a Mix configuration file, it calls do_ez_target_mix. It
# should be called as:
#
#   $(call ez_target,path_to_app)

define ez_target
dist_$(1)_appdir  = $(2)
dist_$(1)_appfile = $$(dist_$(1)_appdir)/ebin/$(1).app
dist_$(1)_mixfile = $$(dist_$(1)_appdir)/mix.exs

$$(if $$(shell test -f $$(dist_$(1)_appfile) && echo OK), \
  $$(eval $$(call do_ez_target_erlangmk,$(1),$$(call get_app_version,$$(dist_$(1)_appfile)),$$(dist_$(1)_appdir))), \
  $$(if $$(shell test -f $$(dist_$(1)_mixfile) && [ "x$(1)" != "xrabbitmqctl" ] && [ "x$(1)" != "xrabbitmq_cli" ] && echo OK), \
    $$(eval $$(call do_ez_target_mix,$(1),$$(call get_mix_project_version,$$(dist_$(1)_appdir)),$$(dist_$(1)_appdir)))))

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
	$(dist_verbose) $(RSYNC) -a $(RSYNC_V) \
		--exclude '/ebin/dep_built' \
		--exclude '/ebin/test' \
		--include '/ebin/***' \
		--include '/include/***' \
		--include '/priv/***' \
		--exclude '*' \
		$(call core_unix_path,$(SRC_DIR))/ $(call core_unix_path,$(EZ_DIR))/
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

$(MIX_DIST_EZS): $(mix_task_archive_deps)
	$(verbose) cd $(SRC_DIR) && \
		$(MIX) do deps.get, deps.compile, compile, archive.build.all \
		-e -o $(abspath $(DIST_DIR)) --skip "rabbit $(ERLANGMK_DIST_APPS)"

MIX_TASK_ARCHIVE_DEPS_URL = https://github.com/rabbitmq/mix_task_archive_deps/releases/download/$(MIX_TASK_ARCHIVE_DEPS_VERSION)/mix_task_archive_deps-$(MIX_TASK_ARCHIVE_DEPS_VERSION).ez

$(mix_task_archive_deps):
	$(gen_verbose) mix archive.install --force $(MIX_TASK_ARCHIVE_DEPS_URL)

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

ifneq ($(filter-out rabbit_common amqp10_common rabbitmq_stream_common,$(PROJECT)),)
dist:: install-cli
test-build:: install-cli
endif

install-cli: install-cli-scripts install-cli-escripts
	@:

ifeq ($(PROJECT),rabbit)
install-cli-scripts:
	$(gen_verbose) \
	if command -v flock >/dev/null; then \
		flock $(CLI_SCRIPTS_LOCK) \
		sh -c 'mkdir -p "$(CLI_SCRIPTS_DIR)" && \
		for file in scripts/*; do \
			cmp -s "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")" || \
			cp -a "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")"; \
		done'; \
	elif command -v lockf >/dev/null; then \
		lockf $(CLI_SCRIPTS_LOCK) \
		sh -c 'mkdir -p "$(CLI_SCRIPTS_DIR)" && \
		for file in scripts/*; do \
			cmp -s "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")" || \
			cp -a "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")"; \
		done'; \
	else \
		mkdir -p "$(CLI_SCRIPTS_DIR)" && \
		for file in scripts/*; do \
			cmp -s "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")" || \
			cp -a "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")"; \
		done; \
	fi
else

install-cli-scripts:
	$(gen_verbose) \
	set -e; \
	if test -d "$(APPS_DIR)/rabbit/scripts"; then \
		rabbit_scripts_dir='$(APPS_DIR)/rabbit/scripts'; \
	elif test -d "$(DEPS_DIR)/rabbit/scripts"; then \
		rabbit_scripts_dir='$(DEPS_DIR)/rabbit/scripts'; \
	else \
		echo 'rabbit/scripts directory not found' 1>&2; \
		exit 1; \
	fi; \
	test -d "$$rabbit_scripts_dir"; \
	if command -v flock >/dev/null; then \
		flock $(CLI_SCRIPTS_LOCK) \
		sh -e -c 'mkdir -p "$(CLI_SCRIPTS_DIR)" && \
		for file in "'$$rabbit_scripts_dir'"/*; do \
			test -f "$$file"; \
			cmp -s "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")" || \
			cp -a "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")"; \
		done'; \
	elif command -v lockf >/dev/null; then \
		lockf $(CLI_SCRIPTS_LOCK) \
		sh -e -c 'mkdir -p "$(CLI_SCRIPTS_DIR)" && \
		for file in "'$$rabbit_scripts_dir'"/*; do \
			test -f "$$file"; \
			cmp -s "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")" || \
			cp -a "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")"; \
		done'; \
	else \
		mkdir -p "$(CLI_SCRIPTS_DIR)" && \
		for file in "$$rabbit_scripts_dir"/*; do \
			test -f "$$file"; \
			cmp -s "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")" || \
			cp -a "$$file" "$(CLI_SCRIPTS_DIR)/$$(basename "$$file")"; \
		done; \
	fi
endif

install-cli-escripts:
	$(gen_verbose) \
	if command -v flock >/dev/null; then \
		flock $(CLI_ESCRIPTS_LOCK) \
		sh -c 'mkdir -p "$(CLI_ESCRIPTS_DIR)" && \
		$(MAKE) -C "$(APPS_DIR)/rabbitmq_cli" install \
			PREFIX="$(abspath $(CLI_ESCRIPTS_DIR))" \
			DESTDIR='; \
	elif command -v lockf >/dev/null; then \
		lockf $(CLI_ESCRIPTS_LOCK) \
		sh -c 'mkdir -p "$(CLI_ESCRIPTS_DIR)" && \
		$(MAKE) -C "$(APPS_DIR)/rabbitmq_cli" install \
			PREFIX="$(abspath $(CLI_ESCRIPTS_DIR))" \
			DESTDIR='; \
	else \
		mkdir -p "$(CLI_ESCRIPTS_DIR)" && \
		$(MAKE) -C "$(APPS_DIR)/rabbitmq_cli" install \
			PREFIX="$(abspath $(CLI_ESCRIPTS_DIR))" \
			DESTDIR= ; \
	fi

clean-dist::
	$(gen_verbose) rm -rf \
		"$(DIST_DIR)" \
		"$(CLI_SCRIPTS_DIR)" \
		"$(CLI_ESCRIPTS_DIR)"

clean:: clean-dist
