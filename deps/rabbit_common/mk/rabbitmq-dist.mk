.PHONY: dist test-dist do-dist clean-dist

DIST_DIR = plugins

dist_verbose_0 = @echo " DIST  " $@;
dist_verbose_2 = set -x;
dist_verbose = $(dist_verbose_$(V))

# We take the version of an Erlang application from the .app file. This
# macro is called like this:
#
#   $(call get_app_version,/path/to/name.app.src)

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

# Define the target to create an .ez plugin archive. This macro is
# called like this:
#
#   $(call do_ez_target,app_name,app_version,app_dir)

define do_ez_target
dist_$(1)_ez_dir = $$(if $(2),$(DIST_DIR)/$(1)-$(2),$$(if $$(VERSION),$(DIST_DIR)/$(1)-$$(VERSION),$(DIST_DIR)/$(1)))
dist_$(1)_ez = $$(dist_$(1)_ez_dir).ez


$$(dist_$(1)_ez): APP     = $(1)
$$(dist_$(1)_ez): VSN     = $(2)
$$(dist_$(1)_ez): SRC_DIR = $(3)
$$(dist_$(1)_ez): EZ_DIR  = $$(abspath $$(dist_$(1)_ez_dir))
$$(dist_$(1)_ez): EZ      = $$(dist_$(1)_ez)
$$(dist_$(1)_ez): $$(if $$(wildcard $(3)/ebin $(3)/include $(3)/priv),\
	$$(call core_find,$$(wildcard $(3)/ebin $(3)/include $(3)/priv),*),)

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

DIST_EZS += $$(dist_$(1)_ez)

endef

# Real entry point: it tests the existence of an .app file to determine
# if it is an Erlang application (and therefore if it should be provided
# as an .ez plugin archive). Then, if calls do_ez_target. It should be
# called as:
#
#   $(call ez_target,app_name)

define ez_target
dist_$(1)_appdir = $$(if $$(filter $(PROJECT),$(1)), \
			$(CURDIR), \
			$$(if $$(shell test -d $(APPS_DIR)/$(1) && echo OK), \
			      $(APPS_DIR)/$(1), \
			      $(DEPS_DIR)/$(1)))
dist_$(1)_appfile = $$(dist_$(1)_appdir)/ebin/$(1).app

$$(if $$(shell test -f $$(dist_$(1)_appfile) && echo OK), \
  $$(eval $$(call do_ez_target,$(1),$$(call get_app_version,$$(dist_$(1)_appfile)),$$(dist_$(1)_appdir))))

endef

ifneq ($(filter do-dist,$(MAKECMDGOALS)),)
# The following code is evaluated only when running "make do-dist",
# otherwise it would trigger an infinite loop, as this code calls "make
# list-dist-deps" (see do_ez_target).
ifdef DIST_PLUGINS_LIST
# Now, try to create an .ez target for the top-level project and all
# dependencies.

ifeq ($(wildcard $(DIST_PLUGINS_LIST)),)
$(error DIST_PLUGINS_LIST ($(DIST_PLUGINS_LIST)) is missing)
endif

$(eval $(foreach app, \
  $(filter-out rabbit,$(sort $(notdir $(shell cat $(DIST_PLUGINS_LIST)))) $(PROJECT)), \
  $(call ez_target,$(app))))
endif
endif

# The actual recipe to create the .ez plugin archive. Some variables are
# defined in the do_ez_target macro above. All .ez archives are also
# listed in this do_ez_target macro.

RSYNC ?= rsync
RSYNC_V_0 =
RSYNC_V_1 = -v
RSYNC_V = $(RSYNC_V_$(V))

ZIP ?= zip
ZIP_V_0 = -q
ZIP_V_1 =
ZIP_V = $(ZIP_V_$(V))

$(DIST_DIR)/%.ez:
	$(verbose) rm -rf $(EZ_DIR) $(EZ)
	$(verbose) mkdir -p $(EZ_DIR)
	$(dist_verbose) $(RSYNC) -a $(RSYNC_V) \
		--include '/ebin/***' \
		--include '/include/***' \
		--include '/priv/***' \
		--exclude '*' \
		$(SRC_DIR)/ $(EZ_DIR)/
	@# Give a chance to the application to make any modification it
	@# wants to the tree before we make an archive.
	$(verbose) ! (test -f $(SRC_DIR)/rabbitmq-components.mk \
		&& grep -q '^prepare-dist::' $(SRC_DIR)/Makefile) || \
		$(MAKE) --no-print-directory -C $(SRC_DIR) prepare-dist \
		APP=$(APP) VSN=$(VSN) EZ_DIR=$(EZ_DIR)
	$(verbose) (cd $(DIST_DIR) && $(ZIP) $(ZIP_V) -r $*.ez $*)
	$(verbose) rm -rf $(EZ_DIR)

# We need to recurse because the top-level make instance is evaluated
# before dependencies are downloaded.

MAYBE_APPS_LIST = $(if $(shell test -f $(ERLANG_MK_TMP)/apps.log && echo OK),$(ERLANG_MK_TMP)/apps.log)

dist:: $(ERLANG_MK_RECURSIVE_DEPS_LIST) all
	$(gen_verbose) $(MAKE) do-dist DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_DEPS_LIST) $(MAYBE_APPS_LIST)"

test-dist:: $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) test-build
	$(gen_verbose) $(MAKE) do-dist DIST_PLUGINS_LIST="$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) $(MAYBE_APPS_LIST)"

do-dist:: $(DIST_EZS)
	$(verbose) unwanted='$(filter-out $(DIST_EZS),$(wildcard $(DIST_DIR)/*.ez))'; \
	test -z "$$unwanted" || (echo " RM     $$unwanted" && rm -f $$unwanted)

clean-dist::
	$(gen_verbose) rm -rf $(DIST_DIR)

clean:: clean-dist
