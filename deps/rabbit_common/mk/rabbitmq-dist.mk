.PHONY: dist test-dist do-dist clean-dist

DIST_DIR = plugins

dist_verbose_0 = @echo " DIST  " $@;
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
dist_$(1)_ez = $$(if $(2),$(DIST_DIR)/$(1)-$(2).ez,$(DIST_DIR)/$(1).ez)

$$(dist_$(1)_ez): APP = $(1)
$$(dist_$(1)_ez): VSN = $(2)
$$(dist_$(1)_ez): DIR = $(3)
$$(dist_$(1)_ez): $(3)/ebin/$(1).app $(3)/ebin/*.beam $$(call core_find $(3)/include,*)

DIST_EZS += $$(dist_$(1)_ez)

endef

# Real entry point: it tests the existence of an .app file to determine
# if it is an Erlang application (and therefore if it should be provided
# as an .ez plugin archive). Then, if calls do_ez_target. It should be
# called as:
#
#   $(call ez_target,app_name)

define ez_target
dist_$(1)_appdir = $$(if $$(filter $(PROJECT),$(1)),$(CURDIR),$(DEPS_DIR)/$(1))
dist_$(1)_appfile = $$(dist_$(1)_appdir)/ebin/$(1).app

$$(if $$(shell test -f $$(dist_$(1)_appfile) && echo OK), \
  $$(eval $$(call do_ez_target,$(1),$$(call get_app_version,$$(dist_$(1)_appfile)),$$(dist_$(1)_appdir))))

endef

# Now, try to create an .ez target for the top-level project and all
# dependencies.
#
# FIXME: Taking everything in $(DEPS_DIR) is not correct: it could
# contain unrelated applications. We need to get the real list of
# dependencies; see `make list-deps`.

$(eval $(foreach app, \
  $(filter-out rabbit,$(sort $(notdir $(wildcard $(DEPS_DIR)/*))) $(PROJECT)), \
  $(call ez_target,$(app))))

# The actual recipe to create the .ez plugin archive. Some variables are
# defined in the do_ez_target macro above. All .ez archives are also
# listed in this do_ez_target macro.

$(DIST_DIR)/%.ez:
	$(dist_verbose) mkdir -p $(DIST_DIR) && \
	rm -f $(DIST_DIR)/$* && \
	ln -s $(DIR) $(DIST_DIR)/$* && \
	(cd $(DIST_DIR) && zip -q0 -r $*.ez \
		$*/ebin \
		$*/include) && \
	rm -f $(DIST_DIR)/$*

# We need to recurse because the top-level make instance is evaluated
# before dependencies are downloaded.

dist:: all
	@$(MAKE) do-dist

test-dist:: test-build
	@$(MAKE) do-dist

do-dist:: $(DIST_EZS)
	@:

clean-dist::
	$(gen_verbose) rm -rf $(DIST_DIR)

clean:: clean-dist
