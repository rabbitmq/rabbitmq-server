# --------------------------------------------------------------------
# Compiler flags.
# --------------------------------------------------------------------

# FIXME: We copy Erlang.mk default flags here: rabbitmq-build.mk is
# loaded as a plugin, so before those variables are defined. And because
# Erlang.mk uses '?=', the flags we set here override the default set.
#
# See: https://github.com/ninenines/erlang.mk/issues/502

WARNING_OPTS += +debug_info \
		+warn_export_vars \
		+warn_shadow_vars \
		+warn_obsolete_guard
ERLC_OPTS += -Werror $(WARNING_OPTS)
TEST_ERLC_OPTS += $(WARNING_OPTS)

define compare_version
$(shell awk 'BEGIN {
	split("$(1)", v1, ".");
	version1 = v1[1] * 1000000 + v1[2] * 10000 + v1[3] * 100 + v1[4];

	split("$(2)", v2, ".");
	version2 = v2[1] * 1000000 + v2[2] * 10000 + v2[3] * 100 + v2[4];

	if (version1 $(3) version2) {
		print "true";
	} else {
		print "false";
	}
}')
endef

# Erlang R16B03 has no support for new types in Erlang 17.0, leading to
# a build-time error.
ERTS_VER := $(shell erl -version 2>&1 | sed -E 's/.* version //')
old_builtin_types_MAX_ERTS_VER = 6.0
ifeq ($(call compare_version,$(ERTS_VER),$(old_builtin_types_MAX_ERTS_VER),<),true)
RMQ_ERLC_OPTS += -Duse_old_builtin_types
endif

# Push our compilation options to both the normal and test ERLC_OPTS.
ERLC_OPTS += $(RMQ_ERLC_OPTS)
TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Common test flags.
# --------------------------------------------------------------------

# Disable most messages on Travis and Concourse.
#
# On CI, set $RABBITMQ_CT_SKIP_AS_ERROR so that any skipped
# testsuite/testgroup/testcase is considered an error.

CT_QUIET_FLAGS = -verbosity 50 \
		 -erl_args \
		 -kernel error_logger silent

ifdef TRAVIS
CT_OPTS += $(CT_QUIET_FLAGS)
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
ifdef CONCOURSE
CT_OPTS += $(CT_QUIET_FLAGS)
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif

# Enable JUnit-like report on Jenkins. Jenkins parses those reports so
# the results can be browsed from its UI. Furthermore, it displays a
# graph showing evolution of the results over time.
ifdef JENKINS_HOME
CT_OPTS += -ct_hooks cth_surefire
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif

# --------------------------------------------------------------------
# Hex.pm.
# --------------------------------------------------------------------

.PHONY: hex-publish hex-publish-docs

HEXPM_URL = file:///home/dumbbell/Projects/pivotal/other-repos/hexpm-cli/hexpm
HEXPM_CLI = $(ERLANG_MK_TMP)/hexpm

$(HEXPM_CLI):
	$(gen_verbose) $(call core_http_get,$@,$(HEXPM_URL))
	$(verbose) chmod +x $@

rebar.config: dep_rabbit_common = hex $(PROJECT_VERSION)
rebar.config: dep_amqp_client = hex $(PROJECT_VERSION)

define RABBITMQ_HEXPM_DEFAULT_FILES
	    "erlang.mk",
	    "git-revisions.txt",
	    "include",
	    "LICENSE*",
	    "Makefile",
	    "rabbitmq-components.mk",
	    "README",
	    "README.md",
	    "src"
endef

hex-publish: $(HEXPM_CLI) app rebar.config
	$(gen_verbose) echo "$(PROJECT_DESCRIPTION) $(PROJECT_VERSION)" \
		> git-revisions.txt
ifneq ($(PROJECT),rabbit_common)
	$(verbose) mv rabbitmq-components.mk rabbitmq-components.mk.not-hexpm
	$(verbose) cp \
		$(DEPS_DIR)/rabbit_common/mk/rabbitmq-components.hexpm.mk \
		rabbitmq-components.mk
	$(verbose) touch -r rabbitmq-components.mk.not-hexpm \
		rabbitmq-components.mk
endif
	$(verbose) trap '\
		rm -f git-revisions.txt rebar.lock; \
		if test -f rabbitmq-components.mk.not-hexpm; then \
		  mv rabbitmq-components.mk.not-hexpm rabbitmq-components.mk; \
		fi' EXIT INT; \
		$(HEXPM_CLI) publish

hex-publish-docs: $(HEXPM_CLI) app docs
	$(gen_verbose) trap 'rm -f rebar.lock' EXIT INT; \
		$(HEXPM_CLI) docs
