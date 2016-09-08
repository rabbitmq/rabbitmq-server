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
# Concourse doesn't set any environment variables to help us automate
# things. In rabbitmq-ci, we run tests under the `concourse` user so,
# look at that...
CT_QUIET_FLAGS = -verbosity 50 \
		 -erl_args \
		 -kernel error_logger silent
ifdef TRAVIS
CT_OPTS += $(CT_QUIET_FLAGS)
endif
ifdef CONCOURSE
CT_OPTS += $(CT_QUIET_FLAGS)
endif

# Enable JUnit-like report on Jenkins. Jenkins parses those reports so
# the results can be browsed from its UI. Furthermore, it displays a
# graph showing evolution of the results over time.
ifdef JENKINS_HOME
CT_OPTS += -ct_hooks cth_surefire
endif
