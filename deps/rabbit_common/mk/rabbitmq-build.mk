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
TEST_ERLC_OPTS += +nowarn_export_all $(WARNING_OPTS)

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
# Common Test flags.
# --------------------------------------------------------------------

# Enable the following common_test hooks on Travis and Concourse:
#
# cth_fail_fast
#   This hook will make sure the first failure puts an end to the
#   testsuites; ie. all remaining tests are skipped.
#
# cth_styledout
#   This hook will change the output of common_test to something more
#   concise and colored.
#
# On Jenkins, in addition to those common_test hooks, enable JUnit-like
# report. Jenkins parses those reports so the results can be browsed
# from its UI. Furthermore, it displays a graph showing evolution of the
# results over time.

CT_HOOKS ?=

RMQ_CT_HOOKS = cth_fail_fast cth_styledout
ifdef TRAVIS
CT_HOOKS += $(RMQ_CT_HOOKS)
TEST_DEPS += $(RMQ_CT_HOOKS)
endif
ifdef CONCOURSE
CT_HOOKS += $(RMQ_CT_HOOKS)
TEST_DEPS += $(RMQ_CT_HOOKS)
endif
ifdef JENKINS_HOME
CT_HOOKS += cth_surefire $(RMQ_CT_HOOKS)
TEST_DEPS += $(RMQ_CT_HOOKS)
endif

dep_cth_fail_fast = git https://github.com/rabbitmq/cth_fail_fast.git master
dep_cth_styledout = git https://github.com/rabbitmq/cth_styledout.git master

CT_HOOKS_PARAM_VALUE = $(patsubst %,and %,$(CT_HOOKS))
CT_OPTS += -ct_hooks $(wordlist 2,$(words $(CT_HOOKS_PARAM_VALUE)),$(CT_HOOKS_PARAM_VALUE))

# Disable most messages on Travis because it might exceed the limit
# set by Travis.
#
# On Concourse, we keep the default verbosity. With Erlang 19.2+, the
# cth_styledout hook will change the output to something concise and all
# messages are available in in HTML reports. With Erlang up-to 19.1,
# stdout will be flooded with messages, but we'll live with that.
#
# CAUTION: All arguments after -erl_args are passed to the emulator and
# common_test doesn't interpret them! Therefore, all common_test flags
# *MUST* appear before.

CT_QUIET_FLAGS = -verbosity 50 \
		 -erl_args \
		 -kernel error_logger silent

ifdef TRAVIS
CT_OPTS += $(CT_QUIET_FLAGS)
endif

# On CI, set $RABBITMQ_CT_SKIP_AS_ERROR so that any skipped
# testsuite/testgroup/testcase is considered an error.

ifdef TRAVIS
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
ifdef CONCOURSE
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
ifdef JENKINS_HOME
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
