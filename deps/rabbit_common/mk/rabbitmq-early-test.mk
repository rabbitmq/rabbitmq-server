# --------------------------------------------------------------------
# xref
# --------------------------------------------------------------------

ifeq ($(filter distclean distclean-xref,$(MAKECMDGOALS)),)
ifneq ($(PROJECT),rabbit_common)
XREFR := $(DEPS_DIR)/rabbit_common/mk/xrefr
else
XREFR := mk/xrefr
endif
endif

# --------------------------------------------------------------------
# %-on-concourse dependencies.
# --------------------------------------------------------------------

ifneq ($(words $(filter %-on-concourse,$(MAKECMDGOALS))),0)
TEST_DEPS += ci $(RMQ_CI_CT_HOOKS)
dep_ci = git git@github.com:rabbitmq/rabbitmq-ci master
endif

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

CT_HOOKS ?= cth_styledout
TEST_DEPS += cth_styledout

RMQ_CI_CT_HOOKS = cth_fail_fast
ifdef TRAVIS
CT_HOOKS += $(RMQ_CI_CT_HOOKS)
TEST_DEPS += $(RMQ_CI_CT_HOOKS)
endif
ifdef CONCOURSE
CT_HOOKS += $(RMQ_CI_CT_HOOKS)
TEST_DEPS += $(RMQ_CI_CT_HOOKS)
endif
ifdef JENKINS_HOME
CT_HOOKS += cth_surefire $(RMQ_CI_CT_HOOKS)
TEST_DEPS += $(RMQ_CI_CT_HOOKS)
endif

dep_cth_fail_fast = git https://github.com/rabbitmq/cth_fail_fast.git master
dep_cth_styledout = git https://github.com/rabbitmq/cth_styledout.git master

CT_HOOKS_PARAM_VALUE = $(patsubst %,and %,$(CT_HOOKS))
CT_OPTS += -ct_hooks $(wordlist 2,$(words $(CT_HOOKS_PARAM_VALUE)),$(CT_HOOKS_PARAM_VALUE))

# Disable most messages on Travis because it might exceed the limit
# set by Travis.
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
