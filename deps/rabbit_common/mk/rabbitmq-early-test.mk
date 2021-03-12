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
# dialyzer
# --------------------------------------------------------------------

DIALYZER_OPTS ?= -Werror_handling -Wrace_conditions

# --------------------------------------------------------------------
# %-on-concourse dependencies.
# --------------------------------------------------------------------

ifneq ($(words $(filter %-on-concourse,$(MAKECMDGOALS))),0)
TEST_DEPS += ci $(RMQ_CI_CT_HOOKS)
NO_AUTOPATCH += ci $(RMQ_CI_CT_HOOKS)
dep_ci = git git@github.com:rabbitmq/rabbitmq-ci master
endif

# --------------------------------------------------------------------
# Common Test flags.
# --------------------------------------------------------------------

# We start the common_test node as a hidden Erlang node. The benefit
# is that other Erlang nodes won't try to connect to each other after
# discovering the common_test node if they are not meant to.
#
# This helps when several unrelated RabbitMQ clusters are started in
# parallel.

CT_OPTS += -hidden

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

ifndef TRAVIS
CT_HOOKS ?= cth_styledout
TEST_DEPS += cth_styledout
endif

ifdef TRAVIS
FAIL_FAST = 1
SKIP_AS_ERROR = 1
endif

ifdef CONCOURSE
FAIL_FAST = 1
SKIP_AS_ERROR = 1
endif

RMQ_CI_CT_HOOKS = cth_fail_fast
ifeq ($(FAIL_FAST),1)
CT_HOOKS += $(RMQ_CI_CT_HOOKS)
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

ifeq ($(SKIP_AS_ERROR),1)
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif

# --------------------------------------------------------------------
# Looking Glass rules.
# --------------------------------------------------------------------

ifneq ("$(RABBITMQ_TRACER)","")
BUILD_DEPS += looking_glass
ERL_LIBS := "$(ERL_LIBS):../looking_glass:../lz4"
export RABBITMQ_TRACER
endif

define lg_callgrind.erl
lg_callgrind:profile_many("traces.lz4.*", "callgrind.out", #{running => true}),
halt().
endef

.PHONY: profile clean-profile

profile:
	$(gen_verbose) $(call erlang,$(call lg_callgrind.erl))

clean:: clean-profile

clean-profile:
	$(gen_verbose) rm -f traces.lz4.* callgrind.out.*
