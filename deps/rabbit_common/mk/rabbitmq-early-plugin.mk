# --------------------------------------------------------------------
# dialyzer
# --------------------------------------------------------------------

DIALYZER_OPTS ?= -Werror_handling -Wunmatched_returns -Wunknown

# --------------------------------------------------------------------
# Common Test flags.
# --------------------------------------------------------------------

ifneq ($(PROJECT),rabbitmq_server_release)
CT_LOGS_DIR = $(abspath $(CURDIR)/../../logs)
endif

# We start the common_test node as a hidden Erlang node. The benefit
# is that other Erlang nodes won't try to connect to each other after
# discovering the common_test node if they are not meant to.
#
# This helps when several unrelated RabbitMQ clusters are started in
# parallel.

CT_OPTS += -hidden

# We set a low tick time to deal with distribution failures quicker.

CT_OPTS += -kernel net_ticktime 5

ifdef GITHUB_ACTIONS
# On CI, set $RABBITMQ_CT_SKIP_AS_ERROR so that any skipped
# testsuite/testgroup/testcase is considered an error.
export RABBITMQ_CT_SKIP_AS_ERROR = true
else
# This hook will change the output of common_test to something more
# concise and colored. Not used on GitHub Actions except in parallel
# CT where it is hardcoded.
CT_HOOKS += cth_styledout
endif

TEST_DEPS += cth_styledout
dep_cth_styledout = git https://github.com/rabbitmq/cth_styledout.git master

ifneq ($(strip $(CT_HOOKS)),)
CT_OPTS += -ct_hooks $(CT_HOOKS)
endif
