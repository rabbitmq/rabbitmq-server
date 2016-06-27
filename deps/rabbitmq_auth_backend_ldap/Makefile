PROJECT = rabbitmq_auth_backend_ldap

DEPS = amqp_client
TEST_DEPS = rabbit amqp_client ct_helper
dep_ct_helper = git https://github.com/extend/ct_helper.git master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))
include erlang.mk

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

# ifneq ($(filter tests tests-with-broker test,$(MAKECMDGOALS)),)
# ifeq ($(shell nc -z localhost 3890 && echo true),true)
# WITH_BROKER_TEST_MAKEVARS := \
# 	RABBITMQ_CONFIG_FILE=$(CURDIR)/etc/rabbit-test
# WITH_BROKER_TEST_COMMANDS := \
# 	eunit:test([rabbit_auth_backend_ldap_unit_test,rabbit_auth_backend_ldap_test],[verbose])
# else
# $(info Skipping LDAP tests; no LDAP server found on localhost)
# endif
# endif
