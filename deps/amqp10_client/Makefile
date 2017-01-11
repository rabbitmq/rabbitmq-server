PROJECT = amqp10_client
PROJECT_DESCRIPTION = AMQP 1.0 client from the RabbitMQ Project
PROJECT_MOD = amqp10_client_app

BUILD_DEPS = rabbit_common
DEPS = amqp10_common
TEST_DEPS = rabbit rabbitmq_amqp1_0 rabbitmq_ct_helpers

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# ActiveMQ for the testsuite.
# --------------------------------------------------------------------

ACTIVEMQ_VERSION := 5.14.3
ACTIVEMQ_URL := 'http://www.apache.org/dyn/closer.cgi?filename=/activemq/$(ACTIVEMQ_VERSION)/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz&action=download'

ACTIVEMQ := $(abspath test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)/bin/activemq)
export ACTIVEMQ

$(ACTIVEMQ): \
  test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz
	$(gen_verbose) cd "$(dir $<)" && tar zxf "$(notdir $<)"

test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz:
	$(gen_verbose) $(call core_http_get,$@,$(ACTIVEMQ_URL))

tests:: $(ACTIVEMQ)

ct ct-system: $(ACTIVEMQ)
