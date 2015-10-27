PROJECT = rabbitmq_amqp1_0

DEPS = amqp_client

TEST_DEPS = rabbit rabbitmq_java_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

EXTRA_SOURCES += include/rabbit_amqp1_0_framing.hrl \
		 src/rabbit_amqp1_0_framing0.erl

.DEFAULT_GOAL = all
$(PROJECT).d:: $(EXTRA_SOURCES)

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Framing sources generation.
# --------------------------------------------------------------------

CODEGEN       = $(CURDIR)/codegen.py
CODEGEN_DIR  ?= $(DEPS_DIR)/rabbitmq_codegen
CODEGEN_AMQP  = $(CODEGEN_DIR)/amqp_codegen.py
CODEGEN_SPECS = spec/messaging.xml spec/security.xml spec/transport.xml \
		spec/transactions.xml

include/rabbit_amqp1_0_framing.hrl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(CODEGEN_SPECS)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	  $(CODEGEN) hrl $(CODEGEN_SPECS) > $@

src/rabbit_amqp1_0_framing0.erl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(CODEGEN_SPECS)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	  $(CODEGEN) erl $(CODEGEN_SPECS) > $@

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

WITH_BROKER_TEST_SCRIPTS := $(CURDIR)/test/swiftmq/run-tests.sh

STANDALONE_TEST_COMMANDS := eunit:test(rabbit_amqp1_0_test,[verbose])

distclean:: distclean-swiftmq

distclean-swiftmq:
	$(gen_verbose) $(MAKE) -C test/swiftmq clean
