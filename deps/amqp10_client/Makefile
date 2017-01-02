PROJECT = amqp10_client
PROJECT_DESCRIPTION = AMQP 1.0 client from the RabbitMQ Project
PROJECT_MOD = amqp10_client_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

BUILD_DEPS = rabbitmq_codegen rabbit_common
TEST_DEPS = rabbit rabbitmq_amqp1_0 rabbitmq_ct_helpers

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

PYTHON       ?= python
CODEGEN       = $(CURDIR)/codegen.py
CODEGEN_DIR  ?= $(DEPS_DIR)/rabbitmq_codegen
CODEGEN_AMQP  = $(CODEGEN_DIR)/amqp_codegen.py
CODEGEN_SPECS = spec/messaging.xml spec/security.xml spec/transport.xml \
		spec/transactions.xml

include/rabbit_amqp1_0_framing.hrl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(CODEGEN_SPECS)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	  $(PYTHON) $(CODEGEN) hrl $(CODEGEN_SPECS) > $@

src/rabbit_amqp1_0_framing0.erl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(CODEGEN_SPECS)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	  $(PYTHON) $(CODEGEN) erl $(CODEGEN_SPECS) > $@

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)
