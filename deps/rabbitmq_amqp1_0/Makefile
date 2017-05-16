PROJECT = rabbitmq_amqp1_0
PROJECT_DESCRIPTION = AMQP 1.0 support for RabbitMQ

define PROJECT_ENV
[
	    {default_user, "guest"},
	    {default_vhost, <<"/">>},
	    {protocol_strict_mode, false}
	  ]
endef

BUILD_DEPS = rabbitmq_codegen
DEPS = rabbit_common rabbit amqp_client
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
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
CODEGEN_SPECS = $(CODEGEN_DIR)/amqp-1.0/messaging.xml			\
		$(CODEGEN_DIR)/amqp-1.0/security.xml			\
		$(CODEGEN_DIR)/amqp-1.0/transport.xml			\
		$(CODEGEN_DIR)/amqp-1.0/transactions.xml

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

distclean:: distclean-dotnet-tests distclean-java-tests

distclean-dotnet-tests:
	$(gen_verbose) cd test/system_SUITE_data/dotnet-tests && \
		rm -rf bin obj && \
		rm -f project.lock.json TestResult.xml

distclean-java-tests:
	$(gen_verbose) cd test/system_SUITE_data/java-tests && mvn clean
