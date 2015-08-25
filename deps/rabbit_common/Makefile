PROJECT = rabbit_common

DEPS = rabbitmq_codegen
dep_rabbitmq_codegen = git file:///home/dumbbell/Projects/pivotal/rabbitmq-public-umbrella/rabbitmq-codegen erlang.mk

EXTRA_SOURCES += include/rabbit_framing.hrl				\
		 src/rabbit_framing_amqp_0_8.erl			\
		 src/rabbit_framing_amqp_0_9_1.erl

include erlang.mk

# --------------------------------------------------------------------
# Framing sources generation.
# --------------------------------------------------------------------

CODEGEN       = $(CURDIR)/codegen.py
CODEGEN_DIR  ?= $(DEPS_DIR)/rabbitmq_codegen
CODEGEN_AMQP  = $(CODEGEN_DIR)/amqp_codegen.py

AMQP_SPEC_JSON_FILES_0_8   = $(CODEGEN_DIR)/amqp-rabbitmq-0.8.json
AMQP_SPEC_JSON_FILES_0_9_1 = $(CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json	\
			     $(CODEGEN_DIR)/credit_extension.json

include/rabbit_framing.hrl: $(CODEGEN) $(CODEGEN_AMQP) \
    $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) --ignore-conflicts header \
	 $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8) $@

src/rabbit_framing_amqp_0_9_1.erl: $(CODEGEN) $(CODEGEN_AMQP) \
    $(AMQP_SPEC_JSON_FILES_0_9_1)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) body $(AMQP_SPEC_JSON_FILES_0_9_1) $@

src/rabbit_framing_amqp_0_8.erl: $(CODEGEN) $(CODEGEN_AMQP) \
    $(AMQP_SPEC_JSON_FILES_0_8)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) body $(AMQP_SPEC_JSON_FILES_0_8) $@
