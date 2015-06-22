PROJECT = rabbitmq-common

DEPS = rabbitmq-codegen
dep_rabbitmq-codegen = git file:///home/dumbbell/Projects/pivotal/rabbitmq-public-umbrella/rabbitmq-codegen erlang.mk

CODEGEN       = $(CURDIR)/codegen.py
CODEGEN_DIR  ?= $(DEPS_DIR)/rabbitmq-codegen
CODEGEN_AMQP  = $(CODEGEN_DIR)/amqp_codegen.py

GENERATED_SOURCES = include/rabbit_framing.hrl \
		    src/rabbit_framing_amqp_0_8.erl \
		    src/rabbit_framing_amqp_0_9_1.erl

.DEFAULT_GOAL = all

app:: $(GENERATED_SOURCES)

include erlang.mk

clean:: clean-generated

clean-generated:
	$(gen_verbose) rm -f $(GENERATED_SOURCES)

AMQP_SPEC_JSON_FILES_0_8   = $(CODEGEN_DIR)/amqp-rabbitmq-0.8.json
AMQP_SPEC_JSON_FILES_0_9_1 = $(CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json \
			     $(CODEGEN_DIR)/credit_extension.json

include/rabbit_framing.hrl: deps $(CODEGEN) $(CODEGEN_AMQP) $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) --ignore-conflicts header \
	 $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8) $@

src/rabbit_framing_amqp_0_9_1.erl: deps $(CODEGEN) $(CODEGEN_AMQP) $(AMQP_SPEC_JSON_FILES_0_9_1)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) body $(AMQP_SPEC_JSON_FILES_0_9_1) $@

src/rabbit_framing_amqp_0_8.erl: deps $(CODEGEN) $(CODEGEN_AMQP) $(AMQP_SPEC_JSON_FILES_0_8)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) body $(AMQP_SPEC_JSON_FILES_0_8) $@
