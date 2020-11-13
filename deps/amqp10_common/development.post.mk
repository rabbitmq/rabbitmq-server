# --------------------------------------------------------------------
# Framing sources generation.
# --------------------------------------------------------------------

PYTHON       ?= python
CODEGEN       = $(CURDIR)/codegen.py
CODEGEN_DIR  ?= $(DEPS_DIR)/rabbitmq_codegen
CODEGEN_AMQP  = $(CODEGEN_DIR)/amqp_codegen.py

AMQP_SPEC_1_0 = $(CODEGEN_DIR)/amqp-1.0/messaging.xml			\
		$(CODEGEN_DIR)/amqp-1.0/security.xml			\
		$(CODEGEN_DIR)/amqp-1.0/transport.xml			\
		$(CODEGEN_DIR)/amqp-1.0/transactions.xml

include/amqp10_framing.hrl:: $(CODEGEN) $(CODEGEN_AMQP) $(AMQP_SPEC_1_0)
	$(verbose) mkdir -p $(dir $@)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	  $(PYTHON) $(CODEGEN) hrl $(AMQP_SPEC_1_0) > $@

src/amqp10_framing0.erl:: $(CODEGEN) $(CODEGEN_AMQP) $(AMQP_SPEC_1_0)
	$(verbose) mkdir -p $(dir $@)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	  $(PYTHON) $(CODEGEN) erl $(AMQP_SPEC_1_0) > $@

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)
