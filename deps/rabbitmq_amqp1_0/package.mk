RELEASABLE:=true
APP_NAME=rabbitmq_amqp1_0
DEPS:=rabbitmq-server rabbitmq-erlang-client
STANDALONE_TEST_COMMANDS:=eunit:test(rabbit_amqp1_0_test,[verbose])

FRAMING_HRL=$(PACKAGE_DIR)/include/rabbit_amqp1_0_framing.hrl
FRAMING_ERL=$(PACKAGE_DIR)/src/rabbit_amqp1_0_framing0.erl
CODEGEN=$(PACKAGE_DIR)/codegen.py
CODEGEN_SPECS=$(PACKAGE_DIR)/spec/messaging.xml $(PACKAGE_DIR)/spec/security.xml $(PACKAGE_DIR)/spec/transport.xml $(PACKAGE_DIR)/spec/transactions.xml

INCLUDE_HRLS+=$(FRAMING_HRL)
SOURCE_ERLS+=$(FRAMING_ERL)

define package_rules

$(FRAMING_ERL): $(CODEGEN) $(CODEGEN_SPECS)
	$(CODEGEN) erl $(CODEGEN_SPECS) > $$@

$(FRAMING_HRL): $(CODEGEN) $(CODEGEN_SPECS)
	$(CODEGEN) hrl $(CODEGEN_SPECS) > $$@

$(PACKAGE_DIR)+clean::
	rm -f $(FRAMING_HRL) $(FRAMING_ERL)

endef
