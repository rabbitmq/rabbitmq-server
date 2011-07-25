RELEASABLE:=true
APP_NAME=rabbitmq_amqp1_0
DEPS:=rabbitmq-server rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_amqp1_0_test,[verbose])

FRAMING_ERL=src/rabbit_amqp1_0_framing0.erl
FRAMING_HRL=include/rabbit_amqp1_0_framing.hrl
CODEGEN=./codegen.py
CODEGEN_SPECS=spec/messaging.xml spec/transport.xml

$(FRAMING_ERL): $(CODEGEN) $(CODEGEN_SPECS)
	$(CODEGEN) erl $(CODEGEN_SPECS) > $@

$(FRAMING_HRL): $(CODEGEN) $(CODEGEN_SPECS)
	$(CODEGEN) hrl $(CODEGEN_SPECS) > $@

$(EBIN_BEAMS): $(FRAMING_HRL)

$(PACKAGE_DIR)+clean::
	rm -f $(FRAMING_HRL) $(FRAMING_ERL)
