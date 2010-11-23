PACKAGE=rabbit_amqp1_0
APPNAME=rabbit_amqp1_0
DEPS=rabbitmq-server rabbitmq-erlang-client

# EXTRA_PACKAGE_DIRS=priv

START_RABBIT_IN_TESTS=true
TEST_APPS=rabbit_amqp1_0
UNIT_TEST_COMMANDS=eunit:test(rabbit_amqp1_0_test,[verbose])

include ../include.mk

test: unittest

unittest: $(TARGETS) $(TEST_TARGETS)
	ERL_LIBS=$(LIBS_PATH) $(ERL) $(TEST_LOAD_PATH) \
		$(foreach CMD,$(UNIT_TEST_COMMANDS),-eval '$(CMD)') \
		-eval 'init:stop()' | tee $(TMPDIR)/rabbit-amqp1.0-unittest-output |\
			egrep "passed" >/dev/null
