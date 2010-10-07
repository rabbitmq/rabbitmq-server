PACKAGE=rabbit_stomp
APPNAME=rabbit_stomp
DEPS=rabbitmq-server rabbitmq-erlang-client

START_RABBIT_IN_TESTS=true
TEST_APPS=rabbit_stomp
TEST_SCRIPTS=./test/test.py
UNIT_TEST_COMMANDS=eunit:test(rabbit_stomp_test_util,[verbose])

TEST_ARGS=-rabbit_stomp listeners "[{\"0.0.0.0\",61613}]"

include ../include.mk

testdeps:
	make -C deps/stomppy

test: unittest testdeps

unittest: $(TARGETS) $(TEST_TARGETS)
	ERL_LIBS=$(LIBS_PATH) $(ERL) $(TEST_LOAD_PATH) \
		$(foreach CMD,$(UNIT_TEST_COMMANDS),-eval '$(CMD)') \
		-eval 'init:stop()' | tee $(TMPDIR)/rabbit-stomp-unittest-output |\
			egrep "passed" >/dev/null

