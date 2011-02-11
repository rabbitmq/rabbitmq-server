PACKAGE=rabbitmq-federation
APPNAME=rabbit_federation
DEPS=rabbitmq-server rabbitmq-erlang-client

TEST_APPS=amqp_client rabbit_federation
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=eunit:test(rabbit_federation_test,[verbose])

include ../include.mk

test: cleantest

cleantest:
	rm -rf tmp
