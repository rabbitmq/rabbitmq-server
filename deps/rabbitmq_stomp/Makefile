PACKAGE=rabbitmq-stomp
DEPS=rabbitmq-server rabbitmq-erlang-client

START_RABBIT_IN_TESTS=true
TEST_APPS=amqp_client rabbit_stomp
TEST_COMMANDS=io:format(os:cmd(\"./test/test.py\")) io:format(\"~n\")

TEST_ARGS=-rabbit_stomp listeners "[{\"0.0.0.0\",61613}]"

include ../include.mk

