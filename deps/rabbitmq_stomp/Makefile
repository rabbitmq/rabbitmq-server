PACKAGE=rabbitmq_stomp
DEPS=rabbitmq-server rabbitmq-erlang-client

START_RABBIT_IN_TESTS=true
TEST_APPS=amqp_client rabbitmq_stomp
TEST_SCRIPTS=./test/test.py

TEST_ARGS=-rabbitmq_stomp listeners "[{\"0.0.0.0\",61613}]"

include ../include.mk

