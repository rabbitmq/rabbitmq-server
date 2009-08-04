PACKAGE=rabbitmq-http-server
DEPS=rabbitmq-server rabbitmq-erlang-client
INTERNAL_DEPS=mochiweb
TEST_APPS=mochiweb inets rabbitmq_http_server rabbitmq_http_test
TEST_COMMANDS=rabbitmq_http_server_test:test()
START_RABBIT_IN_TESTS=true

include ../include.mk
