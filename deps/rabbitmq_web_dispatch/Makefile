PACKAGE=mod_http
DEPS=rabbitmq-server rabbitmq-erlang-client
INTERNAL_DEPS=mochiweb
TEST_APPS=mochiweb inets mod_http mod_http_test_app
TEST_COMMANDS=mod_http_test:test()
START_RABBIT_IN_TESTS=true

include ../include.mk
