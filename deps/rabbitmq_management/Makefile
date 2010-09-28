PACKAGE=rabbitmq-management
APPNAME=rabbit_management
DEPS=rabbitmq-mochiweb rabbitmq-server rabbitmq-erlang-client
INTERNAL_DEPS=webmachine
RUNTIME_DEPS=webmachine

TEST_APPS=crypto inets mochiweb rabbit_mochiweb rabbit_management amqp_client
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=eunit:test(rabbit_mgmt_test_http,[verbose]) rabbit_mgmt_test_db:test()

EXTRA_PACKAGE_DIRS=priv

WEB_DIR=priv/www
JAVASCRIPT_DIR=$(WEB_DIR)/js
TEMPLATES_DIR=$(JAVASCRIPT_DIR)/tmpl
EXTRA_TARGETS=$(wildcard $(TEMPLATES_DIR)/*.ejs) \
    $(wildcard $(JAVASCRIPT_DIR)/*.js) \
    $(wildcard $(WEB_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/css/*.css) \
    $(wildcard $(WEB_DIR)/img/*.png) \

include ../include.mk

test: cleantest

cleantest:
	rm -rf tmp
