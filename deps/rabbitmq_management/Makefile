PACKAGE=rabbitmq-management
APPNAME=rabbit_management
DEPS=rabbitmq-mochiweb rabbitmq-server rabbitmq-erlang-client rabbitmq-management-agent
INTERNAL_DEPS=webmachine
RUNTIME_DEPS=webmachine

TEST_APPS=inets mochiweb rabbit_mochiweb webmachine rabbit_management_agent amqp_client rabbit_management
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=rabbit_mgmt_test_all:all_tests()

EXTRA_PACKAGE_DIRS=priv

WEB_DIR=priv/www
JAVASCRIPT_DIR=$(WEB_DIR)/js
TEMPLATES_DIR=$(JAVASCRIPT_DIR)/tmpl
EXTRA_TARGETS=$(wildcard $(TEMPLATES_DIR)/*.ejs) \
    $(wildcard $(JAVASCRIPT_DIR)/*.js) \
    $(wildcard $(WEB_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/css/*.css) \
    $(wildcard $(WEB_DIR)/img/*.png) \
    priv/www-cli/rabbitmqadmin \

include ../include.mk

priv/www-cli/rabbitmqadmin: bin/rabbitmqadmin
	cp $< $@

test: cleantest

cleantest:
	rm -rf tmp
