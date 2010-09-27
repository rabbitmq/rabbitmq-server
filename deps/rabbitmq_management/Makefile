DEPS:=rabbitmq-mochiweb webmachine rabbitmq-server rabbitmq-erlang-client

TEST_APPS=crypto inets mochiweb rabbit_mochiweb rabbit_management amqp_client
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=eunit:test(rabbit_mgmt_test_unit,[verbose]) rabbit_mgmt_test_db:test()

EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv

WEB_DIR:=$(EXTRA_PACKAGE_DIRS)/www
JAVASCRIPT_DIR:=$(WEB_DIR)/js
TEMPLATES_DIR:=$(JAVASCRIPT_DIR)/tmpl
EXTRA_TARGETS:=$(wildcard $(TEMPLATES_DIR)/*.ejs) \
    $(wildcard $(JAVASCRIPT_DIR)/*.js) \
    $(wildcard $(WEB_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/css/*.css) \
    $(wildcard $(WEB_DIR)/img/*.png) \

test: cleantest

cleantest:
	rm -rf tmp

include ../include.mk
