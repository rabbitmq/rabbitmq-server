PACKAGE=rabbitmq-management
DEPS=rabbitmq-mochiweb rabbitmq-server rabbitmq-erlang-client
INTERNAL_DEPS=webmachine
RUNTIME_DEPS=webmachine

TEST_APPS=mochiweb rabbit_mochiweb rabbit_management
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true
EXTRA_PACKAGE_DIRS=priv

WEB_DIR=priv/www
JAVASCRIPT_DIR=$(WEB_DIR)/js
TEMPLATES_DIR=$(JAVASCRIPT_DIR)/tmpl
EXTRA_TARGETS=$(wildcard $(TEMPLATES_DIR)/*.ejs) \
    $(wildcard $(JAVASCRIPT_DIR)/*.js) \
    $(wildcard $(WEB_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/css/*.css) \
    $(wildcard $(WEB_DIR)/img/*.png) \
    priv/dispatch.conf \

include ../include.mk
