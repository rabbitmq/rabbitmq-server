PACKAGE=rabbit_management
DEPS=rabbitmq-mochiweb rabbitmq-server rabbitmq-erlang-client

TEST_APPS=mochiweb rabbit_mochiweb rabbit_management
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true
EXTRA_PACKAGE_DIRS=priv

TEMPLATES_DIR=templates
WEB_DIR=priv/www
JAVASCRIPT_DIR=$(WEB_DIR)/js
EXTRA_TARGETS=$(JAVASCRIPT_DIR)/templates.js \
    $(wildcard $(JAVASCRIPT_DIR)/*.js) \
    $(wildcard $(WEB_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/css/*.css) \

include ../include.mk

$(JAVASCRIPT_DIR)/templates.js: templates-codegen.py $(wildcard $(TEMPLATES_DIR)/*.html)
	./templates-codegen.py $(TEMPLATES_DIR) $@

clean::
	rm -f $(JAVASCRIPT_DIR)/templates.js

