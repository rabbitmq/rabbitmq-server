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
    $(wildcard $(JAVASCRIPT_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/*.html) \
    $(wildcard $(WEB_DIR)/css/*.css) \

#TODO get rid of
GENERATED_SOURCES=template

include ../include.mk

ERLTL_DIR=erltl
ERLTL_EBIN_DIR=$(ERLTL_DIR)/ebin
ERLTL_SRC_DIR=$(ERLTL_DIR)/src

$(ERLTL_EBIN_DIR)/%.beam: $(ERLTL_SRC_DIR)/%.erl
	@mkdir -p $(ERLTL_EBIN_DIR)
	$(ERLC) -o $(ERLTL_EBIN_DIR) -Wall +debug_info $<

$(EBIN_DIR)/template.beam: $(SOURCE_DIR)/template.et $(ERLTL_EBIN_DIR)/erltl.beam
	$(ERL) -I -pa $(ERLTL_EBIN_DIR) -noshell -eval 'erltl:compile("$(SOURCE_DIR)/template.et", [{outdir, "$(EBIN_DIR)"}, report_errors, report_warnings, nowarn_unused_vars]), halt().'

$(JAVASCRIPT_DIR)/templates.js: templates-codegen.py $(wildcard $(TEMPLATES_DIR)/*.html)
	./templates-codegen.py $(TEMPLATES_DIR) $@

clean::
	rm -f $(ERLTL_EBIN_DIR)/*.beam
	rm -f $(JAVASCRIPT_DIR)/templates.js

