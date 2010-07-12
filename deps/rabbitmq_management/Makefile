PACKAGE=rabbit_status
DEPS=rabbitmq-mochiweb rabbitmq-server rabbitmq-erlang-client

TEST_APPS=mochiweb rabbit_mochiweb rabbit_status
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true

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

clean::
	rm -f $(ERLTL_EBIN_DIR)/*.beam
