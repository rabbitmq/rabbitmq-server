DEPS:=rabbitmq-server rabbitmq-erlang-client
RETAIN_ORIGINAL_VERSION:=true

STANDALONE_TEST_COMMANDS:=rjms_topic_selector_unit_tests:test() sjx_parser_tests:test() sjx_evaluate_tests:test()
WITH_BROKER_TEST_COMMANDS:=rjms_topic_selector_tests:all_tests()

# Generated parser files:
PARSER_FILES:=sjx_parser.erl sjx_scanner.erl

PKG_DIR:=$(PACKAGE_DIR)/src
PKG_GEN:=$(addprefix $(PKG_DIR)/, $(PARSER_FILES))

# Generate the parser and hook into exchange build:
define package_rules

$(PACKAGE_DIR)+clean::
	-rm -rf $(PKG_GEN)

SOURCE_ERLS += $(PKG_GEN)

$(PACKAGE_DIR)+dist:: $(PKG_GEN)

$(PKG_DIR)/sjx_parser.erl : $(PKG_DIR)/sjx_parser.yrl
	erl -I -pa ebin -noshell -eval 'yecc:file("'$$(<)'", [warnings_as_errors, verbose]), halt().'

$(PKG_DIR)/sjx_scanner.erl : $(PKG_DIR)/sjx_scanner.xrl
	erl -I -pa ebin -noshell -eval 'leex:file("'$$(<)'", [warnings_as_errors, verbose]), halt().'

endef
