RELEASABLE:=true
DEPS:=rabbitmq-mochiweb webmachine-wrapper rabbitmq-server rabbitmq-erlang-client rabbitmq-management-agent
TEST_COMMANDS:=rabbit_mgmt_test_all:all_tests()
EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv
EXTRA_TARGETS:=$(shell find $(EXTRA_PACKAGE_DIRS) -type f) $(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin

define package_targets

$(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin: $(PACKAGE_DIR)/bin/rabbitmqadmin
	cp $$< $$@

# Check the erlang version before running tests (httpc issue)
$(PACKAGE_DIR)+pre-test-checks::
	if [ "`erl -noshell -eval 'io:format(lists:map(fun erlang:list_to_integer/1, string:tokens(erlang:system_info(version), ".")) >= [5,8]),halt().'`" != true ] ; then \
	  echo "Need Erlang/OTP R14A or higher to run tests" ; \
	  exit 1 ; \
	fi

$(PACKAGE_DIR)+clean::
	rm -f $(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin

endef
