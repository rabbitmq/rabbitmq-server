RELEASABLE:=true
DEPS:=rabbitmq-web-dispatch webmachine-wrapper rabbitmq-server rabbitmq-erlang-client rabbitmq-management-agent
WITH_BROKER_TEST_COMMANDS:=rabbit_mgmt_test_all:all_tests()
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
WITH_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/src/rabbitmqadmin-test.py

CONSTRUCT_APP_PREREQS:=$(shell find $(PACKAGE_DIR)/priv -type f) $(PACKAGE_DIR)/bin/rabbitmqadmin
define construct_app_commands
	cp -r $(PACKAGE_DIR)/priv $(APP_DIR)
	sed 's/%%VSN%%/$(VERSION)/' $(PACKAGE_DIR)/bin/rabbitmqadmin > $(APP_DIR)/priv/www/cli/rabbitmqadmin
endef

# The tests require erlang/OTP R14 (httpc issue)
$(PACKAGE_DIR)+pre-test::
	if [ "`erl -noshell -eval 'io:format([list_to_integer(X) || X <- string:tokens(erlang:system_info(version), ".")] >= [5,8]), halt().'`" != true ] ; then \
	  echo "Need Erlang/OTP R14A or higher" ; \
	  exit 1 ; \
	fi