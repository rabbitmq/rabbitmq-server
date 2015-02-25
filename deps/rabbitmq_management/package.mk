RELEASABLE:=true
DEPS:=rabbitmq-web-dispatch webmachine-wrapper rabbitmq-server rabbitmq-erlang-client rabbitmq-management-agent rabbitmq-test
FILTER:=all
COVER:=false
WITH_BROKER_TEST_COMMANDS:=rabbit_test_runner:run_in_broker(\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\")
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
STANDALONE_TEST_COMMANDS:=rabbit_test_runner:run_multi(\"$(UMBRELLA_BASE_DIR)/rabbitmq-server\",\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\",$(COVER),\"/tmp/rabbitmq-multi-node/plugins\")
WITH_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/src/rabbitmqadmin-test-wrapper.sh

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
	rm -rf /tmp/rabbitmq-multi-node/plugins
	mkdir -p /tmp/rabbitmq-multi-node/plugins/plugins
	cp -p $(UMBRELLA_BASE_DIR)/rabbitmq-management/dist/*.ez /tmp/rabbitmq-multi-node/plugins/plugins

