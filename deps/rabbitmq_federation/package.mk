RELEASABLE:=true
DEPS:=rabbitmq-erlang-client rabbitmq-test
FILTER:=all
COVER:=false
WITH_BROKER_TEST_COMMANDS:=rabbit_test_runner:run_in_broker(\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\")
WITH_BROKER_SETUP_SCRIPTS:=$(PACKAGE_DIR)/etc/setup-rabbit-test.sh
STANDALONE_TEST_COMMANDS:=rabbit_test_runner:run_multi(\"$(UMBRELLA_BASE_DIR)/rabbitmq-server\",\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\",$(COVER),\"/tmp/rabbitmq-multi-node/plugins\")

# NB: we cannot use PACKAGE_DIR in the body of this rule as it gets
# expanded at the wrong time and set to the value of a completely
# arbitrary package!
$(PACKAGE_DIR)+pre-test:: $(PACKAGE_DIR)+dist
	rm -rf /tmp/rabbitmq-multi-node/plugins
	mkdir -p /tmp/rabbitmq-multi-node/plugins/plugins
	cp -p $(UMBRELLA_BASE_DIR)/rabbitmq-federation/dist/*.ez /tmp/rabbitmq-multi-node/plugins/plugins
