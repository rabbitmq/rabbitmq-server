RELEASABLE:=true
DEPS:=rabbitmq-erlang-client rabbitmq-test
FILTER:=all
COVER:=false
WITH_BROKER_TEST_COMMANDS:=rabbit_test_runner:run_in_broker(\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\")
WITH_BROKER_SETUP_SCRIPTS:=$(PACKAGE_DIR)/etc/setup-rabbit-test.sh
STANDALONE_TEST_COMMANDS:=rabbit_test_runner:run_multi(\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\",$(COVER),\"/tmp/rabbitmq-multi-node/plugins\")

$(PACKAGE_DIR)+pre-test::
	rm -rf /tmp/rabbitmq-multi-node/plugins
	mkdir -p /tmp/rabbitmq-multi-node/plugins/plugins
	cp -p dist/*.ez /tmp/rabbitmq-multi-node/plugins/plugins
