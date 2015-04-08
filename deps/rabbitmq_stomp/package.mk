RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client rabbitmq-test
STANDALONE_TEST_COMMANDS:=eunit:test([rabbit_stomp_test_util,rabbit_stomp_test_frame],[verbose])
WITH_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/src/test.py $(PACKAGE_DIR)/test/src/test_connect_options.py $(PACKAGE_DIR)/test/src/test_ssl.py
WITH_BROKER_TEST_COMMANDS:=rabbit_stomp_test:all_tests() rabbit_stomp_amqqueue_test:all_tests()
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/test/ebin/test

define package_rules

$(PACKAGE_DIR)+pre-test::
	rm -rf $(PACKAGE_DIR)/test/certs
	mkdir $(PACKAGE_DIR)/test/certs
	mkdir -p $(PACKAGE_DIR)/test/ebin
	sed -e "s|%%CERTS_DIR%%|$(abspath $(PACKAGE_DIR))/test/certs|g" < $(PACKAGE_DIR)/test/src/test.config > $(PACKAGE_DIR)/test/ebin/test.config
	$(MAKE) -C $(PACKAGE_DIR)/../rabbitmq-test/certs all PASSWORD=test DIR=$(abspath $(PACKAGE_DIR))/test/certs
	$(MAKE) -C $(PACKAGE_DIR)/deps/stomppy

$(PACKAGE_DIR)+clean::
	rm -rf $(PACKAGE_DIR)/test/certs

$(PACKAGE_DIR)+clean-with-deps::
	$(MAKE) -C $(PACKAGE_DIR)/deps/stomppy distclean

endef
