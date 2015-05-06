RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client rabbitmq-test
WITH_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/test.sh
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/test/ebin/test
WITH_BROKER_SETUP_SCRIPTS:=$(PACKAGE_DIR)/test/setup-rabbit-test.sh

define package_rules

$(PACKAGE_DIR)+pre-test::
	rm -rf $(PACKAGE_DIR)/test/certs
	mkdir $(PACKAGE_DIR)/test/certs
	mkdir -p $(PACKAGE_DIR)/test/ebin
	sed -E -e "s|%%CERTS_DIR%%|$(abspath $(PACKAGE_DIR))/test/certs|g" < $(PACKAGE_DIR)/test/src/test.config > $(PACKAGE_DIR)/test/ebin/test.config
	$(MAKE) -C $(PACKAGE_DIR)/../rabbitmq-test/certs all PASSWORD=bunnychow DIR=$(abspath $(PACKAGE_DIR))/test/certs

$(PACKAGE_DIR)+clean::
	rm -rf $(PACKAGE_DIR)/test/certs

endef
