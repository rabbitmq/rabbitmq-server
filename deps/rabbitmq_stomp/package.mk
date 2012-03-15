RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client
STANDALONE_TEST_COMMANDS:=eunit:test([rabbit_stomp_test_util,rabbit_stomp_test_frame],[verbose])
WITH_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/src/test.py $(PACKAGE_DIR)/test/src/test_connect_options.py
WITH_BROKER_TEST_COMMANDS:=rabbit_stomp_test:all_tests() rabbit_stomp_amqqueue_test:all_tests()

RABBITMQ_TEST_PATH=$(PACKAGE_DIR)/../../rabbitmq-test
ABS_PACKAGE_DIR:=$(abspath $(PACKAGE_DIR))

CERTS_DIR:=$(ABS_PACKAGE_DIR)/test/certs
CAN_RUN_SSL:=$(shell if [ -d $(RABBITMQ_TEST_PATH) ]; then echo "true"; else echo "false"; fi)

TEST_CONFIG_PATH=$(TEST_EBIN_DIR)/test.config
WITH_BROKER_TEST_CONFIG:=$(TEST_EBIN_DIR)/test

.PHONY: $(TEST_CONFIG_PATH)

ifeq ($(CAN_RUN_SSL),true)

WITH_BROKER_TEST_SCRIPTS += $(PACKAGE_DIR)/test/src/test_ssl.py

$(TEST_CONFIG_PATH): $(CERTS_DIR) $(ABS_PACKAGE_DIR)/test/src/ssl.config
	sed -e "s|%%CERTS_DIR%%|$(CERTS_DIR)|g" < $(ABS_PACKAGE_DIR)/test/src/ssl.config > $@
	@echo "\nRunning SSL tests\n"

$(CERTS_DIR):
	mkdir -p $(CERTS_DIR)
	make -C $(RABBITMQ_TEST_PATH)/certs all PASSWORD=test DIR=$(CERTS_DIR)

else
$(TEST_CONFIG_PATH): $(ABS_PACKAGE_DIR)/test/src/non_ssl.config
	cp $(ABS_PACKAGE_DIR)/test/src/non_ssl.config $@
	@echo "\nNOT running SSL tests - looked in $(RABBITMQ_TEST_PATH) \n"

endif

define package_rules

$(PACKAGE_DIR)+pre-test:: $(TEST_CONFIG_PATH)
	make -C $(PACKAGE_DIR)/deps/stomppy

$(PACKAGE_DIR)+clean::
	rm -rf $(CERTS_DIR)

$(PACKAGE_DIR)+clean-with-deps::
	make -C $(PACKAGE_DIR)/deps/stomppy distclean

endef