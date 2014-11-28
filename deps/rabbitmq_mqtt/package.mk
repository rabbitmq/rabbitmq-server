RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client

RABBITMQ_TEST_PATH=$(PACKAGE_DIR)/../rabbitmq-test
ABS_PACKAGE_DIR:=$(abspath $(PACKAGE_DIR))

CERTS_DIR:=$(ABS_PACKAGE_DIR)/test/certs
CAN_RUN_SSL:=$(shell if [ -d $(RABBITMQ_TEST_PATH) ]; then echo "true"; else echo "false"; fi)

TEST_CONFIG_PATH_MQTT=$(TEST_EBIN_DIR)/test.config
WITH_BROKER_TEST_CONFIG:=$(TEST_EBIN_DIR)/test
WITH_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/test.sh


.PHONY: $(TEST_CONFIG_PATH_MQTT)

ifeq ($(CAN_RUN_SSL),true)

WITH_BROKER_SETUP_SCRIPTS:=$(ABS_PACKAGE_DIR)/test/setup-rabbit-test.sh

$(TEST_CONFIG_PATH_MQTT): $(CERTS_DIR) $(ABS_PACKAGE_DIR)/test/src/ssl.config
	sed -e "s|%%CERTS_DIR%%|$(CERTS_DIR)|g" < $(ABS_PACKAGE_DIR)/test/src/ssl.config > $@
	@echo "\nRunning SSL tests\n"

$(CERTS_DIR):
	mkdir	-p $(ABS_PACKAGE_DIR)/test/ebin
	mkdir -p $(CERTS_DIR)
	make -C $(RABBITMQ_TEST_PATH)/certs all PASSWORD=bunnychow DIR=$(CERTS_DIR)

else
$(TEST_CONFIG_PATH_MQTT): $(ABS_PACKAGE_DIR)/test/src/non_ssl.config
	cp $(ABS_PACKAGE_DIR)/test/src/non_ssl.config $@
	@echo "\nNOT running SSL tests - looked in $(RABBITMQ_TEST_PATH) \n"

endif

define package_rules
$(PACKAGE_DIR)+pre-test:: $(TEST_CONFIG_PATH_MQTT)



$(PACKAGE_DIR)+clean::
	rm -rf $(CERTS_DIR)

endef
