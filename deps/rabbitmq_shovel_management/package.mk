RELEASABLE:=true
DEPS:=rabbitmq-management rabbitmq-shovel
WITH_BROKER_TEST_COMMANDS:=rabbit_shovel_mgmt_test_all:all_tests()
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test

CONSTRUCT_APP_PREREQS:=$(shell find $(PACKAGE_DIR)/priv -type f)
define construct_app_commands
	cp -r $(PACKAGE_DIR)/priv $(APP_DIR)
endef
