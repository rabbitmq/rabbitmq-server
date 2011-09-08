RELEASABLE:=true
DEPS:=rabbitmq-management
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_tracing_test,[verbose])

CONSTRUCT_APP_PREREQS:=$(shell find $(PACKAGE_DIR)/priv -type f)
define construct_app_commands
	cp -r $(PACKAGE_DIR)/priv $(APP_DIR)
endef
