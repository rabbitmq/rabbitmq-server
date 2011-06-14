RELEASABLE:=true
DEPS:=rabbitmq-management

CONSTRUCT_APP_PREREQS:=$(shell find $(PACKAGE_DIR)/priv -type f)
define construct_app_commands
	cp -r $(PACKAGE_DIR)/priv $(APP_DIR)
endef
