RELEASABLE:=true
DEPS:=rabbitmq-mochiweb webmachine-wrapper rabbitmq-server rabbitmq-erlang-client rabbitmq-management-agent
TEST_COMMANDS:=rabbit_mgmt_test_all:all_tests()
EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv
EXTRA_TARGETS:=$(shell find $(EXTRA_PACKAGE_DIRS) -type f) $(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin

$(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin: $(PACKAGE_DIR)/bin/rabbitmqadmin
	cp $< $@
