RELEASABLE:=true
DEPS:=rabbitmq-mochiweb webmachine-wrapper rabbitmq-server rabbitmq-erlang-client rabbitmq-management-agent
IN_BROKER_TEST_COMMANDS:=rabbit_mgmt_test_all:all_tests()
EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv
EXTRA_TARGETS:=$(shell find $(EXTRA_PACKAGE_DIRS) -type f) $(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin

define package_targets

$(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin: $(PACKAGE_DIR)/bin/rabbitmqadmin
	cp $$< $$@

# The tests require erlang/OTP R14 (httpc issue)
$(PACKAGE_DIR)+pre-test:: assert-erlang-r14

$(PACKAGE_DIR)+clean::
	rm -f $(PACKAGE_DIR)/priv/www-cli/rabbitmqadmin

endef
