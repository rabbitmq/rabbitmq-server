RELEASABLE:=true
DEPS:=rabbitmq-mochiweb webmachine-wrapper rabbitmq-server rabbitmq-erlang-client
TEST_COMMANDS:=rabbit_mgmt_test_all:all_tests()
EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv
EXTRA_TARGETS:=$(shell find $(EXTRA_PACKAGE_DIRS) -type f)
