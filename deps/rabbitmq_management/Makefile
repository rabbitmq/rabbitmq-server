RELEASABLE:=true
DEPS:=rabbitmq-mochiweb webmachine-wrapper rabbitmq-server rabbitmq-erlang-client
TEST_COMMANDS:=eunit:test(rabbit_mgmt_test_unit,[verbose]) rabbit_mgmt_test_db:test()
EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv
EXTRA_TARGETS:=$(shell find $(EXTRA_PACKAGE_DIRS) -type f)

include ../include.mk
