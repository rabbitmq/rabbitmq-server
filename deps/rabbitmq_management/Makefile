DEPS:=rabbitmq-mochiweb webmachine-wrapper rabbitmq-server rabbitmq-erlang-client

TEST_APPS=crypto inets mochiweb rabbit_mochiweb rabbit_management amqp_client
TEST_ARGS=-rabbit_mochiweb port 55672
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=eunit:test(rabbit_mgmt_test_unit,[verbose]) rabbit_mgmt_test_db:test()

EXTRA_PACKAGE_DIRS:=$(PACKAGE_DIR)/priv

EXTRA_TARGETS:=$(shell find $(EXTRA_PACKAGE_DIRS) -type f)

test: cleantest

cleantest:
	rm -rf tmp

include ../include.mk
