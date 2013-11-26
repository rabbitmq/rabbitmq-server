RELEASABLE:=true
DEPS:=rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=rabbit_shovel_test_all:all_tests()
# To make test_dyn:plugin_dir() work - TODO could this be better?
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
