DEPS:=rabbitmq-server rabbitmq-erlang-client
RETAIN_ORIGINAL_VERSION:=true
WITH_BROKER_TEST_COMMANDS:=rabbit_exchange_type_recent_history_test:test()
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
