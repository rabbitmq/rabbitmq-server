RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=rabbit_exchange_type_consistent_hash_test:test()
