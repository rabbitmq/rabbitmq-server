DEPS:=rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_federation_unit_test,[verbose]) eunit:test(rabbit_federation_test,[verbose])
WITH_BROKER_TEST_CONFIG:=etc/test-main
