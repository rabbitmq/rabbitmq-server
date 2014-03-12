RELEASABLE:=true
DEPS:=rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_exchange_type_event_test,[verbose])

