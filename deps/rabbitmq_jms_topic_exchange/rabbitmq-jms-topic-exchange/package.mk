DEPS:=rabbitmq-server rabbitmq-erlang-client
RETAIN_ORIGINAL_VERSION:=true

STANDALONE_TEST_COMMANDS:=rjms_topic_selector_unit_tests:test() \
                          sjx_evaluate_tests:test()

WITH_BROKER_TEST_COMMANDS:=rjms_topic_selector_tests:all_tests()
