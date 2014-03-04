DEPS:=rabbitmq-erlang-client rabbitmq-consistent-hash-exchange random-exchange

WITH_BROKER_TEST_COMMANDS:=rabbit_sharding_test_all:all_tests()
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test

$(PACKAGE_DIR)+pre-test::
	rm -rf ${TMPDIR}/rabbitmq-sharding-tests
