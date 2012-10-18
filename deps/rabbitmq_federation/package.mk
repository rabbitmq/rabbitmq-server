RELEASABLE:=true
DEPS:=rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=rabbit_federation_test_all:all_tests()
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
WITH_BROKER_SETUP_SCRIPTS:=$(PACKAGE_DIR)/etc/setup-rabbit-test.sh

$(PACKAGE_DIR)+pre-test::
	rm -rf tmp /tmp/rabbitmq-*-mnesia
	for R in hare flopsy mopsy cottontail dylan bugs jessica ; do \
	  erl_call -sname $$R -q ; \
	done
