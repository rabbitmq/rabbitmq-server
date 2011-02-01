RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-erlang-client
STANDALONE_TEST_COMMANDS:=eunit:test([rabbit_stomp_test_util,rabbit_stomp_test_frame],[verbose])
IN_BROKER_TEST_SCRIPTS:=$(PACKAGE_DIR)/test/src/test.py

define package_targets

$(PACKAGE_DIR)+pre-test::
	make -C $(PACKAGE_DIR)/deps/stomppy

endef