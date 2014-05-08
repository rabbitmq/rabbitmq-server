RELEASABLE:=true
DEPS:=rabbitmq-erlang-client rabbitmq-test
# WITH_BROKER_TEST_COMMANDS:=rabbit_federation_test_all:all_tests()
# WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test
# WITH_BROKER_SETUP_SCRIPTS:=$(PACKAGE_DIR)/etc/setup-rabbit-test.sh
FILTER:=all
COVER:=false
STANDALONE_TEST_COMMANDS:=rabbit_test_runner:run_multi(\"$(PACKAGE_DIR)/test/ebin\",\"$(FILTER)\",$(COVER),\"/tmp/rabbitmq-multi-node/plugins\")

$(PACKAGE_DIR)+pre-test::
	rm -rf /tmp/rabbitmq-multi-node/plugins
	mkdir -p /tmp/rabbitmq-multi-node/plugins/plugins
	cp -p dist/*.ez /tmp/rabbitmq-multi-node/plugins/plugins
	# rm -rf tmp /tmp/rabbitmq-*-mnesia
	# for R in hare flopsy mopsy cottontail dylan bugs jessica cycle1 cycle2; do \
	#   erl_call -sname $$R -q ; \
	# done
