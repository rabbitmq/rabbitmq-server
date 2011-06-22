DEPS:=rabbitmq-erlang-client
WITH_BROKER_TEST_COMMANDS:=eunit:test(rabbit_federation_unit_test,[verbose]) eunit:test(rabbit_federation_test,[verbose])
WITH_BROKER_TEST_CONFIG:=$(PACKAGE_DIR)/etc/rabbit-test

$(PACKAGE_DIR)+pre-test::
	rm -rf tmp /tmp/rabbitmq-*-mnesia
	for R in hare bunny lapin flopsy mopsy cottontail ; do \
	  erl_call -sname $$R -q ; \
	done

