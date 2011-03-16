PACKAGE=rabbit_stomp
APPNAME=rabbit_stomp
DEPS=rabbitmq-server rabbitmq-erlang-client

START_RABBIT_IN_TESTS=true
TEST_APPS=rabbit_stomp
UNIT_TEST_COMMANDS=eunit:test([rabbit_stomp_test_util,rabbit_stomp_test_frame],[verbose])

RABBITMQ_TEST_PATH=../../rabbitmq-test
CERTS_DIR:=$(abspath certs)

TEST_SCRIPTS=./test/test.py

include ../include.mk

testdeps:
	make -C deps/stomppy

test: unittest testdeps $(CERTS_DIR)

unittest: $(TARGETS) $(TEST_TARGETS)
	ERL_LIBS=$(LIBS_PATH) $(ERL) $(TEST_LOAD_PATH) \
		$(foreach CMD,$(UNIT_TEST_COMMANDS),-eval '$(CMD)') \
		-eval 'init:stop()' | tee $(TMPDIR)/rabbit-stomp-unittest-output |\
			egrep "passed" >/dev/null


CAN_RUN_SSL=$(shell if [ -d $(RABBITMQ_TEST_PATH) ]; then echo "true"; else echo "false"; fi)

ssl_clean:
	rm -rf $(CERTS_DIR)

SSL_VERIFY:=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.7.0" ]; then echo "true"; else echo "false"; fi)
ifeq (true,$(SSL_VERIFY))
SSL_VERIFY_OPTION :={verify,verify_peer},{fail_if_no_peer_cert,false}
else
SSL_VERIFY_OPTION :={verify_code,1}
endif

ifeq ($(CAN_RUN_SSL),true)

TEST_SCRIPTS += ./test/test_ssl.py

TEST_ARGS := -rabbit_stomp ssl_listeners [61614] -rabbit ssl_options ['{cacertfile,'\"$(CERTS_DIR)/testca/cacert.pem\"'},{certfile,'\"$(CERTS_DIR)/server/cert.pem\"'},{keyfile,'\"$(CERTS_DIR)/server/key.pem\"'},$(SSL_VERIFY_OPTION)']

$(CERTS_DIR):
	mkdir -p $(CERTS_DIR)
	make -C $(RABBITMQ_TEST_PATH)/certs PASSWORD=test DIR=$(abspath $(CERTS_DIR))
else

$(CERTS_DIR):
	mkdir -p $(CERTS_DIR)
	touch $(CERTS_DIR)/.ssl_skip

endif

