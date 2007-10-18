EBIN_DIR=ebin
SOURCE_DIR=src
INCLUDE_DIR=include
ERLC_FLAGS=-W0

compile:
	mkdir -p $(EBIN_DIR)
	erlc +debug_info -I $(INCLUDE_DIR) -o $(EBIN_DIR) $(ERLC_FLAGS) $(SOURCE_DIR)/*.erl

all: compile

test_network: compile
	erl -pa ebin -noshell -eval 'network_client_test:test(),halt().'

test_network_coverage: compile
	erl -pa ebin -noshell -eval 'network_client_test:test_coverage(),halt().'

test_direct: compile
	echo 'direct_client_test:test_wrapper("rabbit-test"),halt().' | SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=/tmp/rabbitmq-test-mnesia RABBIT_ARGS="-s rabbit -pa ./ebin" NODENAME=rabbit-test rabbitmq-server

test_direct_coverage: compile
	echo 'direct_client_test:test_coverage("rabbit-test"),halt().' | SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=/tmp/rabbitmq-test-mnesia RABBIT_ARGS="-s rabbit -pa ./ebin" NODENAME=rabbit-test rabbitmq-server

clean:
	rm $(EBIN_DIR)/*.beam
