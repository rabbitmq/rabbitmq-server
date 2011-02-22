PACKAGE=rabbitmq-federation
APPNAME=rabbit_federation
DEPS=rabbitmq-server rabbitmq-erlang-client

TEST_APPS=amqp_client rabbit_federation
TEST_ARGS=-rabbit_federation exchanges '[[{exchange, "downstream-conf"}, {vhost, "/"}, {upstreams, ["amqp://localhost/%2f/upstream-conf"]}, {type, "topic"}]]'
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=eunit:test(rabbit_federation_unit_test,[verbose]) eunit:test(rabbit_federation_test,[verbose])

OTHER_NODE=bunny
OTHER_PORT=5673

include ../include.mk

test: cleantest stop-other-node

cleantest:
	rm -rf tmp /tmp/rabbitmq-$(OTHER_NODE)-mnesia

start-other-node:
	RABBITMQ_MNESIA_BASE=/tmp/rabbitmq-$(OTHER_NODE)-mnesia \
	RABBITMQ_LOG_BASE=/tmp \
	RABBITMQ_NODENAME=$(OTHER_NODE) \
	RABBITMQ_NODE_PORT=$(OTHER_PORT) \
	RABBITMQ_SERVER_ERL_ARGS="-rabbit_mochiweb port 5$(OTHER_PORT)" \
	../rabbitmq-server/scripts/rabbitmq-server -detached
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) wait

stop-other-node:
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) stop || true
