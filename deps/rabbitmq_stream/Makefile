PROJECT = rabbitmq_stream
PROJECT_DESCRIPTION = RabbitMQ Stream
PROJECT_MOD = rabbit_stream

define PROJECT_ENV
[
	{tcp_listeners, [5552]},
	{num_tcp_acceptors, 10},
	{tcp_listen_options, [{backlog,   128},
                          {nodelay,   true}]},
	{ssl_listeners, []},
	{num_ssl_acceptors, 10},
	{ssl_listen_options, []},
	{initial_credits, 50000},
	{credits_required_for_unblocking, 12500},
	{frame_max, 1048576},
	{max_uncompressed_sub_entry_batch_size, 67108864},
	{heartbeat, 60},
	{advertised_host, undefined},
	{advertised_port, undefined}
]
endef


LOCAL_DEPS = ssl
DEPS = rabbit rabbitmq_stream_common osiris ranch
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client amqp10_client

PLT_APPS += rabbitmq_cli $(ELIXIR_ERL_EBIN) ssl

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

ELIXIR = system

include ../../rabbitmq-components.mk
include ../../erlang.mk

ELIXIR_ERL_EBIN := $(shell dirname $$(elixir -e 'IO.puts(:code.which(:elixir_erl))' 2>/dev/null))
DIALYZER_PLT_OPTS += -pa $(ELIXIR_ERL_EBIN)
DIALYZER_OPTS += -pa $(ELIXIR_ERL_EBIN)
