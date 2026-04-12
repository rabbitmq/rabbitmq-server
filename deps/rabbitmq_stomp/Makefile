PROJECT = rabbitmq_stomp
PROJECT_DESCRIPTION = RabbitMQ STOMP plugin
PROJECT_MOD = rabbit_stomp

define PROJECT_ENV
[
	    {default_user,
	     [{login, <<"guest">>},
	      {passcode, <<"guest">>}]},
	    {default_vhost, <<"/">>},
	    {default_topic_exchange, <<"amq.topic">>},
	    {default_nack_requeue, true},
	    {ssl_cert_login, false},
	    {implicit_connect, false},
	    {tcp_listeners, [61613]},
	    {ssl_listeners, []},
	    {num_tcp_acceptors, 10},
	    {num_ssl_acceptors, 10},
	    {tcp_listen_options, [{backlog,   128},
	                          {nodelay,   true}]},
	    %% see rabbitmq/rabbitmq-stomp#39
	    {trailing_lf, true},
	    %% see rabbitmq/rabbitmq-stomp#57
	    {hide_server_info, false},
	    {proxy_protocol, false}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = ranch rabbit_common rabbit
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers rabbitmq_management proper

PLT_APPS += rabbitmq_cli elixir ssl

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

# Regenerate per-function CT test cases from Python test files.
# The generated .hrl is committed; this rule updates it when Python sources change.
# Runs only when python3 is available.
test/python_SUITE_generated.hrl:: $(wildcard test/python_SUITE_data/src/*.py) test/generate_python_tests.py
	$(if $(shell which python3 2>/dev/null),python3 test/generate_python_tests.py test/python_SUITE_data/src $@,@echo "python3 not found, using committed $@")
