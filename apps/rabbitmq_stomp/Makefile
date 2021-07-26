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

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

LOCAL_DEPS = rabbit_common rabbit amqp_client
DEPS = ranch
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
