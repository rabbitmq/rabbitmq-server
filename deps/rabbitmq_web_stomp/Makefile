PROJECT = rabbitmq_web_stomp
PROJECT_DESCRIPTION = Rabbit WEB-STOMP - WebSockets to Stomp adapter
PROJECT_MOD = rabbit_ws_app

define PROJECT_ENV
[
	    {tcp_config, []},
	    {num_tcp_acceptors, 10},
	    {ssl_config, []},
	    {num_ssl_acceptors, 1},
	    {cowboy_opts, []},
	    {ws_frame, text},
	    {use_http_auth, false}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = cowboy rabbit_common rabbit rabbitmq_stomp
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
