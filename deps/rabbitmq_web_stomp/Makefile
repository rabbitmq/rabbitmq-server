PROJECT = rabbitmq_web_stomp
PROJECT_DESCRIPTION = RabbitMQ STOMP-over-WebSockets support
PROJECT_MOD = rabbit_web_stomp_app

define PROJECT_ENV
[
	    {tcp_config, [{port, 15674}]},
	    {ssl_config, []},
	    {num_tcp_acceptors, 10},
	    {num_ssl_acceptors, 10},
	    {cowboy_opts, []},
	    {proxy_protocol, false},
	    {ws_frame, text},
	    {use_http_auth, false}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = cowboy rabbit_common rabbit rabbitmq_stomp
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk
