PROJECT = rabbitmq_auth_backend_http
PROJECT_DESCRIPTION = RabbitMQ HTTP Authentication Backend
PROJECT_MOD = rabbit_auth_backend_http_app

define PROJECT_ENV
[
	    {http_method,   get},
	    {user_path,     "http://localhost:8000/auth/user"},
	    {vhost_path,    "http://localhost:8000/auth/vhost"},
	    {resource_path, "http://localhost:8000/auth/resource"}
	  ]
endef

LOCAL_DEPS = inets
DEPS = rabbit_common rabbit amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
