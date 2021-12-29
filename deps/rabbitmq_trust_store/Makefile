PROJECT = rabbitmq_trust_store
PROJECT_DESCRIPTION = Client X.509 certificates trust store
PROJECT_MOD = rabbit_trust_store_app

define PROJECT_ENV
[
	    {default_refresh_interval, 30},
	    {providers, [rabbit_trust_store_file_provider]}
	  ]
endef

DEPS = rabbit_common rabbit
LOCAL_DEPS += ssl crypto public_key
## We need the Cowboy's test utilities
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client ct_helper trust_store_http
dep_ct_helper = git https://github.com/extend/ct_helper.git master
dep_trust_store_http = git https://github.com/rabbitmq/trust_store_http.git

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk
