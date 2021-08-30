PROJECT = rabbitmq_web_mqtt_examples
PROJECT_DESCRIPTION = Rabbit WEB-MQTT - examples
PROJECT_MOD = rabbit_web_mqtt_examples_app

define PROJECT_ENV
[
	    {listener, [{port, 15670}]}
	  ]
endef

DEPS = rabbit_common rabbit rabbitmq_web_dispatch rabbitmq_web_mqtt

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk
