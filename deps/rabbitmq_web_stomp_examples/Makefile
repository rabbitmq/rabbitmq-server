PROJECT = rabbitmq_web_stomp_examples
PROJECT_DESCRIPTION = Rabbit WEB-STOMP - examples
PROJECT_MOD = rabbit_web_stomp_examples_app

define PROJECT_ENV
[
	    {listener, [{port, 15670}]}
	  ]
endef

DEPS = rabbit_common rabbit rabbitmq_web_dispatch rabbitmq_web_stomp

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
include ../../github-actions.mk
