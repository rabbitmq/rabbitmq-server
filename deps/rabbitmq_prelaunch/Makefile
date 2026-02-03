PROJECT = rabbitmq_prelaunch
PROJECT_DESCRIPTION = RabbitMQ prelaunch setup
PROJECT_VERSION = 4.0.0
PROJECT_MOD = rabbit_prelaunch_app

DEPS = rabbit_common cuttlefish thoas osiris systemd

PLT_APPS += runtime_tools eunit

# Export `DEP_PLUGINS` to force the rabbitmq-old-compiler.mk plugin on
# dependencies.
export DEP_PLUGINS = rabbit_common/mk/rabbitmq-old-compiler.mk

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-old-compiler.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
