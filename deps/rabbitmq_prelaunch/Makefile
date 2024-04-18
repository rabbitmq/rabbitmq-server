PROJECT = rabbitmq_prelaunch
PROJECT_DESCRIPTION = RabbitMQ prelaunch setup
PROJECT_VERSION = 1.0.0
PROJECT_MOD = rabbit_prelaunch_app

DEPS = rabbit_common cuttlefish thoas

PLT_APPS += runtime_tools eunit osiris systemd

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
include ../../github-actions.mk
