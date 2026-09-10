PROJECT = rabbitmq_prelaunch
PROJECT_DESCRIPTION = RabbitMQ prelaunch setup
PROJECT_VERSION = 4.0.0
PROJECT_MOD = rabbit_prelaunch_app

<<<<<<< HEAD
DEPS = rabbit_common cuttlefish thoas osiris systemd
=======
DEPS = rabbit_common cuttlefish osiris systemd
TEST_DEPS = rabbitmq_ct_helpers
>>>>>>> 2bd427e (Fail the build on end_per_testcase/end_per_suite failures, not just testcase failures)

PLT_APPS += runtime_tools eunit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
