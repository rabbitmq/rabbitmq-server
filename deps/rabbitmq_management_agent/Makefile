PACKAGE=rabbitmq-management-cluster-remote
APPNAME=rabbit_management_cluster_remote
DEPS=
INTERNAL_DEPS=
RUNTIME_DEPS=

TEST_APPS=
TEST_ARGS=
START_RABBIT_IN_TESTS=true
TEST_COMMANDS=

EXTRA_TARGETS=ebin/rabbit_mgmt_external_stats.beam \
	ebin/rabbit_mgmt_db_handler.beam

include ../include.mk

ebin/%.beam:
	cp ../rabbitmq-management/$@ $@
