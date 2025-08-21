PROJECT = rabbitmq_auth_backend_ldap_management
PROJECT_DESCRIPTION = Management extension for the LDAP plugin

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

LOCAL_DEPS = eldap public_key
DEPS = rabbit_common rabbit rabbitmq_management rabbitmq_auth_backend_ldap
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
