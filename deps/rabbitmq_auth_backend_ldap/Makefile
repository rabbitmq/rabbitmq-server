PROJECT = rabbitmq_auth_backend_ldap
PROJECT_DESCRIPTION = RabbitMQ LDAP Authentication Backend
PROJECT_MOD = rabbit_auth_backend_ldap_app

# Note:
# Use of these default values in calls to get_expected_env_str/2
define PROJECT_ENV
[
	    {servers,               undefined},
	    {user_bind_pattern,     none},
	    {user_dn_pattern,       "$${username}"},
	    {dn_lookup_attribute,   none},
	    {dn_lookup_base,        none},
	    {group_lookup_base,     none},
	    {dn_lookup_bind,        as_user},
	    {other_bind,            as_user},
	    {anon_auth,             false},
	    {vhost_access_query,    {constant, true}},
	    {resource_access_query, {constant, true}},
	    {topic_access_query,    {constant, true}},
	    {tag_queries,           [{administrator, {constant, false}}]},
	    {use_ssl,               false},
	    {use_starttls,          false},
	    {ssl_options,           []},
	    {port,                  389},
	    {timeout,               infinity},
	    {log,                   false},
	    {pool_size,             64},
	    {idle_timeout,          300000}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

LOCAL_DEPS = eldap
DEPS = rabbit_common rabbit
TEST_DEPS = ct_helper rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client
dep_ct_helper = git https://github.com/extend/ct_helper.git master

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk
