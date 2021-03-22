PROJECT = rabbitmq_mqtt
PROJECT_DESCRIPTION = RabbitMQ MQTT Adapter
PROJECT_MOD = rabbit_mqtt

define PROJECT_ENV
[
	    {default_user, <<"guest">>},
	    {default_pass, <<"guest">>},
	    {ssl_cert_login,false},
	    %% To satisfy an unfortunate expectation from popular MQTT clients.
	    {allow_anonymous, true},
	    {vhost, <<"/">>},
	    {exchange, <<"amq.topic">>},
	    {subscription_ttl, 86400000}, %% 24 hours
	    {retained_message_store, rabbit_mqtt_retained_msg_store_dets},
	    %% only used by DETS store
	    {retained_message_store_dets_sync_interval, 2000},
	    {prefetch, 10},
	    {ssl_listeners, []},
	    {tcp_listeners, [1883]},
	    {num_tcp_acceptors, 10},
	    {num_ssl_acceptors, 10},
	    {tcp_listen_options, [{backlog,   128},
	                          {nodelay,   true}]},
	    {proxy_protocol, false},
	    {sparkplug, false}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = ranch rabbit_common rabbit amqp_client ra
TEST_DEPS = emqttc ct_helper rabbitmq_ct_helpers rabbitmq_ct_client_helpers

dep_ct_helper = git https://github.com/extend/ct_helper.git master
dep_emqttc = git https://github.com/rabbitmq/emqttc.git remove-logging

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk


clean::
	if test -d test/java_SUITE_data; then cd test/java_SUITE_data && $(MAKE) clean; fi
