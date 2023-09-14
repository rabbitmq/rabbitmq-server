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
	    {max_session_expiry_interval_seconds, 86400}, %% 1 day
	    {retained_message_store, rabbit_mqtt_retained_msg_store_dets},
	    %% only used by DETS store
	    {retained_message_store_dets_sync_interval, 2000},
	    {prefetch, 10},
	    {ssl_listeners, []},
	    {tcp_listeners, [1883]},
	    {num_tcp_acceptors, 10},
	    {num_ssl_acceptors, 10},
	    {tcp_listen_options, [{backlog,   128},
	                          {nodelay,   true},
	                          {send_timeout, 15000},
	                          {send_timeout_close, true}
	                         ]},
	    {proxy_protocol, false},
	    {sparkplug, false},
	    {mailbox_soft_limit, 200},
	    {max_packet_size_unauthenticated, 65536},
	    %% 256 MB is upper limit defined by MQTT spec
	    {max_packet_size_authenticated, 268435455},
	    {topic_alias_maximum, 16}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

# We do not need QUIC as dependency of emqtt.
BUILD_WITHOUT_QUIC=1
export BUILD_WITHOUT_QUIC

LOCAL_DEPS = ssl
DEPS = ranch rabbit_common rabbit ra amqp10_common
TEST_DEPS = emqtt ct_helper rabbitmq_ct_helpers rabbitmq_ct_client_helpers rabbitmq_management rabbitmq_web_mqtt amqp_client rabbitmq_consistent_hash_exchange rabbitmq_amqp1_0 amqp10_client rabbitmq_stomp rabbitmq_stream

dep_ct_helper = git https://github.com/extend/ct_helper.git master
dep_emqtt = git https://github.com/rabbitmq/emqtt.git master

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk


clean::
	if test -d test/java_SUITE_data; then cd test/java_SUITE_data && $(MAKE) clean; fi
