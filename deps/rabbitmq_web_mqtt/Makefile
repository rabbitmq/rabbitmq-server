PROJECT = rabbitmq_web_mqtt
PROJECT_DESCRIPTION = RabbitMQ MQTT-over-WebSockets adapter
PROJECT_MOD = rabbit_web_mqtt_app

define PROJECT_ENV
[
	    {tcp_config, [{port, 15675}]},
	    {ssl_config, []},
	    {num_tcp_acceptors, 10},
	    {num_ssl_acceptors, 10},
	    {cowboy_opts, []},
	    {proxy_protocol, false}
	  ]
endef

# We do not need QUIC as dependency of emqtt.
BUILD_WITHOUT_QUIC=1
export BUILD_WITHOUT_QUIC

LOCAL_DEPS = ssl
DEPS = rabbit cowboy rabbitmq_mqtt
TEST_DEPS = emqtt rabbitmq_ct_helpers rabbitmq_ct_client_helpers rabbitmq_management rabbitmq_stomp rabbitmq_consistent_hash_exchange

PLT_APPS += rabbitmq_cli elixir cowlib ssl

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

dep_emqtt = git https://github.com/emqx/emqtt.git 1.11.0

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

CT_HOOKS = rabbit_ct_hook

# We are using mqtt_shared_SUITE from rabbitmq_mqtt.
CT_OPTS += -pa ../rabbitmq_mqtt/test/

test-build::
	$(verbose) $(MAKE) -C ../rabbitmq_mqtt test-dir
