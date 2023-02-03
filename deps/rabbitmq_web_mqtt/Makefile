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

DEPS = rabbit_common rabbit cowboy rabbitmq_mqtt
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

<<<<<<< HEAD
=======
dep_emqtt = git https://github.com/emqx/emqtt.git 1.7.0-rc.3

>>>>>>> 2d0826c335 (Add OAuth 2.0 MQTT system test)
DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
