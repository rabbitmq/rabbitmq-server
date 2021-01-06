# This is the list of Tier-1 plugins shipped with a release.
#
# IMPORTANT: This list is overriden in the server-release Concourse
# pipeline: Concourse takes all tested plugins and put them in the
# release. So if you want to add or remove a plugin to the distribution,
# you need to update the server-release pipeline!

PLUGINS := rabbitmq_amqp1_0 \
	   rabbitmq_auth_backend_cache \
	   rabbitmq_auth_backend_http \
	   rabbitmq_auth_backend_ldap \
	   rabbitmq_auth_backend_oauth2 \
	   rabbitmq_auth_mechanism_ssl \
	   rabbitmq_consistent_hash_exchange \
	   rabbitmq_event_exchange \
	   rabbitmq_federation \
	   rabbitmq_federation_management \
	   rabbitmq_jms_topic_exchange \
	   rabbitmq_management \
	   rabbitmq_management_agent \
	   rabbitmq_mqtt \
	   rabbitmq_peer_discovery_aws \
	   rabbitmq_peer_discovery_common \
	   rabbitmq_peer_discovery_consul \
	   rabbitmq_peer_discovery_etcd \
	   rabbitmq_peer_discovery_k8s \
	   rabbitmq_prometheus \
	   rabbitmq_random_exchange \
	   rabbitmq_recent_history_exchange \
	   rabbitmq_sharding \
	   rabbitmq_shovel \
	   rabbitmq_shovel_management \
	   rabbitmq_stomp \
	   rabbitmq_stream \
	   rabbitmq_stream_management \
	   rabbitmq_top \
	   rabbitmq_tracing \
	   rabbitmq_trust_store \
	   rabbitmq_web_dispatch \
	   rabbitmq_web_mqtt \
	   rabbitmq_web_mqtt_examples \
	   rabbitmq_web_stomp \
	   rabbitmq_web_stomp_examples
