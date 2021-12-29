VENDORED_COMPONENTS = rabbit_common \
	rabbit \
	amqp_client \
	amqp10_client \
	amqp10_common \
	rabbitmq_amqp1_0 \
	rabbitmq_auth_backend_cache \
	rabbitmq_auth_backend_http \
	rabbitmq_auth_backend_ldap \
	rabbitmq_auth_backend_oauth2 \
	rabbitmq_auth_mechanism_ssl \
	rabbitmq_aws \
	rabbitmq_cli \
	rabbitmq_codegen \
	rabbitmq_consistent_hash_exchange \
	rabbitmq_event_exchange \
	rabbitmq_federation \
	rabbitmq_federation_management \
	rabbitmq_jms_topic_exchange \
	rabbitmq_management \
	rabbitmq_management_agent \
	rabbitmq_mqtt \
	rabbitmq_peer_discovery_common \
	rabbitmq_peer_discovery_aws \
	rabbitmq_peer_discovery_k8s \
	rabbitmq_peer_discovery_consul \
	rabbitmq_peer_discovery_etcd \
	rabbitmq_prometheus \
	rabbitmq_random_exchange \
	rabbitmq_recent_history_exchange \
	rabbitmq_sharding \
	rabbitmq_shovel \
	rabbitmq_shovel_management \
	rabbitmq_stomp \
	rabbitmq_stream \
	rabbitmq_top \
	rabbitmq_tracing \
	rabbitmq_trust_store \
	rabbitmq_web_dispatch \
	rabbitmq_web_mqtt \
	rabbitmq_web_mqtt_examples \
	rabbitmq_web_stomp \
	rabbitmq_web_stomp_examples

.PHONY: distclean-%
distclean-%:
	$(MAKE) -C deps/$* distclean || echo "Failed to distclean $*"

.PHONY: monorepo-distclean
monorepo-distclean: $(foreach dep,$(VENDORED_COMPONENTS),distclean-$(dep))
