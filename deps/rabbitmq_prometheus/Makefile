define PROJECT_ENV
[
	{tcp_config, [{port, 15692}]},
	{ssl_config, []},
	{return_per_object_metrics, false}
]
endef

PROJECT := rabbitmq_prometheus
PROJECT_DESCRIPTION = Prometheus metrics for RabbitMQ
PROJECT_MOD := rabbit_prometheus_app
DEPS = accept cowboy rabbit rabbitmq_management_agent prometheus rabbitmq_web_dispatch
BUILD_DEPS = amqp_client rabbit_common rabbitmq_management
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers eunit_formatters

EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

ifneq ($(DISABLE_METRICS_COLLECTOR),)
RABBITMQ_CONFIG_FILE = $(CURDIR)/rabbitmq-disable-metrics-collector.conf
export RABBITMQ_CONFIG_FILE
endif

include ../../rabbitmq-components.mk
include ../../erlang.mk

.PHONY: readme
readme: # Preview README & live reload on edit
	@docker run --interactive --tty --rm --name changelog_md \
	  --volume $(CURDIR):/data \
	  --volume $(HOME)/.grip:/.grip \
	  --expose 5000 --publish 5000:5000 \
	  mbentley/grip --context=. 0.0.0.0:5000

.PHONY: docker-image-bump
docker-image-bump: # Bump Docker image version across all docker-compose-* files
	@sed -i '' \
	  -e 's|$(DOCKER_IMAGE_NAME):.*|$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)|g' \
	  -e 's|pivotalrabbitmq/perf-test:.*|pivotalrabbitmq/perf-test:2.11.0-ubuntu|g' \
	  docker/docker-compose-{overview,dist-tls,qq,dist-metrics}.yml

RUN ?= up --detach && docker-compose --file $(@F) logs --follow
DOCKER_COMPOSE_FILES := $(wildcard docker/docker-compose-*.yml)
.PHONY: $(DOCKER_COMPOSE_FILES)
$(DOCKER_COMPOSE_FILES):
	@cd docker && \
	docker-compose --file $(@F) $(RUN) ; \
	true
.PHONY: down
down: RUN = down
down: $(DOCKER_COMPOSE_FILES) # Stop all containers

# Defined as explicit, individual targets so that autocompletion works
define DOCKER_COMPOSE_UP
cd docker && \
docker-compose --file docker-compose-$(@F).yml up --detach
endef
.PHONY: metrics
metrics: # Run all metrics containers: Grafana, Prometheus & friends
	@$(DOCKER_COMPOSE_UP)
.PHONY: overview
overview: # Make RabbitMQ Overview panels come alive
	@$(DOCKER_COMPOSE_UP)
.PHONY: dist-tls
dist-tls: # Make Erlang-Distribution panels come alive - HIGH LOAD
	@$(DOCKER_COMPOSE_UP)
.PHONY: qq
qq: # Make RabbitMQ-Quorum-Queues-Raft panels come alive - HIGH LOAD
	@$(DOCKER_COMPOSE_UP)

.PHONY: dist-metrics
dist-metrics: # Make inet_tcp_metrics come alive
	@$(DOCKER_COMPOSE_UP)
