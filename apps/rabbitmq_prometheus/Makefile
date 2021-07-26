define PROJECT_ENV
[
	{return_per_object_metrics, false}
]
endef

PROJECT := rabbitmq_prometheus
PROJECT_MOD := rabbit_prometheus_app

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

LOCAL_DEPS = rabbit rabbitmq_management_agent rabbitmq_web_dispatch
DEPS = accept prometheus
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers eunit_formatters

EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}

DEP_EARLY_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

ifneq ($(DISABLE_METRICS_COLLECTOR),)
RABBITMQ_CONFIG_FILE = $(CURDIR)/rabbitmq-disable-metrics-collector.conf
export RABBITMQ_CONFIG_FILE
endif

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk

# rabbitmq_management is added so that we build a custom version, for the Docker image
$(PROJECT).d::
	$(MAKE) -C $(APPS_DIR)/rabbitmq_management app IS_APP=1

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

JQ := /usr/local/bin/jq
$(JQ):
	@brew install jq

# Defined as explicit, individual targets so that autocompletion works
DASHBOARDS_TO_PATH := $(CURDIR)/docker/grafana/dashboards
define GENERATE_DASHBOARD
cd $(DASHBOARDS_TO_PATH) \
&& $(JQ) --slurp add $(@F) ../__inputs.json \
| $(JQ) '.templating.list[].datasource = "$${DS_PROMETHEUS}"' \
| $(JQ) '.panels[].datasource = "$${DS_PROMETHEUS}"'
endef
.PHONY: Erlang-Distribution.json
Erlang-Distribution.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: Erlang-Memory-Allocators.json
Erlang-Memory-Allocators.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: Erlang-Distributions-Compare.json
Erlang-Distributions-Compare.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: RabbitMQ-Overview.json
RabbitMQ-Overview.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: RabbitMQ-PerfTest.json
RabbitMQ-PerfTest.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: RabbitMQ-Quorum-Queues-Raft.json
RabbitMQ-Quorum-Queues-Raft.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: rabbitmq-exporter_vs_rabbitmq-prometheus.json
rabbitmq-exporter_vs_rabbitmq-prometheus.json: $(JQ)
	@$(GENERATE_DASHBOARD)
.PHONY: RabbitMQ-Stream.json
RabbitMQ-Stream.json: $(JQ)
	@$(GENERATE_DASHBOARD)

