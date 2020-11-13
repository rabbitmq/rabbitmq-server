TODAY := $(shell date -u +'%Y.%m.%d')
# Use the latest alpha RabbitMQ 3.7 release - https://dl.bintray.com/rabbitmq/all-dev/rabbitmq-server/
BASED_ON_RABBITMQ_VERSION := 3.7.22-alpha.5
DOCKER_IMAGE_NAME := pivotalrabbitmq/rabbitmq-prometheus
DOCKER_IMAGE_VERSION := $(BASED_ON_RABBITMQ_VERSION)-$(TODAY)
# RABBITMQ_VERSION is used in rabbitmq-components.mk to set PROJECT_VERSION
RABBITMQ_VERSION ?= $(DOCKER_IMAGE_VERSION)

PROJECT := rabbitmq_prometheus
PROJECT_MOD := rabbit_prometheus_app
DEPS = rabbit rabbitmq_management_agent prometheus rabbitmq_web_dispatch
# Deps that are not applications
# rabbitmq_management is added so that we build a custom version, for the Docker image
BUILD_DEPS = accept amqp_client rabbit_common rabbitmq_management
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers eunit_formatters

EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

ifneq ($(DISABLE_METRICS_COLLECTOR),)
BUILD_DEPS = accept amqp_client rabbit_common
RABBITMQ_CONFIG_FILE = $(CURDIR)/rabbitmq-disable-metrics-collector.conf
export RABBITMQ_CONFIG_FILE
endif

include rabbitmq-components.mk
include erlang.mk

define MAKE_TARGETS
  awk -F: '/^[^\.%\t][a-zA-Z\._\-]*:+.*$$/ { printf "%s\n", $$1 }' $(MAKEFILE_LIST)
endef
define BASH_AUTOCOMPLETE
  complete -W \"$$($(MAKE_TARGETS) | sort | uniq)\" make gmake m
endef
.PHONY: autocomplete
autocomplete: ## ac  | Configure shell for autocompletion - eval "$(gmake autocomplete)"
	@echo "$(BASH_AUTOCOMPLETE)"
.PHONY: ac
ac: autocomplete
# Continuous Feedback for the ac target - run in a separate pane while iterating on it
.PHONY: CFac
CFac:
	@watch -c $(MAKE) ac

.PHONY: clean-docker
clean-docker: ## cd  | Clean all Docker containers & volumes
	@docker system prune -f && \
	docker volume prune -f
.PHONY: cd
cd: clean-docker

define CTOP_CONTAINER
docker pull quay.io/vektorlab/ctop:latest && \
docker run --rm --interactive --tty \
  --cpus 0.5 --memory 128M \
  --volume /var/run/docker.sock:/var/run/docker.sock \
  --name ctop_$(USER) \
  quay.io/vektorlab/ctop:latest
endef
.PHONY: cto
cto: ## cto | Interact with all containers via a top-like utility
	@$(CTOP_CONTAINER)

.PHONY: dockerhub-login
dockerhub-login: ## dl  | Login to Docker Hub as pivotalrabbitmq
	@echo "$$(lpass show --password 7672183166535202820)" | \
	docker login --username pivotalrabbitmq --password-stdin
.PHONY: dl
dl: dockerhub-login

.PHONY: docker-image
docker-image: docker-image-build docker-image-push ## di  | Build & push Docker image to Docker Hub
.PHONY: di
di: docker-image

.PHONY: docker-image-build
docker-image-build: ## dib | Build Docker image locally - make tests
	@docker build --pull \
	  --build-arg PGP_KEYSERVER=pgpkeys.eu \
	  --build-arg RABBITMQ_VERSION=$(BASED_ON_RABBITMQ_VERSION) \
	  --build-arg RABBITMQ_PROMETHEUS_VERSION=$(RABBITMQ_VERSION) \
	  --tag $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) \
	  --tag $(DOCKER_IMAGE_NAME):latest .
.PHONY: dib
dib: docker-image-build

.PHONY: docker-image-bump
docker-image-bump: ## diu | Bump Docker image version across all docker-compose-* files
	@sed -i '' \
	  -e 's|$(DOCKER_IMAGE_NAME):.*|$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)|g' \
	  -e 's|pivotalrabbitmq/perf-test:.*|pivotalrabbitmq/perf-test:2.9.1-ubuntu|g' \
	  docker/docker-compose-{overview,dist-tls,qq}.yml
.PHONY: diu
diu: docker-image-bump

.PHONY: docker-image-push
docker-image-push: ## dip | Push local Docker image to Docker Hub
	@docker push $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION) && \
	docker push $(DOCKER_IMAGE_NAME):latest
.PHONY: dip
dip: docker-image-push

.PHONY: docker-image-run
docker-image-run: ## dir | Run container with local Docker image
	@docker run --interactive --tty \
	  --publish=5672:5672 \
	  --publish=15672:15672 \
	  --publish=15692:15692 \
	  $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)
.PHONY: dir
dir: docker-image-run

DOCKER_COMPOSE_ACTION ?= up --detach && docker-compose --file $(@F) logs --follow
DOCKER_COMPOSE_FILES := $(wildcard docker/docker-compose-*.yml)
.PHONY: $(DOCKER_COMPOSE_FILES)
$(DOCKER_COMPOSE_FILES):
	@cd docker && \
	docker-compose --file $(@F) $(DOCKER_COMPOSE_ACTION) ; \
	true
.PHONY: down
down: DOCKER_COMPOSE_ACTION = down
down: $(DOCKER_COMPOSE_FILES) ## d   | Stop all containers
.PHONY: d
d: down

JQ := /usr/local/bin/jq
$(JQ):
	@brew install jq

OTP_CURRENT_STABLE_MAJOR := 22
define LATEST_STABLE_OTP_VERSION
curl --silent --fail https://api.github.com/repos/erlang/otp/git/refs/tags | \
  $(JQ) -r '.[].ref | sub("refs/tags/OTP.{1}";"") | match("^$(OTP_CURRENT_STABLE_MAJOR)[0-9.]+$$") | .string' | \
  tail -n 1
endef
.PHONY: find-latest-otp
find-latest-otp: $(JQ) ## flo | Find latest OTP version archive + sha1
	@printf "Version: " && \
	export VERSION="$$($(LATEST_STABLE_OTP_VERSION))" && \
	echo "$$VERSION" && \
	printf "Checksum: " && \
	wget --continue --quiet --output-document="/tmp/OTP-$$VERSION.tar.gz" "https://github.com/erlang/otp/archive/OTP-$$VERSION.tar.gz" && \
	shasum -a 256 "/tmp/OTP-$$VERSION.tar.gz"
.PHONY: flo
flo: find-latest-otp

# Defined as explicit, individual targets so that autocompletion works
define DOCKER_COMPOSE_UP
cd docker && \
docker-compose --file docker-compose-$(@F).yml up --detach
endef
.PHONY: metrics
metrics: ## m   | Run all metrics containers: Grafana, Prometheus & friends
	@$(DOCKER_COMPOSE_UP)
.PHONY: m
m: metrics
.PHONY: overview
overview: ## o   | Make RabbitMQ Overview panels come alive
	@$(DOCKER_COMPOSE_UP)
.PHONY: o
o: overview
.PHONY: dist-tls
dist-tls: ## dt  | Make Erlang-Distribution panels come alive - HIGH LOAD
	@$(DOCKER_COMPOSE_UP)
.PHONY: dt
dt: dist-tls
.PHONY: qq
qq: ##     | Make RabbitMQ-Quorum-Queues-Raft panels come alive - HIGH LOAD
	@$(DOCKER_COMPOSE_UP)

SEPARATOR := -------------------------------------------------------------------------------------------------
.PHONY: h
h:
	@awk -F"[:#]" '/^[^\.][a-zA-Z\._\-]+:+.+##.+$$/ { printf "$(SEPARATOR)\n\033[36m%-24s\033[0m %s\n", $$1, $$4 }' $(MAKEFILE_LIST) ; \
	echo $(SEPARATOR)
# Continuous Feedback for the h target - run in a separate pane while iterating on it
.PHONY: CFh
CFh:
	@watch -c $(MAKE) h

# Defined as explicit, individual targets so that autocompletion works
DASHBOARDS_TO_PATH := $(CURDIR)/docker/grafana/dashboards
define GENERATE_DASHBOARD
cd $(DASHBOARDS_TO_PATH) \
&& jq --slurp add $(@F) __inputs.json \
| jq '.templating.list[].datasource = "$${DS_PROMETHEUS}"' \
| jq '.panels[].datasource = "$${DS_PROMETHEUS}"'
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

