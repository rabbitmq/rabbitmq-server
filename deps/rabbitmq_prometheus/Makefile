TODAY := $(shell date -u +'%Y.%m.%d')
# Use the latest alpha RabbitMQ 3.8 release - https://dl.bintray.com/rabbitmq/all-dev/rabbitmq-server/
# BASED_ON_RABBITMQ_VERSION := 3.8.0-alpha.622
BASED_ON_RABBITMQ_VERSION := 3.8-rabbitmq-server-1988
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

.PHONY: docker_login
docker_login:
	@echo "$$(lpass show --password 7672183166535202820)" | \
	docker login --username pivotalrabbitmq --password-stdin
.PHONY: dl
dl: docker_login

.PHONY: docker_image
docker_image: docker_image_build docker_image_push
.PHONY: di
di: docker_image

.PHONY: docker_image_build
docker_image_build:
	@docker build --pull \
	  --build-arg PGP_KEYSERVER=pgpkeys.eu \
	  --build-arg RABBITMQ_VERSION=$(BASED_ON_RABBITMQ_VERSION) \
	  --build-arg RABBITMQ_PROMETHEUS_VERSION=$(PROJECT_VERSION) \
	  --tag pivotalrabbitmq/rabbitmq-prometheus:$(DOCKER_IMAGE_VERSION) \
	  --tag pivotalrabbitmq/rabbitmq-prometheus:latest .
.PHONY: dib
dib: docker_image_build

.PHONY: docker_image_push
docker_image_push:
	@docker push pivotalrabbitmq/rabbitmq-prometheus:$(DOCKER_IMAGE_VERSION) && \
	docker push pivotalrabbitmq/rabbitmq-prometheus:latest
.PHONY: dip
dip: docker_image_push

define RUN_DOCKER_IMAGE
endef

.PHONY: docker_image_run
docker_image_run:
	@docker run --interactive --tty \
	  --publish=5672:5672 \
	  --publish=15672:15672 \
	  --publish=15692:15692 \
	  pivotalrabbitmq/rabbitmq-prometheus:$(DOCKER_IMAGE_VERSION)
.PHONY: dir
dir: docker_image_run

define CTOP_CONTAINER
docker pull quay.io/vektorlab/ctop:latest && \
docker run --rm --interactive --tty \
  --cpus 0.5 --memory 128M \
  --volume /var/run/docker.sock:/var/run/docker.sock \
  --name ctop_$(USER) \
  quay.io/vektorlab/ctop:latest
endef
.PHONY: ctop
ctop:
	@$(CTOP_CONTAINER)
