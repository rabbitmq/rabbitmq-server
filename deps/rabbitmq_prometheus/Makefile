PROJECT = rabbitmq_prometheus
PROJECT_MOD = rabbit_prometheus_app

TEST_DEPS = eunit_formatters
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}

DEPS = rabbit_common rabbit rabbitmq_management_agent prometheus accept rabbitmq_web_dispatch
BUILD_DEPS = amqp_client

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

DOCKER_IMAGE_VERSION := 3.8-2019.04.15

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
docker_image_build: tests
	@docker build --pull \
	  --build-arg PGP_KEYSERVER=pgpkeys.eu \
	  --build-arg RABBITMQ_PROMETHEUS_VERSION=$(PROJECT_VERSION) \
	  --tag pivotalrabbitmq/rabbitmq-prometheus:$(DOCKER_IMAGE_VERSION) \
	  --tag pivotalrabbitmq/rabbitmq-prometheus:latest docker
.PHONY: dib
dib: docker_image_build

.PHONY: docker_image_push
docker_image_push:
	@docker push pivotalrabbitmq/rabbitmq-prometheus:$(DOCKER_IMAGE_VERSION) && \
	docker push pivotalrabbitmq/rabbitmq-prometheus:latest
.PHONY: dib
dib: docker_image_build

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
