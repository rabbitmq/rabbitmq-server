YTT ?= /usr/local/bin/ytt

$(YTT):
	$(error Please install ytt from https://get-ytt.io/)

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

DEPS_YAML_FILE = workflow_sources/deps.yml

define dep_yaml_chunk
$(eval SUITES := $(sort $(subst _SUITE.erl,,$(notdir $(wildcard deps/$(1)/test/*_SUITE.erl)))))
echo "\n- name: $(1)\n  suites:$(if $(SUITES),$(foreach suite,$(SUITES),\n  - name: $(suite)), [])" >> $(DEPS_YAML_FILE);
endef

$(DEPS_YAML_FILE):
	@echo "#@data/values\n---\n#@overlay/match missing_ok=True\ndeps:" > $@
	@$(foreach dep,$(VENDORED_COMPONENTS),$(call dep_yaml_chunk,$(dep)))
	@cat $@ | git stripspace > $@.fixed && mv $@.fixed $@

.github/workflows/base-images.yaml: $(YTT) $(wildcard workflow_sources/base_image/*)
	ytt -f workflow_sources/base_image \
	-f workflow_sources/base_values.yml \
	--output-files /tmp
	cat /tmp/workflow.yml | sed s/a_magic_string_that_we_will_sed_to_on/on/ \
	> $@

.github/workflows/test-erlang-otp-%.yaml: \
  $(YTT) $(DEPS_YAML_FILE) workflow_sources/test-erlang-otp-%.yml $(wildcard workflow_sources/test/*)
	ytt -f workflow_sources/test \
	-f workflow_sources/base_values.yml \
	-f $(DEPS_YAML_FILE) \
	-f workflow_sources/test-erlang-otp-$*.yml \
	--output-files /tmp
	cat /tmp/test-erlang-otp-$*.yml | sed s/a_magic_string_that_we_will_sed_to_on/on/ \
	> $@

monorepo-actions: \
  .github/workflows/base-images.yaml \
  $(patsubst workflow_sources/%.yml,.github/workflows/%.yaml,$(wildcard workflow_sources/test-erlang-otp-*.yml))

DOCKER_REPO ?= eu.gcr.io/cf-rabbitmq-core

.PHONY: erlang-elixir-image-%
erlang-elixir-image-%:
	docker build . \
		-f ci/dockerfiles/$*/erlang_elixir \
		-t $(DOCKER_REPO)/erlang_elixir:$*

.PHONY: ci-base-image-%
ci-base-image-%:
	docker build . \
		-f ci/dockerfiles/ci-base \
		-t $(DOCKER_REPO)/ci-base:$* \
		--build-arg ERLANG_VERSION=$*

.PHONY: ci-base-images
ci-base-images: ci-base-image-23.1

PUSHES = $(foreach v,$(ERLANG_VERSIONS),push-base-image-$(v))
.PHONY: $(PUSHES)
$(PUSHES):
	docker push $(DOCKER_REPO)/ci-base:$(subst push-base-image-,,$@)

.PHONY: push-base-images
push-base-images: $(PUSHES)

LOCAL_CI_GOALS = $(foreach dep,$(filter-out rabbitmq_cli,$(VENDORED_COMPONENTS)),ci-$(dep))
ERLANG_VERSION ?= 23.1
SKIP_DIALYZE ?= False

TAG = erlang-$(ERLANG_VERSION)-rabbitmq-$(shell git rev-parse HEAD)$(shell git diff-index --quiet HEAD -- || echo -dirty)
LOCAL_IMAGE = $(DOCKER_REPO)/ci:$(TAG)

.PHONY: local-ci-image
local-ci-image:
	docker build . \
		-f ci/dockerfiles/ci \
		-t $(LOCAL_IMAGE) \
		--build-arg ERLANG_VERSION=$(ERLANG_VERSION) \
		--build-arg GITHUB_RUN_ID=none \
		--build-arg BUILDEVENT_APIKEY=$(BUILDEVENT_APIKEY) \
		--build-arg GITHUB_SHA=$$(git rev-parse HEAD) \
		--build-arg base_rmq_ref=master \
		--build-arg current_rmq_ref=$$(git rev-parse --abbrev-ref HEAD) \
		--build-arg RABBITMQ_VERSION=3.9.0

.PHONY: $(LOCAL_CI_GOALS)
$(LOCAL_CI_GOALS): local-ci-image
	docker run --rm \
		--env project=$(subst ci-,,$@) \
		--env SKIP_DIALYZE=$(SKIP_DIALYZE) \
		--env GITHUB_RUN_ID=none \
		--env BUILDEVENT_APIKEY=$(BUILDEVENT_APIKEY) \
		--env STEP_START=$$(date +%s) \
		--volume /tmp/ct-logs:/ct-logs \
		--oom-score-adj -500 \
		$(LOCAL_IMAGE) \
		/workspace/rabbitmq/ci/scripts/tests.sh

ci-rabbitmq_cli: local-ci-image
	docker run --rm \
		--env project=$(subst ci-,,$@) \
		--env SKIP_DIALYZE=$(SKIP_DIALYZE) \
		--env GITHUB_RUN_ID=none \
		--env BUILDEVENT_APIKEY=$(BUILDEVENT_APIKEY) \
		--env STEP_START=$$(date +%s) \
		--volume /tmp/broker-logs:/broker-logs \
		$(LOCAL_IMAGE) \
		/workspace/rabbitmq/ci/scripts/rabbitmq_cli.sh

.PHONY: docker
docker: local-ci-image
	docker run --rm -it \
		--oom-score-adj -500 \
		$(LOCAL_IMAGE) \
		/bin/bash

.PHONY: distclean-%
distclean-%:
	$(MAKE) -C deps/$* distclean || echo "Failed to distclean $*"

.PHONY: monorepo-distclean
monorepo-distclean: $(foreach dep,$(VENDORED_COMPONENTS),distclean-$(dep))
