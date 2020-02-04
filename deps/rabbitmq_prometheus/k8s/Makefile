SHELL := bash# we want bash behaviour in all shell invocations
GCP_PROJECT := cf-rabbitmq-for-k8s-bunny
GCLOUD_CONFIG := rmq-for-k8s
GKE_CLUSTER := gerhard-2020-01-16
GKE_ZONE := europe-west2-b
GKE_VERSION := 1.16

LOCAL_BIN := $(CURDIR)/bin
PATH := $(LOCAL_BIN):$(PATH)
export PATH

.DEFAULT_GOAL := help
.PHONY: help
help:
	@awk -F"[:#]" '/^[^\.][a-zA-Z\._\-]+:+.+##.+$$/ { printf "\033[36m%-24s\033[0m %s\n", $$1, $$4 }' $(MAKEFILE_LIST) \
	| sort

define MAKE_TARGETS
  awk -F: '/^[^\.%\t][a-zA-Z\._\-]*:+.*$$/ { printf "%s\n", $$1 }' $(MAKEFILE_LIST)
endef
define BASH_AUTOCOMPLETE
  complete -W \"$$($(MAKE_TARGETS) | sort | uniq)\" make gmake m
endef
.PHONY: autocomplete
autocomplete:
	@echo "$(BASH_AUTOCOMPLETE)"
.PHONY: ac
ac: autocomplete

# Do not use kubectl installed by Docker for Desktop, this will typically be an older version than kubernetes-cli
KUBECTL := $(lastword /usr/local/Cellar/kubernetes-cli/$(GKE_VERSION).0/bin/kubectl $(wildcard /usr/local/Cellar/kubernetes-cli/*/bin/kubectl))
$(KUBECTL):
	brew install kubernetes-cli
bin/kubectl: $(KUBECTL) ## Setup kubectl CLI
	mkdir -p $(LOCAL_BIN) \
	&& ln -sf $(KUBECTL) $(LOCAL_BIN)/kubectl

HELM := /usr/local/opt/helm@2/bin/helm
$(HELM):
	brew install helm@2
bin/helm: $(HELM) ## Setup helm CLI
	mkdir -p $(LOCAL_BIN) \
	&& ln -sf $(HELM) $(LOCAL_BIN)/helm

LPASS := /usr/local/bin/lpass
$(LPASS):
	brew install lastpass-cli

.PHONY: gcloud-login
gcloud-login: ## Login gcloud CLI
	gcloud config configurations activate $(GCLOUD_CONFIG) \
	|| gcloud config configurations create $(GCLOUD_CONFIG)
	gcloud auth login
	gcloud config set project $(GCP_PROJECT)

.PHONY: gke-ls
gke-ls: ## List GKE clusters
	gcloud container clusters list

.PHONY: gke-connect
gke-connect: ## Connect kubectl to GKE cluster
	gcloud container clusters get-credentials $(GKE_CLUSTER) --zone $(GKE_ZONE)

.PHONY: gke-admin
gke-admin: $(KUBECTL)
	$(KUBECTL) apply --filename config/cluster-admin.yaml

.PHONY: haproxy
haproxy: config/haproxy/dhparams.pem config/haproxy/ca.pem config/haproxy/bundle.pem $(HELM) $(KUBECTL)
	cd config/haproxy \
	&& $(HELM) template . --name haproxy > _manifest.yaml \
	&& $(KUBECTL) apply --filename _manifest.yaml

.PHONY: haproxy-delete
haproxy-delete: $(KUBECTL)
	$(KUBECTL) delete -f config/haproxy/_manifest.yaml

config/haproxy/dhparams.pem:
	openssl dhparam -out $(@) 2048

# https://support.cloudflare.com/hc/en-us/articles/115000479507
config/haproxy/ca.pem: $(LPASS)
	$(LPASS) show --notes "Shared-RabbitMQ/cloudflare-origin-ca-rsa-root.pem" > $(@)

config/haproxy/bundle.pem: $(LPASS)
	$(LPASS) show --notes "Shared-RabbitMQ/cloudflare-edge-bundle.pem" > $(@)

.PHONY: cluster-admin-binding
cluster-admin-binding: $(KUBECTL)
	$(KUBECTL) get --filename config/cluster-admin.yaml \
	|| $(KUBECTL) apply --filename config/cluster-admin.yaml

.PHONY: rabbitmq
rabbitmq: cluster-admin-binding config/agentmq/.erlang.cookie $(HELM) $(KUBECTL)
	cd config/agentmq \
	&& $(HELM) template . --name agentmq > _manifest.yaml \
	&& $(KUBECTL) apply --filename _manifest.yaml

.PHONY: rabbitmq-delete
rabbitmq-delete: $(KUBECTL)
	$(KUBECTL) delete -f config/agentmq/_manifest.yaml

.PHONY: rabbitmq-proxy
rabbitmq-proxy: $(KUBECTL)
	$(KUBECTL) port-forward service/agentmq 15672:15672 15692:15692

config/agentmq/.erlang.cookie:
	echo -n "$$(openssl rand -hex 10)" > $(@)

clean: haproxy-delete rabbitmq-delete

LIMIT ?= 20
events: $(KUBECTL)
	$(KUBECTL) get events --sort-by='.metadata.creationTimestamp' | tail -n $(LIMIT)

resources: $(KUBECTL)
	$(KUBECTL) get all -o wide
	$(KUBECTL) get secret
	$(KUBECTL) get configmap
