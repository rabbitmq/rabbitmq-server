SHELL := bash# we want bash behaviour in all shell invocations
PLATFORM := $(shell uname)

# https://stackoverflow.com/questions/4842424/list-of-ansi-color-escape-sequences
RED := \033[1;31m
GREEN := \033[1;32m
YELLOW := \033[1;33m
WHITE := \033[1;37m
BOLD := \033[1m
NORMAL := \033[0m

OK := $(GREEN)OK$(NORMAL)\n

K8S_NAMESPACE := test-rabbitmq

### Tested on OS X 10.14.6 & 10.15.1
ifeq ($(PLATFORM),Darwin)

### DEPS ###
#
VIRTUALBOX := /usr/local/bin/VBoxManage
$(VIRTUALBOX):
	@brew cask install virtualbox \
	|| ( echo "Remember to read & follow the Caveats if installation fails" ; exit 1 )

MINIKUBE := /usr/local/bin/minikube
$(MINIKUBE): $(VIRTUALBOX)
	@brew install minikube

KUBECTL := /usr/local/bin/kubectl
$(KUBECTL):
	@brew install kubectl

### TARGETS ###
#
.DEFAULT_GOAL := wait-for-rabbitmq

.PHONY: start-minikube
start-minikube: $(MINIKUBE)
	@( $(MINIKUBE) status | grep Running ) \
	|| $(MINIKUBE) start --vm-driver=virtualbox --disk-size "10 GB"

.PHONY: run-in-minikube
run-in-minikube: start-minikube $(KUBECTL)
	@( $(KUBECTL) get namespace $(K8S_NAMESPACE) \
	   || $(KUBECTL) create namespace $(K8S_NAMESPACE) ) \
	&& $(KUBECTL) apply -f .

CHECK_EVERY := 5

define RABBITMQ_STATEFULSET_READY_REPLICAS
$(KUBECTL) --namespace=$(K8S_NAMESPACE) get statefulset.apps/rabbitmq --output=jsonpath='{.status.readyReplicas}'
endef

define RABBITMQ_STATEFULSET_REPLICAS
$(KUBECTL) --namespace=$(K8S_NAMESPACE) get statefulset.apps/rabbitmq --output=jsonpath='{.status.replicas}'
endef

define RABBITMQ_STATEFULSET_READY
[ $$($(RABBITMQ_STATEFULSET_REPLICAS)) = $$($(RABBITMQ_STATEFULSET_READY_REPLICAS)) ]
endef

.PHONY: wait-for-rabbitmq
wait-for-rabbitmq: run-in-minikube
	@printf "$(YELLOW)Waiting for RabbitMQ StatefulSet to be ready..." \
	; while ! $(RABBITMQ_STATEFULSET_READY); do printf "."; sleep $(CHECK_EVERY); done \
	&& printf "$(OK)\n" \
	&& printf "$(YELLOW)Checking RabbitMQ cluster status using $(NORMAL)$(BOLD)rabbitmq-diagnostics cluster_status$(NORMAL) ...\n\n" \
	&& $(KUBECTL) exec --namespace=$(K8S_NAMESPACE) rabbitmq-0 rabbitmq-diagnostics cluster_status \
	&& printf "\n$(YELLOW)For connection information see README.md$(NOMAL)\n\n"

endif
