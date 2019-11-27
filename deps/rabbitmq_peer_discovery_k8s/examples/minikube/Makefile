PLATFORM := $(shell uname)

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
.PHONY: install-minikube
install-minikube: $(MINIKUBE)
	@$(MINIKUBE) start --vm-driver=virtualbox --disk-size "10 GB"

.PHONY: run-in-minikube
# run-in-minikube: minikube $(KUBECTL)
run-in-minikube: $(KUBECTL)
	@($(KUBECTL) get namespace test-rabbitmq || $(KUBECTL) create namespace test-rabbitmq) \
	&& $(KUBECTL) apply -f .

endif
