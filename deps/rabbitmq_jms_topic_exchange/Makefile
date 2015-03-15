# Make this RabbitMQ plugin
EXCHANGE:=rabbitmq-jms-topic-exchange
ARTEFACT:=rabbitmq_jms_topic_exchange

# Normally overridden on commandline (to include rjms version suffix)
MAVEN_ARTEFACT:=$(ARTEFACT).ez

# Version of plugin artefact to build: must be supplied on commandline
# RJMS_VERSION:=
# Version of RabbitMQ to build against: must be supplied on commandline
# RMQ_VERSION:=

GIT_BASE:=https://github.com/rabbitmq

RABBIT_DEPS:=rabbitmq-server rabbitmq-erlang-client rabbitmq-codegen
UMBRELLA:=rabbitmq-public-umbrella
RMQ_VERSION_TAG:=rabbitmq_v$(subst .,_,$(RMQ_VERSION))

# command targets ##################################
.PHONY: all clean package dist init cleandist test run-in-broker

all: dist

clean:
	rm -rf $(UMBRELLA)*
	rm -rf target*

dist: init
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) VERSION=$(RMQ_VERSION) dist

package: dist
	mkdir -p target/plugins
	cp $(UMBRELLA)/$(EXCHANGE)/dist/$(ARTEFACT)* target/plugins/$(MAVEN_ARTEFACT)

init: $(addprefix $(UMBRELLA)/,$(EXCHANGE) $(RABBIT_DEPS))

test: dist
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) VERSION=$(RMQ_VERSION) test

cleandist: init
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) VERSION=$(RMQ_VERSION) clean

run-in-broker: dist
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) VERSION=$(RMQ_VERSION) run-in-broker

# artefact targets #################################
$(UMBRELLA).co:
	git clone $(GIT_BASE)/$(UMBRELLA)
	cd $(UMBRELLA); git checkout $(RMQ_VERSION_TAG)
	touch $@

$(UMBRELLA)/$(EXCHANGE): $(UMBRELLA).co $(EXCHANGE)/src/* $(EXCHANGE)/test/src/* $(EXCHANGE)/include/*
	rm -rf $(UMBRELLA)/$(EXCHANGE)
	cp -R $(EXCHANGE) $(UMBRELLA)/.
	for srcfile in $$(grep @RJMS_VERSION@ $(EXCHANGE) -r -l); do \
		sed -e 's|@RJMS_VERSION@|$(RJMS_VERSION)|' <$${srcfile} >$(UMBRELLA)/$${srcfile}; \
	done

$(addprefix $(UMBRELLA)/,$(RABBIT_DEPS)): $(UMBRELLA).co
	rm -rf $@
	cd $(UMBRELLA);git clone $(GIT_BASE)/$(notdir $@)
	cd $@; git checkout $(RMQ_VERSION_TAG)
