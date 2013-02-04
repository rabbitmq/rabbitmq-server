# Make this RabbitMQ plugin
EXCHANGE:=rabbitmq-jms-topic-exchange
ARTEFACT:=rabbitmq_jms_topic_exchange

HG_BASE:=http://hg.rabbitmq.com

RABBIT_DEPS:=rabbitmq-server rabbitmq-erlang-client rabbitmq-codegen
UMBRELLA:=rabbitmq-public-umbrella
RABBIT_VERSION:=rabbitmq_v2_8_7

# command targets ##################################
.PHONY: all clean package dist init cleandist run-in-broker

all: dist

clean:
	rm -rf $(UMBRELLA)*
	rm -rf target*

dist: init
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) dist

package: dist
	mkdir -p target/plugins
	cp $(UMBRELLA)/$(EXCHANGE)/dist/$(ARTEFACT)* target/plugins/.

init: $(addprefix $(UMBRELLA)/,$(EXCHANGE) $(RABBIT_DEPS))

cleandist: init
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) clean

run-in-broker: dist
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) run-in-broker

# artefact targets #################################
$(UMBRELLA).co:
	hg clone $(HG_BASE)/$(UMBRELLA)
	cd $(UMBRELLA); hg up $(RABBIT_VERSION)
	touch $@

$(UMBRELLA)/$(EXCHANGE): $(UMBRELLA).co $(EXCHANGE)/src/*
	rm -rf $(UMBRELLA)/$(EXCHANGE)
	cp -R $(EXCHANGE) $(UMBRELLA)/.

$(addprefix $(UMBRELLA)/,$(RABBIT_DEPS)): $(UMBRELLA).co
	rm -rf $@
	cd $(UMBRELLA);hg clone $(HG_BASE)/$(notdir $@)
	cd $@; hg up $(RABBIT_VERSION)
