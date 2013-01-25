# Make this RabbitMQ plugin
EXCHANGE:=rabbitmq-jms-topic-exchange

HG_BASE:=http://hg.rabbitmq.com

RABBIT_DEPS:=rabbitmq-server rabbitmq-erlang-client rabbitmq-codegen
UMBRELLA:=rabbitmq-public-umbrella
RABBIT_VERSION:=rabbitmq_v2_8_7

# command targets ##################################
.PHONY: all clean dist init cleandist run-in-broker

all: dist

clean:
	rm -rf $(UMBRELLA)
	rm $(UMBRELLA).co

dist: init
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) dist

init: $(addprefix $(UMBRELLA)/,$(EXCHANGE) $(RABBIT_DEPS))

cleandist: init
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) clean

run-in-broker: dist
	$(MAKE) -C $(UMBRELLA)/$(EXCHANGE) run-in-broker

# artefact targets #################################
$(UMBRELLA).co:
	hg clone $(HG_BASE)/$(UMBRELLA)
	cd $(UMBRELLA); hg up $(RABBIT_VERSION)

$(UMBRELLA)/$(EXCHANGE): $(UMBRELLA).co $(EXCHANGE)/src/*
	rm -rf $(UMBRELLA)/$(EXCHANGE)
	cp -R $(EXCHANGE) $(UMBRELLA)/.

$(addprefix $(UMBRELLA)/,$(RABBIT_DEPS)): $(UMBRELLA).co
	rm -rf $@
	cd $(UMBRELLA);hg clone $(HG_BASE)/$(notdir $@)
	cd $@; hg up $(RABBIT_VERSION)
