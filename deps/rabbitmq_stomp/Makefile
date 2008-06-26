RABBIT_SOURCE_ROOT=../rabbitmq
RABBIT_SERVER_SOURCE_ROOT=$(RABBIT_SOURCE_ROOT)/rabbitmq-server
RABBIT_SERVER_INCLUDE_DIR=$(RABBIT_SERVER_SOURCE_ROOT)/include

SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(SOURCES))
ERLC_OPTS=-I $(RABBIT_SERVER_INCLUDE_DIR) -I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall +debug_info # +native -v

all: $(EBIN_DIR) $(TARGETS)

$(EBIN_DIR):
	mkdir -p $@

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES)
	erlc $(ERLC_OPTS) $<

clean:
	rm -f ebin/*.beam $(TARGETS)

run: all start_server

start_server:
	$(MAKE) -C $(RABBIT_SERVER_SOURCE_ROOT) run \
		RABBIT_ARGS='-pa '"$$(pwd)/$(EBIN_DIR)"' -rabbit \
			stomp_listeners [{\"0.0.0.0\",61613}] \
			extra_startup_steps [{\"STOMP-listeners\",rabbit_stomp,kickstart,[]}]'
