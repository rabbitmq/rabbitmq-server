PACKAGE=rabbitmq-stomp
DEPS=rabbitmq-erlang-client

include ../include.mk

EBIN_DIR=ebin

RABBIT_SOURCE_ROOT=..
RABBIT_SERVER_SOURCE_ROOT=$(RABBIT_SOURCE_ROOT)/rabbitmq-server


run: start_stomp_server

start_stomp_server:
	$(MAKE) -C $(RABBIT_SERVER_SOURCE_ROOT) run \
		RABBITMQ_SERVER_START_ARGS='-pa '"$$(pwd)/$(EBIN_DIR)"' \
			-rabbit stomp_listeners [{\"0.0.0.0\",61613}]'

start-cover: all
	$(MAKE) -C $(RABBIT_SERVER_SOURCE_ROOT) start-cover \
		COVER_DIR=../rabbitmq-stomp/

stop-cover:
	$(MAKE) -C $(RABBIT_SERVER_SOURCE_ROOT) stop-cover

test:
	$(MAKE) start-cover
	python test/test.py
	$(MAKE) stop-cover


