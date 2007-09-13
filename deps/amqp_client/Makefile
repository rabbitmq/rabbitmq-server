NODENAME=rabbit
RABBIT_ARGS=
SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(EBIN_DIR)/rabbit_framing.beam $(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(SOURCES))
#other args: +native +"{hipe,[o3,verbose]}" -Ddebug=true +debug_info
ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info

MNESIA_DIR=/tmp/rabbitmq-$(NODENAME)-mnesia
LOG_FILE=/tmp/rabbitmq-$(NODENAME).log
SASL_LOG_FILE=/tmp/rabbitmq-$(NODENAME)-sasl.log
DIST_DIR=dist
SBIN_DIR=$(DIST_DIR)/sbin
#AMQP_SPEC_XML_PATH=../../docs/specs/amqp-8.1.xml
AMQP_SPEC_XML_PATH=../../docs/specs/amqp0-8.xml

# other args: -smp auto +S2
ERL_CMD=erl \
	-boot start_sasl \
	-sname $(NODENAME) \
	+W w \
	-pa $(EBIN_DIR) \
	+K true \
	+A30 \
	-kernel inet_default_listen_options '[{sndbuf, 16384}, {recbuf, 4096}]' \
	-sasl errlog_type error \
	-os_mon start_cpu_sup true \
	-os_mon start_disksup false \
	-os_mon start_memsup false \
	-os_mon start_os_sup false \
	-mnesia dir '"$(MNESIA_DIR)"' \
	$(RABBIT_ARGS)

ERL_CALL=erl_call -sname $(NODENAME) -e

# for the moment we don't use boot files because they introduce a
# dependency on particular versions of OTP applications
#all: $(EBIN_DIR)/rabbit.boot
all: $(TARGETS)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDE_DIR)/rabbit_framing.hrl $(INCLUDE_DIR)/rabbit.hrl
	erlc $(ERLC_OPTS) $<
#	ERLC_EMULATOR="erl -smp" erlc $(ERLC_OPTS) $<

$(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing.erl: codegen.py $(AMQP_SPEC_XML_PATH)
	python -c 'import codegen; codegen.generateHrl("'$(AMQP_SPEC_XML_PATH)'")' > $(INCLUDE_DIR)/rabbit_framing.hrl
	python -c 'import codegen; codegen.generateErl("'$(AMQP_SPEC_XML_PATH)'")' > $(SOURCE_DIR)/rabbit_framing.erl

$(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script: $(EBIN_DIR)/rabbit.app $(EBIN_DIR)/rabbit.rel $(TARGETS)
	erl -noshell -eval 'systools:make_script("ebin/rabbit", [{path, ["ebin"]}]), halt().'

clean: cleandb
	rm -f $(TARGETS)
	rm -f $(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script
	rm -f $(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing.erl codegen.pyc

cleandb: stop-node
	erl -mnesia dir '"$(MNESIA_DIR)"' -noshell -eval 'lists:foreach(fun file:delete/1, filelib:wildcard(mnesia:system_info(directory) ++ "/*")), halt().'

############ various tasks to interact with RabbitMQ ###################

run: all
#	$(ERL_CMD) -boot $(EBIN_DIR)/rabbit
	$(ERL_CMD) -s rabbit

run-silent: all
	$(ERL_CMD) -kernel error_logger '{file,"$(LOG_FILE)"}' -sasl sasl_error_logger '{file,"$(SASL_LOG_FILE)"}' -s rabbit

run-node: all
	$(ERL_CMD)

run-tests: all
	echo "rabbit_tests:all_tests()." | $(ERL_CALL)

start-background-node: stop-node
	$(ERL_CMD) -detached -kernel error_logger '{file,"$(LOG_FILE)"}' -sasl sasl_error_logger '{file,"$(SASL_LOG_FILE)"}'; sleep 1

start-rabbit-on-node: all
	echo "rabbit:start()." | $(ERL_CALL)

stop-rabbit-on-node: all
	echo "rabbit:stop()." | $(ERL_CALL)

stop-node:
	-$(ERL_CALL) -q

start-cover: all
	echo "cover:start(), rabbit_misc:enable_cover()." | $(ERL_CALL)

stop-cover: all
	echo "rabbit_misc:report_cover(), cover:stop()." | $(ERL_CALL)
	cat cover/summary.txt

########################################################################

dist: all
	mkdir -p $(DIST_DIR)
	cp -r ebin include src scripts $(DIST_DIR)
	cp ../../LICENSE ../../LICENSE-MPL-RabbitMQ $(DIST_DIR)
	cp ../../INSTALL $(DIST_DIR)
	rm -rf `find $(DIST_DIR) -name CVS`
	rm -f `find $(DIST_DIR) -name .cvsignore`
	chmod 0755 $(DIST_DIR)/scripts/*

prepare-sbin-dir:
	-mkdir -p $(SBIN_DIR)

pruned-dist: dist
	rm -rf $(DIST_DIR)/scripts

dist-unix: pruned-dist prepare-sbin-dir
	cp scripts/rabbitmq-server scripts/rabbitmqctl $(SBIN_DIR)
	chmod 0755 $(SBIN_DIR)/*

dist-windows: pruned-dist prepare-sbin-dir
	cp scripts/rabbitmq-server.bat scripts/rabbitmqctl.bat $(SBIN_DIR)
	chmod 0755 $(SBIN_DIR)/*

distclean: clean
	rm -rf $(DIST_DIR)
	find . -name '*~' -exec rm {} \;
