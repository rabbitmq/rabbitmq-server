#   The contents of this file are subject to the Mozilla Public License
#   Version 1.1 (the "License"); you may not use this file except in
#   compliance with the License. You may obtain a copy of the License at
#   http://www.mozilla.org/MPL/
#
#   Software distributed under the License is distributed on an "AS IS"
#   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
#   License for the specific language governing rights and limitations
#   under the License.
#
#   The Original Code is the RabbitMQ Erlang Client.
#
#   The Initial Developers of the Original Code are LShift Ltd.,
#   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
#
#   Portions created by LShift Ltd., Cohesive Financial
#   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
#   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
#   Technologies Ltd.; 
#
#   All Rights Reserved.
#
#   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
#

# The client library can either be built from source control or by downloading
# a source tarball from the RabbitMQ site. The intention behind the source tarball is
# to be able to unpack this anywhere and just run a simple a test, under the
# assumption that you have a running broker. This provides the simplest
# possible way of building and running the client.
#
# The source control version, on the other hand, contains far more infrastructure
# to start and stop brokers, package modules from the server, run embedded tests
# and so forth.
#
# This means that the Makefile of the source control version contains a lot of
# functionality that just wouldn't work with the source tarball version.
#
# The purpose of this common Makefile is to define as many commonalities
# between the build requirements of the source control version and the source
# tarball version. This avoids duplicating make definitions and rules and
# helps keep the Makefile maintenence well factored.


ifndef TMPDIR
TMPDIR := /tmp
endif

EBIN_DIR=ebin
export BROKER_DIR=../rabbitmq-server
export INCLUDE_DIR=include
export INCLUDE_SERV_DIR=$(BROKER_DIR)/include
TEST_DIR=test
SOURCE_DIR=src
DIST_DIR=dist
DEPS_DIR=deps
DOC_DIR=doc

ifeq ("$(ERL_LIBS)", "")
	ERL_LIBS :=
else
	ERL_LIBS := :$(ERL_LIBS)
endif

ERL_PATH ?=

DEPS=$(shell erl -noshell -eval '{ok,[{_,_,[_,_,{modules, Mods},_,_,_]}]} = \
                                 file:consult("rabbit_common.app"), \
                                 [io:format("~p ",[M]) || M <- Mods], halt().')

PACKAGE=amqp_client
PACKAGE_NAME=$(PACKAGE).ez
COMMON_PACKAGE=rabbit_common
COMMON_PACKAGE_NAME=$(COMMON_PACKAGE).ez

COMPILE_DEPS=$(DEPS_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)/rabbit.hrl \
             $(DEPS_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)/rabbit_framing.hrl \
             $(DEPS_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

BROKER_HEADERS=$(wildcard $(BROKER_DIR)/$(INCLUDE_DIR)/*.hrl)
BROKER_SOURCES=$(wildcard $(BROKER_DIR)/$(SOURCE_DIR)/*.erl)

LIBS_PATH=ERL_LIBS=$(DEPS_DIR):$(DIST_DIR)$(ERL_LIBS)
LOAD_PATH=$(EBIN_DIR) $(BROKER_DIR)/ebin $(TEST_DIR) $(ERL_PATH)

COVER_START := -s cover start -s rabbit_misc enable_cover ../rabbitmq-erlang-client
COVER_STOP := -s rabbit_misc report_cover ../rabbitmq-erlang-client -s cover stop

MKTEMP=$$(mktemp $(TMPDIR)/tmp.XXXXXXXXXX)

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R13B01 upwards (R13B is eshell 5.7.2)
#
# NB: the test assumes that version number will only contain single digits
export USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.7.1" ]; then echo "true"; else echo "false"; fi)
endif

ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

RABBITMQ_NODENAME=rabbit
PA_LOAD_PATH=-pa $(realpath $(LOAD_PATH))
RABBITMQCTL=$(BROKER_DIR)/scripts/rabbitmqctl

ifdef SSL_CERTS_DIR
SSL := true
ALL_SSL := { $(MAKE) test_ssl || OK=false; }
ALL_SSL_COVERAGE := { $(MAKE) test_ssl_coverage || OK=false; }
SSL_BROKER_ARGS := -rabbit ssl_listeners [{\\\"0.0.0.0\\\",5671}] \
	-rabbit ssl_options [{cacertfile,\\\"$(SSL_CERTS_DIR)/testca/cacert.pem\\\"},{certfile,\\\"$(SSL_CERTS_DIR)/server/cert.pem\\\"},{keyfile,\\\"$(SSL_CERTS_DIR)/server/key.pem\\\"},{verify,verify_peer},{fail_if_no_peer_cert,true}] \
	-erlang_client_ssl_dir \"$(SSL_CERTS_DIR)\"
else
SSL := @echo No SSL_CERTS_DIR defined. && false
ALL_SSL := true
ALL_SSL_COVERAGE := true
SSL_BROKER_ARGS :=
endif

PLT=$(HOME)/.dialyzer_plt
DIALYZER_CALL=dialyzer --plt $(PLT)

.PHONY: all compile compile_tests run run_in_broker dialyzer dialyze_all \
	add_broker_to_plt prepare_tests all_tests test_suites \
	test_suites_coverage run_test_broker start_test_broker_node \
	stop_test_broker_node test_network test_direct test_network_coverage \
	test_direct_coverage test_common_package clean source_tarball package \
	boot_broker unboot_broker

all: package

common_clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f erl_crash.dump
	rm -fr $(DOC_DIR)
	$(MAKE) -C $(TEST_DIR) clean

compile: $(TARGETS)

compile_tests: $(TEST_DIR) $(COMPILE_DEPS) $(EBIN_DIR)/$(PACKAGE).app
	$(MAKE) -C $(TEST_DIR)

run: compile $(EBIN_DIR)/$(PACKAGE).app
	$(LIBS_PATH) erl -pa $(LOAD_PATH)

run_in_broker: compile $(BROKER_DIR) $(EBIN_DIR)/$(PACKAGE).app
	$(MAKE) RABBITMQ_SERVER_START_ARGS='$(PA_LOAD_PATH)' -C $(BROKER_DIR) run

dialyze: $(TARGETS)
	$(DIALYZER_CALL) -c $^

dialyze_all: $(TARGETS) $(TEST_TARGETS)
	$(DIALYZER_CALL) -c $^

add_broker_to_plt: $(BROKER_DIR)/ebin
	$(DIALYZER_CALL) --add_to_plt -r $<

$(DOC_DIR)/overview.edoc: $(SOURCE_DIR)/overview.edoc.in
	mkdir -p $(DOC_DIR)
	sed -e 's:%%VERSION%%:$(VERSION):g' < $< > $@

$(DOC_DIR)/index.html: $(COMPILE_DEPS) $(DOC_DIR)/overview.edoc $(SOURCES)
	$(LIBS_PATH) erl -noshell -eval 'edoc:application(amqp_client, ".", [{preprocess, true}])' -run init stop

doc: $(DOC_DIR)/index.html

###############################################################################
##  Packaging
###############################################################################

$(DIST_DIR)/$(PACKAGE_NAME): $(TARGETS) $(EBIN_DIR)/$(PACKAGE).app
	rm -rf $(DIST_DIR)/$(PACKAGE)
	mkdir -p $(DIST_DIR)/$(PACKAGE)/$(EBIN_DIR)
	cp -r $(EBIN_DIR)/*.beam $(DIST_DIR)/$(PACKAGE)/$(EBIN_DIR)
	cp -r $(EBIN_DIR)/*.app $(DIST_DIR)/$(PACKAGE)/$(EBIN_DIR)
	mkdir -p $(DIST_DIR)/$(PACKAGE)/$(INCLUDE_DIR)
	cp -r $(INCLUDE_DIR)/* $(DIST_DIR)/$(PACKAGE)/$(INCLUDE_DIR)
	(cd $(DIST_DIR); zip -r $(PACKAGE_NAME) $(PACKAGE))

package: $(DIST_DIR)/$(PACKAGE_NAME)

###############################################################################
##  Internal targets
###############################################################################

$(COMPILE_DEPS): $(DIST_DIR)/$(COMMON_PACKAGE_NAME)
	mkdir -p $(DEPS_DIR)
	unzip -o -d $(DEPS_DIR) $(DIST_DIR)/$(COMMON_PACKAGE_NAME)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(COMPILE_DEPS)
	$(LIBS_PATH) erlc $(ERLC_OPTS) $<

$(TEST_DIR)/%.beam: compile_tests

$(BROKER_DIR):
	test -e $(BROKER_DIR)
	$(MAKE_BROKER)

$(DIST_DIR):
	mkdir -p $@

$(DEPS_DIR):
	mkdir -p $@

