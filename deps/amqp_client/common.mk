# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is RabbitMQ.
#
# The Initial Developer of the Original Code is GoPivotal, Inc.
# Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
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
BROKER_DIR=../rabbitmq-server
export INCLUDE_DIR=include
TEST_DIR=test
SOURCE_DIR=src
DIST_DIR=dist
DEPS_DIR=deps
DOC_DIR=doc
DEPS_FILE=deps.mk

ifeq ("$(ERL_LIBS)", "")
	ERL_LIBS :=
else
	ERL_LIBS := :$(ERL_LIBS)
endif

ERL_PATH ?=

PACKAGE=amqp_client
PACKAGE_DIR=$(PACKAGE)-$(VERSION)
PACKAGE_NAME_EZ=$(PACKAGE_DIR).ez
COMMON_PACKAGE=rabbit_common
export COMMON_PACKAGE_DIR=$(COMMON_PACKAGE)-$(VERSION)
COMMON_PACKAGE_EZ=$(COMMON_PACKAGE_DIR).ez
NODE_NAME=amqp_client

DEPS=$(shell erl -noshell -eval '{ok,[{_,_,[_,_,{modules, Mods},_,_,_]}]} = \
                                 file:consult("$(COMMON_PACKAGE).app.in"), \
                                 [io:format("~p ",[M]) || M <- Mods], halt().')

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

LIBS_PATH_UNIX=$(DEPS_DIR):$(DIST_DIR)$(ERL_LIBS)
IS_CYGWIN=$(shell if [ $(shell expr "$(shell uname -s)" : 'CYGWIN_NT') -gt 0 ]; then echo "true"; else echo "false"; fi)
ifeq ($(IS_CYGWIN),true)
    LIBS_PATH=ERL_LIBS="$(shell cygpath -wp $(LIBS_PATH_UNIX))"
else
    LIBS_PATH=ERL_LIBS=$(LIBS_PATH_UNIX)
endif

LOAD_PATH=$(EBIN_DIR) $(TEST_DIR) $(ERL_PATH)

RUN:=$(LIBS_PATH) erl -pa $(LOAD_PATH) -sname $(NODE_NAME)

MKTEMP=$$(mktemp $(TMPDIR)/tmp.XXXXXXXXXX)

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R13B01 upwards (R13B is eshell 5.7.2)
#
# NB: do not mark this variable for export, otherwise it will
# override the test in rabbitmq-server's Makefile when it does the
# make -C, which causes problems whenever the test here and the test
# there compare system_info(version) against *different* eshell
# version numbers.
USE_SPECS:=$(shell erl -noshell -eval 'io:format([list_to_integer(X) || X <- string:tokens(erlang:system_info(version), ".")] >= [5,7,2]), halt().')
endif

ERLC_OPTS=-I $(INCLUDE_DIR) -pa $(EBIN_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(if $(filter true,$(USE_SPECS)),-Duse_specs)

RABBITMQ_NODENAME=rabbit
PA_LOAD_PATH=-pa $(realpath $(LOAD_PATH))
RABBITMQCTL=$(BROKER_DIR)/scripts/rabbitmqctl

ifdef SSL_CERTS_DIR
SSL := true
ALL_SSL := { $(MAKE) test_ssl || OK=false; }
ALL_SSL_COVERAGE := { $(MAKE) test_ssl_coverage || OK=false; }
SSL_BROKER_ARGS := -rabbit ssl_listeners [{\\\"0.0.0.0\\\",5671},{\\\"::1\\\",5671}] \
	-rabbit ssl_options [{cacertfile,\\\"$(SSL_CERTS_DIR)/testca/cacert.pem\\\"},{certfile,\\\"$(SSL_CERTS_DIR)/server/cert.pem\\\"},{keyfile,\\\"$(SSL_CERTS_DIR)/server/key.pem\\\"},{verify,verify_peer},{fail_if_no_peer_cert,true}]
SSL_CLIENT_ARGS := -erlang_client_ssl_dir $(SSL_CERTS_DIR)
else
SSL := @echo No SSL_CERTS_DIR defined. && false
ALL_SSL := true
ALL_SSL_COVERAGE := true
SSL_BROKER_ARGS :=
SSL_CLIENT_ARGS :=
endif

# Versions prior to this are not supported
NEED_MAKE := 3.80
ifneq "$(NEED_MAKE)" "$(firstword $(sort $(NEED_MAKE) $(MAKE_VERSION)))"
$(error Versions of make prior to $(NEED_MAKE) are not supported)
endif

# .DEFAULT_GOAL introduced in 3.81
DEFAULT_GOAL_MAKE := 3.81
ifneq "$(DEFAULT_GOAL_MAKE)" "$(firstword $(sort $(DEFAULT_GOAL_MAKE) $(MAKE_VERSION)))"
.DEFAULT_GOAL=all
endif

all: package

common_clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f erl_crash.dump
	rm -rf $(DEPS_DIR)
	rm -rf $(DOC_DIR)
	rm -f $(DEPS_FILE)
	$(MAKE) -C $(TEST_DIR) clean

compile: $(TARGETS) $(EBIN_DIR)/$(PACKAGE).app

run: compile
	$(RUN)

###############################################################################
##  Packaging
###############################################################################

$(DIST_DIR)/$(PACKAGE_NAME_EZ): $(TARGETS) $(EBIN_DIR)/$(PACKAGE).app | $(DIST_DIR)
	rm -f $@
	rm -rf $(DIST_DIR)/$(PACKAGE_DIR)
	mkdir -p $(DIST_DIR)/$(PACKAGE_DIR)/$(EBIN_DIR)
	mkdir -p $(DIST_DIR)/$(PACKAGE_DIR)/$(INCLUDE_DIR)
	cp -r $(EBIN_DIR)/*.beam $(DIST_DIR)/$(PACKAGE_DIR)/$(EBIN_DIR)
	cp -r $(EBIN_DIR)/*.app $(DIST_DIR)/$(PACKAGE_DIR)/$(EBIN_DIR)
	mkdir -p $(DIST_DIR)/$(PACKAGE_DIR)/$(INCLUDE_DIR)
	cp -r $(INCLUDE_DIR)/* $(DIST_DIR)/$(PACKAGE_DIR)/$(INCLUDE_DIR)
	(cd $(DIST_DIR); zip -q -r $(PACKAGE_NAME_EZ) $(PACKAGE_DIR))

package: $(DIST_DIR)/$(PACKAGE_NAME_EZ)

###############################################################################
##  Internal targets
###############################################################################

$(DEPS_DIR)/$(COMMON_PACKAGE_DIR): $(DIST_DIR)/$(COMMON_PACKAGE_EZ) | $(DEPS_DIR)
	rm -rf $(DEPS_DIR)/$(COMMON_PACKAGE_DIR)
	mkdir -p $(DEPS_DIR)/$(COMMON_PACKAGE_DIR)
	unzip -q -o $< -d $(DEPS_DIR)

$(DEPS_FILE): $(SOURCES) $(INCLUDES)
	rm -f $@
	echo $(subst : ,:,$(foreach FILE,$^,$(FILE):)) | escript $(BROKER_DIR)/generate_deps $@ $(EBIN_DIR)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(DEPS_DIR)/$(COMMON_PACKAGE_DIR) | $(DEPS_FILE)
	$(LIBS_PATH) erlc $(ERLC_OPTS) $<

$(DEPS_DIR):
	mkdir -p $@

# Note that all targets which depend on clean must have clean in their
# name.  Also any target that doesn't depend on clean should not have
# clean in its name, unless you know that you don't need any of the
# automatic dependency generation for that target.

# We want to load the dep file if *any* target *doesn't* contain
# "clean" - i.e. if removing all clean-like targets leaves something

ifeq "$(MAKECMDGOALS)" ""
TESTABLEGOALS:=$(.DEFAULT_GOAL)
else
TESTABLEGOALS:=$(MAKECMDGOALS)
endif

ifneq "$(strip $(patsubst clean%,,$(patsubst %clean,,$(TESTABLEGOALS))))" ""
-include $(DEPS_FILE)
endif
