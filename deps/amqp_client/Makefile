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
# The Initial Developer of the Original Code is VMware, Inc.
# Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
#

VERSION=0.0.0

SOURCE_PACKAGE_DIR=$(PACKAGE)-$(VERSION)-src
SOURCE_PACKAGE_TAR_GZ=$(SOURCE_PACKAGE_DIR).tar.gz

BROKER_HEADERS=$(wildcard $(BROKER_DIR)/$(INCLUDE_DIR)/*.hrl)
BROKER_SOURCES=$(wildcard $(BROKER_DIR)/$(SOURCE_DIR)/*.erl)
BROKER_DEPS=$(BROKER_HEADERS) $(BROKER_SOURCES)

INFILES=$(shell find . -name '*.app.in')
INTARGETS=$(patsubst %.in, %, $(INFILES))

WEB_URL=http://www.rabbitmq.com/

include common.mk

run_in_broker: compile $(BROKER_DEPS) $(EBIN_DIR)/$(PACKAGE).app
	$(MAKE) RABBITMQ_SERVER_START_ARGS='$(PA_LOAD_PATH)' -C $(BROKER_DIR) run

clean: common_clean
	rm -f $(INTARGETS)
	rm -rf $(DIST_DIR)

distribution: documentation source_tarball package

%.app: %.app.in $(SOURCES) $(BROKER_DIR)/generate_app
	escript  $(BROKER_DIR)/generate_app $< $@ $(SOURCE_DIR)
	sed 's/%%VSN%%/$(VERSION)/' $@ > $@.tmp && mv $@.tmp $@

###############################################################################
##  Dialyzer
###############################################################################

RABBIT_PLT=$(BROKER_DIR)/rabbit.plt

dialyze: $(RABBIT_PLT) $(TARGETS)
	dialyzer --plt $(RABBIT_PLT) --no_native -Wrace_conditions $(TARGETS)

.PHONY: $(RABBIT_PLT)
$(RABBIT_PLT):
	$(MAKE) -C $(BROKER_DIR) create-plt

###############################################################################
##  Documentation
###############################################################################

documentation: $(DOC_DIR)/index.html

$(DOC_DIR)/overview.edoc: $(SOURCE_DIR)/overview.edoc.in
	mkdir -p $(DOC_DIR)
	sed -e 's:%%VERSION%%:$(VERSION):g' < $< > $@

$(DOC_DIR)/index.html: $(DEPS_DIR)/$(COMMON_PACKAGE_DIR) $(DOC_DIR)/overview.edoc $(SOURCES)
	$(LIBS_PATH) erl -noshell -eval 'edoc:application(amqp_client, ".", [{preprocess, true}, {macros, [{edoc, true}]}])' -run init stop

###############################################################################
##  Testing
###############################################################################

include test.mk

compile_tests: $(TEST_TARGETS) $(EBIN_DIR)/$(PACKAGE).app

$(TEST_TARGETS): $(TEST_DIR)

.PHONY: $(TEST_DIR)
$(TEST_DIR): $(DEPS_DIR)/$(COMMON_PACKAGE_DIR)
	$(MAKE) -C $(TEST_DIR)

###############################################################################
##  Packaging
###############################################################################

COPY=cp -pR

$(DIST_DIR)/$(COMMON_PACKAGE_EZ): $(BROKER_DEPS) $(COMMON_PACKAGE).app | $(DIST_DIR)
	rm -f $@
	$(MAKE) -C $(BROKER_DIR)
	rm -rf $(DIST_DIR)/$(COMMON_PACKAGE_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE_DIR)/$(INCLUDE_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE_DIR)/$(EBIN_DIR)
	cp $(COMMON_PACKAGE).app $(DIST_DIR)/$(COMMON_PACKAGE_DIR)/$(EBIN_DIR)/
	$(foreach DEP, $(DEPS), \
	    ( cp $(BROKER_DIR)/ebin/$(DEP).beam $(DIST_DIR)/$(COMMON_PACKAGE_DIR)/$(EBIN_DIR)/ \
	    );)
	cp $(BROKER_DIR)/include/*.hrl $(DIST_DIR)/$(COMMON_PACKAGE_DIR)/$(INCLUDE_DIR)/
	(cd $(DIST_DIR); zip -q -r $(COMMON_PACKAGE_EZ) $(COMMON_PACKAGE_DIR))

source_tarball: $(DIST_DIR)/$(COMMON_PACKAGE_EZ) $(EBIN_DIR)/$(PACKAGE).app | $(DIST_DIR)
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(DIST_DIR)
	$(COPY) $(DIST_DIR)/$(COMMON_PACKAGE_EZ) $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(DIST_DIR)/
	$(COPY) README.in $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/README
	elinks -dump -no-references -no-numbering $(WEB_URL)build-erlang-client.html >> $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/README
	$(COPY) common.mk $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/
	$(COPY) test.mk $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/
	sed 's/%%VSN%%/$(VERSION)/' Makefile.in > $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/Makefile
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(SOURCE_DIR)
	$(COPY) $(SOURCE_DIR)/*.erl $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(SOURCE_DIR)/
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(EBIN_DIR)
	$(COPY) $(EBIN_DIR)/*.app $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(EBIN_DIR)/
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(INCLUDE_DIR)
	$(COPY) $(INCLUDE_DIR)/*.hrl $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(INCLUDE_DIR)/
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(TEST_DIR)
	$(COPY) $(TEST_DIR)/*.erl $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(TEST_DIR)/
	$(COPY) $(TEST_DIR)/Makefile $(DIST_DIR)/$(SOURCE_PACKAGE_DIR)/$(TEST_DIR)/
	cd $(DIST_DIR) ; tar czf $(SOURCE_PACKAGE_TAR_GZ) $(SOURCE_PACKAGE_DIR)

$(DIST_DIR):
	mkdir -p $@
