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

DEPS=$(shell erl -noshell -eval '{ok,[{_,_,[_,_,{modules, Mods},_,_,_]}]} = \
                                 file:consult("rabbit_common.app"), \
                                 [io:format("~p ",[M]) || M <- Mods], halt().')

VERSION=0.0.0
SOURCE_PACKAGE_NAME=$(PACKAGE)-$(VERSION)-src

INFILES=$(shell find . -name '*.app.in')
INTARGETS=$(patsubst %.in, %, $(INFILES))

.PHONY: common_package

include common.mk

clean: common_clean
	rm -f $(INTARGETS)
	rm -fr $(DIST_DIR)

%.app: %.app.in
	sed -e 's:%%VSN%%:$(VERSION):g' < $< > $@

###############################################################################
##  Testing
###############################################################################

include test.mk

test_common_package: common_package package prepare_tests
	$(MAKE) start_test_broker_node
	OK=true && \
	TMPFILE=$(MKTEMP) && \
	    { $(LIBS_PATH) erl -noshell -pa $(TEST_DIR) \
	    -eval 'error_logger:tty(false), network_client_SUITE:test(), halt().' 2>&1 | \
		tee $$TMPFILE || OK=false; } && \
	{ egrep "All .+ tests (successful|passed)." $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) stop_test_broker_node && \
	$$OK

###############################################################################
##  Packaging
###############################################################################

COPY=cp -pR

common_package: $(DIST_DIR)/$(COMMON_PACKAGE_NAME)

$(DIST_DIR)/$(COMMON_PACKAGE_NAME): $(BROKER_SOURCES) $(BROKER_HEADERS) $(COMMON_PACKAGE).app
	$(MAKE) -C $(BROKER_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)
	cp $(COMMON_PACKAGE).app $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)
	$(foreach DEP, $(DEPS), \
        ( cp $(BROKER_DIR)/$(EBIN_DIR)/$(DEP).beam \
          $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR) \
        );)
	cp $(BROKER_DIR)/$(INCLUDE_DIR)/*.hrl $(DIST_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)
	(cd $(DIST_DIR); zip -r $(COMMON_PACKAGE_NAME) $(COMMON_PACKAGE))

source_tarball: clean $(DIST_DIR)/$(COMMON_PACKAGE_NAME)
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(DIST_DIR)
	$(COPY) $(DIST_DIR)/$(COMMON_PACKAGE_NAME) $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(DIST_DIR)/
	$(COPY) README $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/
	$(COPY) common.mk $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/
	$(COPY) Makefile.in $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/Makefile
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(SOURCE_DIR)
	$(COPY) $(SOURCE_DIR)/*.erl $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(SOURCE_DIR)/
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(EBIN_DIR)
	$(COPY) $(EBIN_DIR)/*.app $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(EBIN_DIR)/
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(INCLUDE_DIR)
	$(COPY) $(INCLUDE_DIR)/*.hrl $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(INCLUDE_DIR)/
	mkdir -p $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(TEST_DIR)
	$(COPY) $(TEST_DIR)/*.erl $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(TEST_DIR)/
	$(COPY) $(TEST_DIR)/Makefile $(DIST_DIR)/$(SOURCE_PACKAGE_NAME)/$(TEST_DIR)/
	cd $(DIST_DIR) ; tar cvzf $(SOURCE_PACKAGE_NAME).tar.gz $(SOURCE_PACKAGE_NAME)
