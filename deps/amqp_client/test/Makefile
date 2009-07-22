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
#   Contributor(s): ________________________
#


TEST_SOURCES=$(wildcard *.erl)
TEST_TARGETS=$(patsubst %.erl, %.beam, $(TEST_SOURCES))
INCLUDES=$(wildcard ../$(INCLUDE_DIR)/*.hrl)

ERLC_OPTS=-I ../$(INCLUDE_DIR) -I ../$(INCLUDE_SERV_DIR) -o ./ -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

all: compile

compile: $(TEST_TARGETS)

%.beam: %.erl $(INCLUDES)
	erlc $(ERLC_OPTS) $<

clean:
	rm -f *.beam
