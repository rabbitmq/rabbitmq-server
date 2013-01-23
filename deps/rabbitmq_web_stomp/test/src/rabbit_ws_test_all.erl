%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2012-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_ws_test_all).

-export([all_tests/0]).

all_tests() ->
    ok = eunit:test(rabbit_ws_test_raw_websocket, [verbose]),
    ok = eunit:test(rabbit_ws_test_sockjs_websocket, [verbose]),
    ok.

