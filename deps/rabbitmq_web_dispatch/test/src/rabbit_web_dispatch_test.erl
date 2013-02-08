%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_web_dispatch:register_static_context(test, [{port, 12345}],
                                                "rabbit_web_dispatch_test",
                                                ?MODULE, "priv/www", "Test"),
    {ok, {_Status, _Headers, Body}} =
        httpc:request("http://localhost:12345/rabbit_web_dispatch_test/index.html"),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0).

add_idempotence_test() ->
    F = fun(_Req) -> ok end,
    L = {"/foo", "Foo"},
    rabbit_web_dispatch_registry:add(foo, [{port, 12345}], F, F, L),
    rabbit_web_dispatch_registry:add(foo, [{port, 12345}], F, F, L),
    ?assertEqual(
       1, length([ok || {"/foo", _, _} <-
                            rabbit_web_dispatch_registry:list_all()])),
    passed.
