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
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mochiweb_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_mochiweb:register_static_context(test, "rabbit_mochiweb_test",
                                            ?MODULE, "priv/www", "Test"),
    {ok, {_Status, _Headers, Body}} =
        httpc:request("http://localhost:55670/rabbit_mochiweb_test/index.html"),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0).

add_idempotence_test() ->
    F = fun(_Req) -> ok end,
    L = {"/foo", "Foo"},
    rabbit_mochiweb_registry:add(foo, F, F, L),
    rabbit_mochiweb_registry:add(foo, F, F, L),
    ?assertEqual(
       1, length([ok || {"/foo", _, _} <-
                            rabbit_mochiweb_registry:list_all()])),
    passed.
