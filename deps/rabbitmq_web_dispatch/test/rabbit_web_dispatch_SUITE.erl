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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_dispatch_SUITE).


-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                query_static_resource_test,
                                add_idempotence_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_http_extra]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

query_static_resource_test(Config) ->
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_http_extra),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, query_static_resource_test1, [Config, Host, Port]).
query_static_resource_test1(_Config, Host, Port) ->
    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_web_dispatch:register_static_context(test, [{port, Port}],
                                                "rabbit_web_dispatch_test",
                                                ?MODULE, "test/priv/www", "Test"),
    {ok, {_Status, _Headers, Body}} =
        httpc:request(format("http://~s:~w/rabbit_web_dispatch_test/index.html", [Host, Port])),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0),

    passed.

add_idempotence_test(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_http_extra),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, add_idempotence_test1, [Port]).
add_idempotence_test1(Port) ->
    F = fun(_Req) -> ok end,
    L = {"/foo", "Foo"},
    rabbit_web_dispatch_registry:add(foo, [{port, Port}], F, F, L),
    rabbit_web_dispatch_registry:add(foo, [{port, Port}], F, F, L),
    ?assertEqual(
       1, length([ok || {"/foo", _, _} <-
                            rabbit_web_dispatch_registry:list_all()])),
    passed.



format(Fmt, Val) ->
    lists:flatten(io_lib:format(Fmt, Val)).
