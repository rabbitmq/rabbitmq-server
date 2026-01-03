%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_inbroker_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(US_NAME, <<"upstream">>).
-define(DS_NAME, <<"fed.downstream">>).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          serialisation
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% Test that we apply binding changes in the correct order even when
%% they arrive out of order.
serialisation(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, serialisation1, []).

serialisation1() ->
    with_exchanges(
      fun(X) ->
              [B1, B2, B3] = [b(K) || K <- [<<"1">>, <<"2">>, <<"3">>]],
              remove_bindings(4, X, [B1, B3]),
              add_binding(5, X, B1),
              add_binding(1, X, B1),
              add_binding(2, X, B2),
              add_binding(3, X, B3),
              %% List of lists because one for each link
              Keys = rabbit_federation_exchange_link:list_routing_keys(
                       X#exchange.name),
              [[<<"1">>, <<"2">>]] =:= Keys
      end).

with_exchanges(Fun) ->
    {ok, _} = rabbit_exchange:declare(
                r(?US_NAME), fanout, false, false, false, [],
                <<"acting-user">>),
    {ok, X} = rabbit_exchange:declare(
                r(?DS_NAME), fanout, false, false, false, [],
                <<"acting-user">>),
    Fun(X),
    %% Delete downstream first or it will recreate the upstream
    rabbit_exchange:delete(r(?DS_NAME), false, <<"acting-user">>),
    rabbit_exchange:delete(r(?US_NAME), false, <<"acting-user">>),
    ok.

add_binding(Ser, X, B) ->
    rabbit_federation_exchange:add_binding(transaction, X, B),
    rabbit_federation_exchange:add_binding(Ser, X, B).

remove_bindings(Ser, X, Bs) ->
    rabbit_federation_exchange:remove_bindings(transaction, X, Bs),
    rabbit_federation_exchange:remove_bindings(Ser, X, Bs).

r(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).

b(Key) ->
    #binding{source = ?DS_NAME, destination = <<"whatever">>,
             key = Key, args = []}.
