%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_inbroker_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_federation.hrl").

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
          serialisation,
          scratch_space,
          remove_credentials,
          get_connection_name,
          upstream_validation,
          upstream_set_validation
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

scratch_space(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, scratch_space1, []).

scratch_space1() ->
    A = <<"A">>,
    B = <<"B">>,
    DB = rabbit_federation_db,
    with_exchanges(
      fun(#exchange{name = N}) ->
              DB:set_active_suffix(N, upstream(x), A),
              DB:set_active_suffix(N, upstream(y), A),
              DB:prune_scratch(N, [upstream(y), upstream(z)]),
              DB:set_active_suffix(N, upstream(y), B),
              DB:set_active_suffix(N, upstream(z), A),
              none = DB:get_active_suffix(N, upstream(x), none),
              B    = DB:get_active_suffix(N, upstream(y), none),
              A    = DB:get_active_suffix(N, upstream(z), none)
      end).

remove_credentials(Config) ->
    Test0 = fun (In, Exp) ->
                    Act = rabbit_ct_broker_helpers:rpc(Config, 0,
                      rabbit_federation_upstream, remove_credentials, [In]),
                    Exp = Act
            end,
    Cat = fun (Bs) ->
                  list_to_binary(lists:append([binary_to_list(B) || B <- Bs]))
          end,
    Test = fun (Scheme, Rest) ->
                   Exp = Cat([Scheme, Rest]),
                   Test0(Exp,                                   Exp),
                   Test0(Cat([Scheme, <<"user@">>, Rest]),      Exp),
                   Test0(Cat([Scheme, <<"user:pass@">>, Rest]), Exp)
           end,
    Test(<<"amqp://">>,  <<"">>),
    Test(<<"amqp://">>,  <<"localhost">>),
    Test(<<"amqp://">>,  <<"localhost/">>),
    Test(<<"amqp://">>,  <<"localhost/foo">>),
    Test(<<"amqp://">>,  <<"localhost:5672">>),
    Test(<<"amqp://">>,  <<"localhost:5672/foo">>),
    Test(<<"amqps://">>, <<"localhost:5672/%2f">>),
    ok.

get_connection_name(Config) ->
    Amqqueue = rabbit_ct_broker_helpers:rpc(
          Config, 0,
          amqqueue, new, [rabbit_misc:r(<<"/">>, queue, <<"queue">>),
                          self(),
                          false,
                          false,
                          none,
                          [],
                          undefined,
                          #{},
                          classic]),
    AmqqueueWithPolicy = amqqueue:set_policy(Amqqueue, [{name, <<"my.federation.policy">>}]),
    AmqqueueWithEmptyPolicy = amqqueue:set_policy(Amqqueue, []),


    <<"Federation link (upstream: my.upstream, policy: my.federation.policy)">> = rabbit_federation_link_util:get_connection_name(
        #upstream{name = <<"my.upstream">>},
        #upstream_params{x_or_q = AmqqueueWithPolicy}
    ),
    <<"Federation link (upstream: my.upstream, policy: my.federation.policy)">> = rabbit_federation_link_util:get_connection_name(
        #upstream{name = <<"my.upstream">>},
        #upstream_params{x_or_q = #exchange{policy = [{name, <<"my.federation.policy">>}]}}
    ),
    <<"Federation link">> = rabbit_federation_link_util:get_connection_name(
        #upstream{},
        #upstream_params{x_or_q = AmqqueueWithEmptyPolicy}
    ),
    <<"Federation link">> = rabbit_federation_link_util:get_connection_name(
        #upstream{},
        #upstream_params{x_or_q = #exchange{policy = []}}
    ),
    <<"Federation link">> = rabbit_federation_link_util:get_connection_name(
        whatever,
        whatever
    ),
    ok.

upstream_set_validation(_Config) ->
    ?assertEqual(rabbit_federation_parameters:validate(<<"/">>, <<"federation-upstream-set">>,
                                         <<"a-name">>,
                                          [[{<<"upstream">>, <<"devtest1">>}],
                                           [{<<"upstream">>, <<"devtest2">>}]],
                                         <<"acting-user">>),
                 [[ok], [ok]]),
    ?assertEqual(rabbit_federation_parameters:validate(<<"/">>, <<"federation-upstream-set">>,
                                         <<"a-name">>,
                                          [#{<<"upstream">> => <<"devtest3">>},
                                           #{<<"upstream">> => <<"devtest4">>}],
                                         <<"acting-user">>),
                 [[ok], [ok]]),
    ok.

upstream_validation(_Config) ->
    ?assertEqual(rabbit_federation_parameters:validate(<<"/">>, <<"federation-upstream">>,
                                         <<"a-name">>,
                                          [{<<"uri">>, <<"amqp://127.0.0.1/%2f">>}],
                                         <<"acting-user">>),
                 [ok]),
    ?assertEqual(rabbit_federation_parameters:validate(<<"/">>, <<"federation-upstream">>,
                                         <<"a-name">>,
                                          #{<<"uri">> => <<"amqp://127.0.0.1/%2f">>},
                                         <<"acting-user">>),
                 [ok]),
    ok.

with_exchanges(Fun) ->
    rabbit_exchange:declare(r(?US_NAME), fanout, false, false, false, [],
                            <<"acting-user">>),
    X = rabbit_exchange:declare(r(?DS_NAME), fanout, false, false, false, [],
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

upstream(UpstreamName) ->
    #upstream{name          = atom_to_list(UpstreamName),
              exchange_name = <<"upstream">>}.
