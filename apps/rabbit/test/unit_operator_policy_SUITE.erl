%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_operator_policy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          merge_operator_policy_definitions
        ]}
    ].

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.


%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

merge_operator_policy_definitions(_Config) ->
    P1 = undefined,
    P2 = [{definition, [{<<"message-ttl">>, 3000}]}],
    ?assertEqual([{<<"message-ttl">>, 3000}], rabbit_policy:merge_operator_definitions(P1, P2)),
    ?assertEqual([{<<"message-ttl">>, 3000}], rabbit_policy:merge_operator_definitions(P2, P1)),

    ?assertEqual([{<<"message-ttl">>, 3000}], rabbit_policy:merge_operator_definitions(P1, rabbit_data_coercion:to_map(P2))),
    ?assertEqual([{<<"message-ttl">>, 3000}], rabbit_policy:merge_operator_definitions(rabbit_data_coercion:to_map(P2), P1)),

    ?assertEqual(undefined, rabbit_policy:merge_operator_definitions(undefined, undefined)),

    ?assertEqual([], rabbit_policy:merge_operator_definitions([],  [])),
    ?assertEqual([], rabbit_policy:merge_operator_definitions(#{}, [])),
    ?assertEqual([], rabbit_policy:merge_operator_definitions(#{}, #{})),
    ?assertEqual([], rabbit_policy:merge_operator_definitions([],  #{})),

    %% operator policy takes precedence
    ?assertEqual([{<<"message-ttl">>, 3000}], rabbit_policy:merge_operator_definitions(
      [{definition, [
        {<<"message-ttl">>, 5000}
      ]}],
      [{definition, [
        {<<"message-ttl">>, 3000}
      ]}]
    )),

    ?assertEqual([{<<"delivery-limit">>, 20},
                  {<<"message-ttl">>, 3000}],
                  rabbit_policy:merge_operator_definitions(
                    [{definition, [
                      {<<"message-ttl">>, 5000},
                      {<<"delivery-limit">>, 20}
                    ]}],
                    [{definition, [
                      {<<"message-ttl">>, 3000}
                    ]}])
    ),

    ?assertEqual(
                 [{<<"delivery-limit">>, 20},
                  {<<"message-ttl">>,    3000},
                  {<<"unknown">>,        <<"value">>}],

                  rabbit_policy:merge_operator_definitions(
                    #{definition => #{
                      <<"message-ttl">> => 5000,
                      <<"delivery-limit">> => 20
                    }},
                    #{definition => #{
                      <<"message-ttl">> => 3000,
                      <<"unknown">> => <<"value">>
                    }})
    ),

    ?assertEqual(
                 [{<<"delivery-limit">>, 20},
                  {<<"message-ttl">>, 3000}],

                  rabbit_policy:merge_operator_definitions(
                    #{definition => #{
                      <<"message-ttl">> => 5000,
                      <<"delivery-limit">> => 20
                    }},
                    [{definition, [
                      {<<"message-ttl">>, 3000}
                    ]}])
    ),

    passed.
