%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
