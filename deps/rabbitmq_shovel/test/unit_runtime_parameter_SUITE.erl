%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_runtime_parameter_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-define(EXCHANGE,    <<"test_exchange">>).
-define(TO_SHOVEL,   <<"to_the_shovel">>).
-define(FROM_SHOVEL, <<"from_the_shovel">>).
-define(UNSHOVELLED, <<"unshovelled">>).
-define(SHOVELLED,   <<"shovelled">>).
-define(TIMEOUT,     1000).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [parallel], [
          parse_amqp091_maps,
          parse_amqp091_proplists,
          parse_amqp091_empty_maps,
          parse_amqp091_empty_proplists,
          parse_amqp10,
          parse_amqp10_minimal,
          validate_amqp10,
          validate_amqp10_with_a_map,
          test_src_protocol_defaults,
          test_src_protocol_explicit,
          test_dest_protocol_defaults,
          test_dest_protocol_explicit,
          test_protocols_defaults,
          test_protocols_explicit,
          test_protocols_mixed,
          test_protocols_with_maps
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Secret = crypto:strong_rand_bytes(128),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:rpc(Config2, 0, credentials_obfuscation,
                                      set_secret, [Secret]),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
  Config.

end_per_testcase(_Testcase, Config) ->
  Config.


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

parse_amqp091_maps(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_amqp091_maps_0, []).

parse_amqp091_maps_0() ->
    Params =
        [{<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-protocol">>, <<"amqp091">>},
         {<<"dest-protocol">>, <<"amqp091">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"add-forward-headers">>, true},
         {<<"add-timestamp-header">>, true},
         {<<"publish-properties">>, #{<<"cluster_id">> => <<"x">>,
                                      <<"delivery_mode">> => 2}},
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"src-delete-after">>, <<"queue-length">>},
         {<<"prefetch-count">>, 30},
         {<<"reconnect-delay">>, 1001},
         {<<"src-queue">>, <<"a-src-queue">>},
         {<<"dest-queue">>, <<"a-dest-queue">>}
        ],

    test_parse_amqp091(Params).

parse_amqp091_proplists(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_amqp091_proplists_0, []).

parse_amqp091_proplists_0() ->
    Params =
        [{<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-protocol">>, <<"amqp091">>},
         {<<"dest-protocol">>, <<"amqp091">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-add-forward-headers">>, true},
         {<<"dest-add-timestamp-header">>, true},
         {<<"dest-publish-properties">>, [{<<"cluster_id">>, <<"x">>},
                                          {<<"delivery_mode">>, 2}]},
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"src-delete-after">>, <<"queue-length">>},
         {<<"src-prefetch-count">>, 30},
         {<<"reconnect-delay">>, 1001},
         {<<"src-queue">>, <<"a-src-queue">>},
         {<<"dest-queue">>, <<"a-dest-queue">>}
        ],
    test_parse_amqp091(Params).

parse_amqp091_empty_maps(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_amqp091_empty_maps_0, []).

parse_amqp091_empty_maps_0() ->
    Params =
        [{<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-protocol">>, <<"amqp091">>},
         {<<"dest-protocol">>, <<"amqp091">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-add-forward-headers">>, true},
         {<<"dest-add-timestamp-header">>, true},
         {<<"dest-publish-properties">>, #{}},
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"src-delete-after">>, <<"queue-length">>},
         {<<"src-prefetch-count">>, 30},
         {<<"reconnect-delay">>, 1001},
         {<<"src-queue">>, <<"a-src-queue">>},
         {<<"dest-queue">>, <<"a-dest-queue">>}
        ],
    test_parse_amqp091_with_blank_proprties(Params).

parse_amqp091_empty_proplists(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_amqp091_empty_proplists_0, []).

parse_amqp091_empty_proplists_0() ->
    Params =
        [{<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-protocol">>, <<"amqp091">>},
         {<<"dest-protocol">>, <<"amqp091">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-add-forward-headers">>, true},
         {<<"dest-add-timestamp-header">>, true},
         {<<"dest-publish-properties">>, []},
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"src-delete-after">>, <<"queue-length">>},
         {<<"src-prefetch-count">>, 30},
         {<<"reconnect-delay">>, 1001},
         {<<"src-queue">>, <<"a-src-queue">>},
         {<<"dest-queue">>, <<"a-dest-queue">>}
        ],
    test_parse_amqp091_with_blank_proprties(Params).


test_parse_amqp091(Params) ->
    ObfuscatedParams = rabbit_shovel_parameters:obfuscate_uris_in_definition(Params),
    {ok, Result} = rabbit_shovel_parameters:parse({"vhost", "name"},
                                                  "my-cluster", ObfuscatedParams),
    #{ack_mode := on_publish,
      name := "name",
      reconnect_delay := 1001,
      dest := #{module := rabbit_amqp091_shovel,
                uris := ["amqp://remotehost:5672"],
                props_fun := {M, F, Args}
               },
      source := #{module := rabbit_amqp091_shovel,
                  uris := ["amqp://localhost:5672"],
                  prefetch_count := 30,
                  queue := <<"a-src-queue">>,
                  delete_after := 'queue-length'}
     } = Result,

    #'P_basic'{headers = ActualHeaders,
               delivery_mode = 2,
               cluster_id = <<"x">>} = apply(M, F, Args ++ ["amqp://localhost:5672",
                                                            "amqp://remotehost:5672",
                                                            #'P_basic'{headers = undefined}]),
    assert_amqp901_headers(ActualHeaders),
    ok.

test_parse_amqp091_with_blank_proprties(Params) ->
    ObfuscatedParams = rabbit_shovel_parameters:obfuscate_uris_in_definition(Params),
    {ok, Result} = rabbit_shovel_parameters:parse({"vhost", "name"},
                                                  "my-cluster", ObfuscatedParams),
    #{ack_mode := on_publish,
      name := "name",
      reconnect_delay := 1001,
      dest := #{module := rabbit_amqp091_shovel,
                uris := ["amqp://remotehost:5672"],
                props_fun := {M, F, Args}
               },
      source := #{module := rabbit_amqp091_shovel,
                  uris := ["amqp://localhost:5672"],
                  prefetch_count := 30,
                  queue := <<"a-src-queue">>,
                  delete_after := 'queue-length'}
     } = Result,

    #'P_basic'{headers = ActualHeaders} = apply(M, F, Args ++ ["amqp://localhost:5672",
                                                               "amqp://remotehost:5672",
                                                               #'P_basic'{headers = undefined}]),
    assert_amqp901_headers(ActualHeaders),
    ok.

assert_amqp901_headers(ActualHeaders) ->
    {_, array, [{table, Shovelled}]} = lists:keyfind(<<"x-shovelled">>, 1, ActualHeaders),
    {_, long, _} = lists:keyfind(<<"x-shovelled-timestamp">>, 1, ActualHeaders),

    ExpectedHeaders =
    [{<<"shovelled-by">>, "my-cluster"},
     {<<"shovel-type">>, <<"dynamic">>},
     {<<"shovel-name">>, "name"},
     {<<"shovel-vhost">>, "vhost"},
     {<<"src-uri">>,"amqp://localhost:5672"},
     {<<"dest-uri">>,"amqp://remotehost:5672"},
     {<<"src-queue">>,<<"a-src-queue">>},
     {<<"dest-queue">>,<<"a-dest-queue">>}],
    lists:foreach(fun({K, V}) ->
                          ?assertMatch({K, _, V},
                                       lists:keyfind(K, 1, Shovelled))
                  end, ExpectedHeaders),
    ok.

parse_amqp10(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_amqp10_0, []).
        
parse_amqp10_0() ->
    Params =
        [
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"reconnect-delay">>, 1001},

         {<<"src-protocol">>, <<"amqp10">>},
         {<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-address">>, <<"a-src-queue">>},
         {<<"src-delete-after">>, <<"never">>},
         {<<"src-prefetch-count">>, 30},

         {<<"dest-protocol">>, <<"amqp10">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-address">>, <<"a-dest-queue">>},
         {<<"dest-add-forward-headers">>, true},
         {<<"dest-add-timestamp-header">>, true},
         {<<"dest-application-properties">>, [{<<"some-app-prop">>,
                                               <<"app-prop-value">>}]},
         {<<"dest-message-annotations">>, [{<<"some-message-ann">>,
                                            <<"message-ann-value">>}]},
         {<<"dest-properties">>, [{<<"user_id">>, <<"some-user">>}]}
        ],
    ObfuscatedParams = rabbit_shovel_parameters:obfuscate_uris_in_definition(Params),
    ?assertMatch(
       {ok, #{name := "my_shovel",
              ack_mode := on_publish,
              source := #{module := rabbit_amqp10_shovel,
                          uris := ["amqp://localhost:5672"],
                          delete_after := never,
                          prefetch_count := 30,
                          source_address := <<"a-src-queue">>
                          },
              dest := #{module := rabbit_amqp10_shovel,
                        uris := ["amqp://remotehost:5672"],
                        target_address := <<"a-dest-queue">>,
                        message_annotations := #{<<"some-message-ann">> :=
                                                 <<"message-ann-value">>},
                        application_properties := #{<<"some-app-prop">> :=
                                                    <<"app-prop-value">>},
                        properties := #{user_id := <<"some-user">>},
                        add_timestamp_header := true,
                        add_forward_headers := true
                       }
             }},
        rabbit_shovel_parameters:parse({"vhost", "my_shovel"}, "my-cluster",
                                       ObfuscatedParams)),
    ok.

parse_amqp10_minimal(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_amqp10_minimal_0, []).

parse_amqp10_minimal_0() ->
    Params =
        [
         {<<"src-protocol">>, <<"amqp10">>},
         {<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-address">>, <<"a-src-queue">>},

         {<<"dest-protocol">>, <<"amqp10">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-address">>, <<"a-dest-queue">>}
        ],
    ObfuscatedParams = rabbit_shovel_parameters:obfuscate_uris_in_definition(Params),
    ?assertMatch(
       {ok, #{name := "my_shovel",
              ack_mode := on_confirm,
              source := #{module := rabbit_amqp10_shovel,
                          uris := ["amqp://localhost:5672"],
                          delete_after := never,
                          source_address := <<"a-src-queue">>
                          },
              dest := #{module := rabbit_amqp10_shovel,
                        uris := ["amqp://remotehost:5672"],
                        unacked := #{},
                        target_address := <<"a-dest-queue">>
                       }
             }},
        rabbit_shovel_parameters:parse({"vhost", "my_shovel"}, "my-cluster",
                                       ObfuscatedParams)),
    ok.

validate_amqp10(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, validate_amqp10_0, []).

validate_amqp10_0() ->
    Params =
        [
         {<<"ack-mode">>, <<"on-publish">>},
         {<<"reconnect-delay">>, 1001},

         {<<"src-protocol">>, <<"amqp10">>},
         {<<"src-uri">>, <<"amqp://localhost:5672">>},
         {<<"src-address">>, <<"a-src-queue">>},
         {<<"src-delete-after">>, <<"never">>},
         {<<"src-prefetch-count">>, 30},

         {<<"dest-protocol">>, <<"amqp10">>},
         {<<"dest-uri">>, <<"amqp://remotehost:5672">>},
         {<<"dest-address">>, <<"a-dest-queue">>},
         {<<"dest-add-forward-headers">>, true},
         {<<"dest-add-timestamp-header">>, true},
         {<<"dest-application-properties">>, [{<<"some-app-prop">>,
                                               <<"app-prop-value">>}]},
         {<<"dest-message-annotations">>, [{<<"some-message-ann">>,
                                               <<"message-ann-value">>}]},
         {<<"dest-properties">>, [{<<"user_id">>, <<"some-user">>}]}
        ],

        Res = rabbit_shovel_parameters:validate("my-vhost", <<"shovel">>,
                                                "my-shovel", Params, none),
        [] = validate_ok(Res),
        ok.

validate_amqp10_with_a_map(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, validate_amqp10_with_a_map_0, []).

validate_amqp10_with_a_map_0() ->
    Params =
        #{
         <<"ack-mode">> => <<"on-publish">>,
         <<"reconnect-delay">> => 1001,

         <<"src-protocol">> => <<"amqp10">>,
         <<"src-uri">> => <<"amqp://localhost:5672">>,
         <<"src-address">> => <<"a-src-queue">>,
         <<"src-delete-after">> => <<"never">>,
         <<"src-prefetch-count">> => 30,

         <<"dest-protocol">> => <<"amqp10">>,
         <<"dest-uri">> => <<"amqp://remotehost:5672">>,
         <<"dest-address">> => <<"a-dest-queue">>,
         <<"dest-add-forward-headers">> => true,
         <<"dest-add-timestamp-header">> => true,
         <<"dest-application-properties">> => [{<<"some-app-prop">>,
                                                <<"app-prop-value">>}],
         <<"dest-message-annotations">> => [{<<"some-message-ann">>, <<"message-ann-value">>}],
         <<"dest-properties">> => #{<<"user_id">> => <<"some-user">>}
        },

        Res = rabbit_shovel_parameters:validate("my-vhost", <<"shovel">>,
                                                "my-shovel", Params, none),
        [] = validate_ok(Res),
        ok.

validate_ok([ok | T]) ->
    validate_ok(T);
validate_ok([[_|_] = L | T]) ->
    validate_ok(L) ++ validate_ok(T);
validate_ok([]) -> [];
validate_ok(X) ->
    exit({not_ok, X}).

%% -------------------------------------------------------------------
%% Protocol detection tests
%% -------------------------------------------------------------------

test_src_protocol_defaults(_Config) ->
    DefProplist = [{<<"src-uri">>, <<"amqp://localhost">>},
                   {<<"dest-uri">>, <<"amqp://remote">>}],
    ?assertEqual(amqp091, rabbit_shovel_parameters:src_protocol(DefProplist)),

    DefMap = #{<<"src-uri">> => <<"amqp://localhost">>,
               <<"dest-uri">> => <<"amqp://remote">>},
    ?assertEqual(amqp091, rabbit_shovel_parameters:src_protocol(DefMap)),
    ok.

test_src_protocol_explicit(_Config) ->
    Def091 = [{<<"src-protocol">>, <<"amqp091">>}],
    ?assertEqual(amqp091, rabbit_shovel_parameters:src_protocol(Def091)),

    Def10 = [{<<"src-protocol">>, <<"amqp10">>}],
    ?assertEqual(amqp10, rabbit_shovel_parameters:src_protocol(Def10)),

    DefLocal = [{<<"src-protocol">>, <<"local">>}],
    ?assertEqual(local, rabbit_shovel_parameters:src_protocol(DefLocal)),

    DefMap091 = #{<<"src-protocol">> => <<"amqp091">>},
    ?assertEqual(amqp091, rabbit_shovel_parameters:src_protocol(DefMap091)),

    DefMap10 = #{<<"src-protocol">> => <<"amqp10">>},
    ?assertEqual(amqp10, rabbit_shovel_parameters:src_protocol(DefMap10)),

    DefMapLocal = #{<<"src-protocol">> => <<"local">>},
    ?assertEqual(local, rabbit_shovel_parameters:src_protocol(DefMapLocal)),
    ok.

test_dest_protocol_defaults(_Config) ->
    DefProplist = [{<<"src-uri">>, <<"amqp://localhost">>},
                   {<<"dest-uri">>, <<"amqp://remote">>}],
    ?assertEqual(amqp091, rabbit_shovel_parameters:dest_protocol(DefProplist)),

    DefMap = #{<<"src-uri">> => <<"amqp://localhost">>,
               <<"dest-uri">> => <<"amqp://remote">>},
    ?assertEqual(amqp091, rabbit_shovel_parameters:dest_protocol(DefMap)),
    ok.

test_dest_protocol_explicit(_Config) ->
    Def091 = [{<<"dest-protocol">>, <<"amqp091">>}],
    ?assertEqual(amqp091, rabbit_shovel_parameters:dest_protocol(Def091)),

    Def10 = [{<<"dest-protocol">>, <<"amqp10">>}],
    ?assertEqual(amqp10, rabbit_shovel_parameters:dest_protocol(Def10)),

    DefLocal = [{<<"dest-protocol">>, <<"local">>}],
    ?assertEqual(local, rabbit_shovel_parameters:dest_protocol(DefLocal)),

    DefMap091 = #{<<"dest-protocol">> => <<"amqp091">>},
    ?assertEqual(amqp091, rabbit_shovel_parameters:dest_protocol(DefMap091)),

    DefMap10 = #{<<"dest-protocol">> => <<"amqp10">>},
    ?assertEqual(amqp10, rabbit_shovel_parameters:dest_protocol(DefMap10)),

    DefMapLocal = #{<<"dest-protocol">> => <<"local">>},
    ?assertEqual(local, rabbit_shovel_parameters:dest_protocol(DefMapLocal)),
    ok.

test_protocols_defaults(_Config) ->
    DefProplist = [{<<"src-uri">>, <<"amqp://localhost">>},
                   {<<"dest-uri">>, <<"amqp://remote">>}],
    ?assertEqual({amqp091, amqp091}, rabbit_shovel_parameters:protocols(DefProplist)),

    DefMap = #{<<"src-uri">> => <<"amqp://localhost">>,
               <<"dest-uri">> => <<"amqp://remote">>},
    ?assertEqual({amqp091, amqp091}, rabbit_shovel_parameters:protocols(DefMap)),
    ok.

test_protocols_explicit(_Config) ->
    Def091 = [{<<"src-protocol">>, <<"amqp091">>},
              {<<"dest-protocol">>, <<"amqp091">>}],
    ?assertEqual({amqp091, amqp091}, rabbit_shovel_parameters:protocols(Def091)),

    Def10 = [{<<"src-protocol">>, <<"amqp10">>},
             {<<"dest-protocol">>, <<"amqp10">>}],
    ?assertEqual({amqp10, amqp10}, rabbit_shovel_parameters:protocols(Def10)),

    DefLocal = [{<<"src-protocol">>, <<"local">>},
                {<<"dest-protocol">>, <<"local">>}],
    ?assertEqual({local, local}, rabbit_shovel_parameters:protocols(DefLocal)),

    DefMap091 = #{<<"src-protocol">> => <<"amqp091">>,
                  <<"dest-protocol">> => <<"amqp091">>},
    ?assertEqual({amqp091, amqp091}, rabbit_shovel_parameters:protocols(DefMap091)),

    DefMap10 = #{<<"src-protocol">> => <<"amqp10">>,
                 <<"dest-protocol">> => <<"amqp10">>},
    ?assertEqual({amqp10, amqp10}, rabbit_shovel_parameters:protocols(DefMap10)),

    DefMapLocal = #{<<"src-protocol">> => <<"local">>,
                    <<"dest-protocol">> => <<"local">>},
    ?assertEqual({local, local}, rabbit_shovel_parameters:protocols(DefMapLocal)),
    ok.

test_protocols_mixed(_Config) ->
    Def091to10 = [{<<"src-protocol">>, <<"amqp091">>},
                  {<<"dest-protocol">>, <<"amqp10">>}],
    ?assertEqual({amqp091, amqp10}, rabbit_shovel_parameters:protocols(Def091to10)),

    Def10to091 = [{<<"src-protocol">>, <<"amqp10">>},
                  {<<"dest-protocol">>, <<"amqp091">>}],
    ?assertEqual({amqp10, amqp091}, rabbit_shovel_parameters:protocols(Def10to091)),

    DefLocalTo091 = [{<<"src-protocol">>, <<"local">>},
                     {<<"dest-protocol">>, <<"amqp091">>}],
    ?assertEqual({local, amqp091}, rabbit_shovel_parameters:protocols(DefLocalTo091)),

    Def091ToLocal = [{<<"src-protocol">>, <<"amqp091">>},
                     {<<"dest-protocol">>, <<"local">>}],
    ?assertEqual({amqp091, local}, rabbit_shovel_parameters:protocols(Def091ToLocal)),

    Def10ToLocal = [{<<"src-protocol">>, <<"amqp10">>},
                    {<<"dest-protocol">>, <<"local">>}],
    ?assertEqual({amqp10, local}, rabbit_shovel_parameters:protocols(Def10ToLocal)),

    DefLocalTo10 = [{<<"src-protocol">>, <<"local">>},
                    {<<"dest-protocol">>, <<"amqp10">>}],
    ?assertEqual({local, amqp10}, rabbit_shovel_parameters:protocols(DefLocalTo10)),

    DefMap091to10 = #{<<"src-protocol">> => <<"amqp091">>,
                      <<"dest-protocol">> => <<"amqp10">>},
    ?assertEqual({amqp091, amqp10}, rabbit_shovel_parameters:protocols(DefMap091to10)),

    DefMap10to091 = #{<<"src-protocol">> => <<"amqp10">>,
                      <<"dest-protocol">> => <<"amqp091">>},
    ?assertEqual({amqp10, amqp091}, rabbit_shovel_parameters:protocols(DefMap10to091)),

    DefMapLocalTo091 = #{<<"src-protocol">> => <<"local">>,
                         <<"dest-protocol">> => <<"amqp091">>},
    ?assertEqual({local, amqp091}, rabbit_shovel_parameters:protocols(DefMapLocalTo091)),

    DefMap091ToLocal = #{<<"src-protocol">> => <<"amqp091">>,
                         <<"dest-protocol">> => <<"local">>},
    ?assertEqual({amqp091, local}, rabbit_shovel_parameters:protocols(DefMap091ToLocal)),

    DefMap10ToLocal = #{<<"src-protocol">> => <<"amqp10">>,
                        <<"dest-protocol">> => <<"local">>},
    ?assertEqual({amqp10, local}, rabbit_shovel_parameters:protocols(DefMap10ToLocal)),

    DefMapLocalTo10 = #{<<"src-protocol">> => <<"local">>,
                        <<"dest-protocol">> => <<"amqp10">>},
    ?assertEqual({local, amqp10}, rabbit_shovel_parameters:protocols(DefMapLocalTo10)),
    ok.

test_protocols_with_maps(_Config) ->
    DefMap1 = #{<<"src-protocol">> => <<"amqp091">>,
                <<"dest-protocol">> => <<"amqp10">>},
    ?assertEqual({amqp091, amqp10}, rabbit_shovel_parameters:protocols(DefMap1)),

    DefMap2 = #{<<"src-protocol">> => <<"local">>,
                <<"dest-protocol">> => <<"local">>},
    ?assertEqual({local, local}, rabbit_shovel_parameters:protocols(DefMap2)),

    DefMap3 = #{<<"src-protocol">> => <<"amqp10">>},
    ?assertEqual({amqp10, amqp091}, rabbit_shovel_parameters:protocols(DefMap3)),

    DefMap4 = #{<<"dest-protocol">> => <<"local">>},
    ?assertEqual({amqp091, local}, rabbit_shovel_parameters:protocols(DefMap4)),
    ok.
