%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp_system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
      {group, dotnet},
      {group, java}
    ].

groups() ->
    [
      {dotnet, [], [
          roundtrip,
          roundtrip_to_amqp_091,
          default_outcome,
          no_routes_is_released,
          outcomes,
          fragmentation,
          message_annotations,
          footer,
          data_types,
          %% TODO at_most_once,
          reject,
          redelivery,
          released,
          routing,
          invalid_routes,
          auth_failure,
          access_failure,
          access_failure_not_allowed,
          access_failure_send,
          streams
        ]},
      {java, [], [
          roundtrip
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {amqp_client_library, Group}
      ]),
    GroupSetupStep = case Group of
        dotnet -> fun build_dotnet_test_project/1;
        java   -> fun build_maven_test_project/1
    end,
    rabbit_ct_helpers:run_setup_steps(Config1, [
        GroupSetupStep
      ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    enable_feature_flags(Config,
                         [
                          message_containers_store_amqp_v1,
                          credit_api_v2,
                          quorum_queues_v4
                          % amqp_address_v1
                         ]),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

build_dotnet_test_project(Config) ->
    TestProjectDir = filename:join(
                       [?config(data_dir, Config), "fsharp-tests"]),
    Ret = rabbit_ct_helpers:exec(["dotnet", "restore"],
                                 [{cd, TestProjectDir}]),
    case Ret of
        {ok, _} ->
            rabbit_ct_helpers:set_config(
              Config, {dotnet_test_project_dir, TestProjectDir});
        _ ->
            {skip, "Failed to fetch .NET Core test project dependencies"}
    end.

build_maven_test_project(Config) ->
    TestProjectDir = filename:join([?config(data_dir, Config), "java-tests"]),
    Ret = rabbit_ct_helpers:exec([TestProjectDir ++ "/mvnw", "test-compile"],
      [{cd, TestProjectDir}]),
    case Ret of
        {ok, _} ->
            rabbit_ct_helpers:set_config(Config,
              {maven_test_project_dir, TestProjectDir});
        _ ->
            {skip, "Failed to build Maven test project"}
    end.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

roundtrip(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "quorum"),
    run(Config, [{dotnet, "roundtrip"},
                 {java, "RoundTripTest"}]).

streams(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "stream"),
    run(Config, [{dotnet, "streams"}]).

roundtrip_to_amqp_091(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "roundtrip_to_amqp_091"}]).

default_outcome(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "default_outcome"}]).

no_routes_is_released(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch, #'exchange.declare'{exchange = <<"no_routes_is_released">>,
                                              durable = true}),
    run(Config, [{dotnet, "no_routes_is_released"}]).

outcomes(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "outcomes"}]).

fragmentation(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "fragmentation"}]).

message_annotations(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "message_annotations"}]).

footer(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "footer"}]).

data_types(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "data_types"}]).

reject(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(Config, [{dotnet, "reject"}]).

redelivery(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "quorum"),
    run(Config, [{dotnet, "redelivery"}]).

released(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "quorum"),
    run(Config, [{dotnet, "released"}]).

routing(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"test">>,
                                           durable = true}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"transient_q">>,
                                           durable = false}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"durable_q">>,
                                           durable = true}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"quorum_q">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"stream_q">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"autodel_q">>,
                                           auto_delete = true}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"fanout_q">>,
                                           durable = false}),
    amqp_channel:call(Ch, #'queue.bind'{queue = <<"fanout_q">>,
                                        exchange = <<"amq.fanout">>
                                       }),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"direct_q">>,
                                           durable = false}),
    amqp_channel:call(Ch, #'queue.bind'{queue = <<"direct_q">>,
                                        exchange = <<"amq.direct">>,
                                        routing_key = <<"direct_q">>
                                       }),

    run(Config, [
        {dotnet, "routing"}
      ]).

invalid_routes(Config) ->
    run(Config, [
        {dotnet, "invalid_routes"}
      ]).

auth_failure(Config) ->
    run(Config, [ {dotnet, "auth_failure"} ]).

access_failure(Config) ->
    User = atom_to_binary(?FUNCTION_NAME),
    rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>,
                                             <<".*">>, %% configure
                                             <<"^banana.*">>, %% write
                                             <<"^banana.*">>  %% read
                                            ),
    run(Config, [ {dotnet, "access_failure"} ]).

access_failure_not_allowed(Config) ->
    User = atom_to_binary(?FUNCTION_NAME),
    rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    run(Config, [ {dotnet, "access_failure_not_allowed"} ]).

access_failure_send(Config) ->
    User = atom_to_binary(?FUNCTION_NAME),
    rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>,
                                             <<".*">>, %% configure
                                             <<"^banana.*">>, %% write
                                             <<"^banana.*">>  %% read
                                            ),
    run(Config, [ {dotnet, "access_failure_send"} ]).

run(Config, Flavors) ->
    ClientLibrary = ?config(amqp_client_library, Config),
    Fun = case ClientLibrary of
              dotnet -> fun run_dotnet_test/2;
              java   -> fun run_java_test/2
          end,
    {ClientLibrary, TestName} = proplists:lookup(ClientLibrary, Flavors),
    Fun(Config, TestName).

run_dotnet_test(Config, Method) ->
    TestProjectDir = ?config(dotnet_test_project_dir, Config),
    Uri = rabbit_ct_broker_helpers:node_uri(Config, 0, [{use_ipaddr, true}]),
    Ret = rabbit_ct_helpers:exec(["dotnet", "run", "--", Method, Uri ],
      [
        {cd, TestProjectDir}
      ]),
    ct:pal("~s: result ~p", [?FUNCTION_NAME, Ret]),
    {ok, _} = Ret.

run_java_test(Config, Class) ->
    TestProjectDir = ?config(maven_test_project_dir, Config),
    Ret = rabbit_ct_helpers:exec([
        TestProjectDir ++ "/mvnw",
        "test",
        {"-Dtest=~ts", [Class]},
        {"-Drmq_broker_uri=~ts", [rabbit_ct_broker_helpers:node_uri(Config, 0)]}
      ],
      [{cd, TestProjectDir}]),
    {ok, _} = Ret.


enable_feature_flags(Config, Flags) ->
    [begin
         case rabbit_ct_broker_helpers:enable_feature_flag(Config, Flag) of
             ok -> ok;
             _ ->
                 throw({skip, "feature flag ~s could not be enabled"})
         end
     end || Flag <- Flags].

declare_queue(Config, Name, Type) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue = atom_to_binary(Name, utf8),
                                               durable = true,
                                               arguments = [{<<"x-queue-type">>,
                                                             longstr, Type}]}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.
