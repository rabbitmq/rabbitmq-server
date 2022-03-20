%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

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
          outcomes,
          fragmentation,
          message_annotations,
          footer,
          data_types,
          %% TODO at_most_once,
          reject,
          redelivery,
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
        ]},
      {streams, [
                 streams
                ], [
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

init_per_group(streams, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false ->
            Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Suffix},
                {amqp10_client_library, dotnet}
              ]),
            rabbit_ct_helpers:run_setup_steps(Config1, [
                fun build_dotnet_test_project/1
            ] ++
            rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps());
        _     ->
            {skip, "stream tests are skipped in mixed mode"}
    end;
init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {amqp10_client_library, Group}
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
            rabbit_ct_helpers:set_config(Config,
              {dotnet_test_project_dir, TestProjectDir});
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
    run(Config, [
        {dotnet, "roundtrip"},
        {java, "RoundTripTest"}
      ]).

streams(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"stream_q">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, "stream"}]}),
    run(Config, [
        {dotnet, "streams"}
      ]).

roundtrip_to_amqp_091(Config) ->
    run(Config, [
        {dotnet, "roundtrip_to_amqp_091"}
      ]).

default_outcome(Config) ->
    run(Config, [
        {dotnet, "default_outcome"}
      ]).

outcomes(Config) ->
    run(Config, [
        {dotnet, "outcomes"}
      ]).

fragmentation(Config) ->
    run(Config, [
        {dotnet, "fragmentation"}
      ]).

message_annotations(Config) ->
    run(Config, [
        {dotnet, "message_annotations"}
      ]).

footer(Config) ->
    run(Config, [
        {dotnet, "footer"}
      ]).

data_types(Config) ->
    run(Config, [
        {dotnet, "data_types"}
      ]).

reject(Config) ->
    run(Config, [
        {dotnet, "reject"}
      ]).

redelivery(Config) ->
    run(Config, [
        {dotnet, "redelivery"}
      ]).

routing(Config) ->

    StreamQT =
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, stream_queue) of
        ok ->
            <<"stream">>;
        _ ->
            %% if the feature flag could not be enabled we run the stream
            %% routing test using a classc quue instead
            ct:pal("stream feature flag could not be enabled"
                   "running stream tests against classic"),
            <<"classic">>
    end,
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"transient_q">>,
                                           durable = false}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"durable_q">>,
                                           durable = true}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"quorum_q">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"stream_q">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, StreamQT}]}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"stream_q2">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, StreamQT}]}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"autodel_q">>,
                                           auto_delete = true}),
    run(Config, [
        {dotnet, "routing"}
      ]).

%% TODO: this tests doesn't test anything that the standard routing test
%% already does. We should test stream specific things here like attaching
%% to a given offset
stream_interop_basics(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue   = <<"stream_q">>,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),
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
    User = <<"access_failure">>,
    rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>,
                                             <<".*">>, %% configure
                                             <<"^banana.*">>, %% write
                                             <<"^banana.*">>  %% read
                                            ),
    run(Config, [ {dotnet, "access_failure"} ]).

access_failure_not_allowed(Config) ->
    User = <<"access_failure_not_allowed">>,
    rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    run(Config, [ {dotnet, "access_failure_not_allowed"} ]).

access_failure_send(Config) ->
    User = <<"access_failure_send">>,
    rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>,
                                             <<".*">>, %% configure
                                             <<"^banana.*">>, %% write
                                             <<"^banana.*">>  %% read
                                            ),
    run(Config, [ {dotnet, "access_failure_send"} ]).

run(Config, Flavors) ->
    ClientLibrary = ?config(amqp10_client_library, Config),
    Fun = case ClientLibrary of
        dotnet -> fun run_dotnet_test/2;
        java   -> fun run_java_test/2
    end,
    case proplists:get_value(ClientLibrary, Flavors) of
        false    -> ok;
        TestName -> Fun(Config, TestName)
    end.

run_dotnet_test(Config, Method) ->
    TestProjectDir = ?config(dotnet_test_project_dir, Config),
    Uri = rabbit_ct_broker_helpers:node_uri(Config, 0),
    Ret = rabbit_ct_helpers:exec(["dotnet", "run", "--", Method, Uri ],
      [
        {cd, TestProjectDir}
      ]),
    {ok, _} = Ret.

run_java_test(Config, Class) ->
    TestProjectDir = ?config(maven_test_project_dir, Config),
    Ret = rabbit_ct_helpers:exec([
        TestProjectDir ++ "/mvnw",
        "test",
        {"-Dtest=~s", [Class]},
        {"-Drmq_broker_uri=~s", [rabbit_ct_broker_helpers:node_uri(Config, 0)]}
      ],
      [{cd, TestProjectDir}]),
    {ok, _} = Ret.
