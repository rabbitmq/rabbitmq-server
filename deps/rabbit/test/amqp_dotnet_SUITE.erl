%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp_dotnet_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [{cluster_size_1, [],
      [
       roundtrip,
       roundtrip_to_amqp_091,
       default_outcome,
       no_routes_is_released,
       outcomes,
       fragmentation,
       message_annotations,
       footer,
       data_types,
       reject,
       redelivery,
       released,
       routing,
       invalid_routes,
       auth_failure,
       access_failure_not_allowed,
       access_failure_send,
       streams
      ]
     }].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_1, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodename_suffix, Suffix}),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                [fun build_dotnet_test_project/1] ++
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config2, 'rabbitmq_4.0.0'),
    Config2.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

build_dotnet_test_project(Config) ->
    TestProjectDir = filename:join([?config(data_dir, Config), "fsharp-tests"]),
    case rabbit_ct_helpers:exec(["dotnet", "restore"],
                                [{cd, TestProjectDir}]) of
        {ok, _} ->
            rabbit_ct_helpers:set_config(
              Config, {dotnet_test_project_dir, TestProjectDir});
        Other ->
            ct:fail({"'dotnet restore' failed", Other})
    end.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

roundtrip(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "quorum"),
    run(?FUNCTION_NAME, Config).

roundtrip_to_amqp_091(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

default_outcome(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

no_routes_is_released(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch, #'exchange.declare'{exchange = <<"no_routes_is_released">>,
                                              durable = true}),
    run(?FUNCTION_NAME, Config).

outcomes(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

fragmentation(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

message_annotations(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

footer(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

data_types(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

reject(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "classic"),
    run(?FUNCTION_NAME, Config).

redelivery(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "quorum"),
    run(?FUNCTION_NAME, Config).

released(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "quorum"),
    run(?FUNCTION_NAME, Config).

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
    run(?FUNCTION_NAME, Config).

invalid_routes(Config) ->
    run(?FUNCTION_NAME, Config).

auth_failure(Config) ->
    run(?FUNCTION_NAME, Config).

access_failure_not_allowed(Config) ->
    User = atom_to_binary(?FUNCTION_NAME),
    ok = rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    run(?FUNCTION_NAME, Config),
    ok = rabbit_ct_broker_helpers:delete_user(Config, User).

access_failure_send(Config) ->
    User = atom_to_binary(?FUNCTION_NAME),
    ok = rabbit_ct_broker_helpers:add_user(Config, User, <<"boo">>),
    ok = rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>,
                                                  <<".*">>, %% configure
                                                  <<"^banana.*">>, %% write
                                                  <<"^banana.*">>  %% read
                                                 ),
    run(?FUNCTION_NAME, Config),
    ok = rabbit_ct_broker_helpers:delete_user(Config, User).

streams(Config) ->
    declare_queue(Config, ?FUNCTION_NAME, "stream"),
    run(?FUNCTION_NAME, Config).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

declare_queue(Config, Name, Type) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} =
    amqp_channel:call(Ch, #'queue.declare'{queue = atom_to_binary(Name, utf8),
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>,
                                                         longstr, Type}]}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

run(TestNameAtom, Config) ->
    TestName = atom_to_list(TestNameAtom),
    TestProjectDir = ?config(dotnet_test_project_dir, Config),
    Uri = rabbit_ct_broker_helpers:node_uri(Config, 0, [{use_ipaddr, true}]),
    case rabbit_ct_helpers:exec(["dotnet", "run", "--", TestName, Uri],
                                [{cd, TestProjectDir}]) of
        {ok, _Stdout_} ->
            ok;
        {error, _ExitCode, _Stdout} ->
            ct:fail(TestName)
    end.
