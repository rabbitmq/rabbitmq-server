%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp_jms_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/3]).
-import(amqp_utils,
        [init/1,
         close/1,
         connection_config/1,
         detach_link_sync/1,
         end_session_sync/1,
         close_connection_sync/1]).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [{cluster_size_1, [shuffle],
      [
       %% CT test case per Java class
       jms_connection,
       jms_temporary_queue,

       %% CT test case per test in Java class JmsTest
       message_types_jms_to_jms,
       message_types_jms_to_amqp,
       temporary_queue_rpc,
       temporary_queue_delete,
       message_selector_application_properties,
       message_selector_header_fields
      ]
     }].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

suite() ->
    [
     {timetrap, {minutes, 2}}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_1, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                {rmq_nodename_suffix, Suffix}),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit,
                 [{permit_deprecated_features,
                   %% We want to test JMS solely with AMQP address v2
                   %% since that will be the only option in the future.
                   #{amqp_address_v1 => false}
                  }]
                }),
    Config3 = rabbit_ct_helpers:run_setup_steps(
                Config2,
                [fun build_maven_test_project/1] ++
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config3, 'rabbitmq_4.0.0'),
    Config3.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Assert that every testcase cleaned up.
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, [])), 1000, 5),
    eventually(?_assertEqual([], rpc(Config, rabbit_amqp_session, list_local, [])), 1000, 5),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

build_maven_test_project(Config) ->
    TestProjectDir = ?config(data_dir, Config),
    case rabbit_ct_helpers:exec([filename:join([TestProjectDir, "mvnw"]), "test-compile"],
                                [{cd, TestProjectDir}]) of
        {ok, _} ->
            Config;
        Other ->
            ct:fail({"'mvnw test-compile' failed", Other})
    end.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

jms_connection(Config) ->
    ok = run(?FUNCTION_NAME, [{"-Dtest=~s", [<<"JmsConnectionTest">>]}], Config).

jms_temporary_queue(Config) ->
    ok = run(?FUNCTION_NAME, [{"-Dtest=~s", [<<"JmsTemporaryQueueTest">>]}], Config).

%% Send different message types from JMS client to JMS client.
message_types_jms_to_jms(Config) ->
    ok = run_jms_test(?FUNCTION_NAME, Config).

%% Send different message types from JMS client to Erlang AMQP 1.0 client.
message_types_jms_to_amqp(Config) ->
    ok = run_jms_test(?FUNCTION_NAME, Config).

temporary_queue_rpc(Config) ->
    ok = run_jms_test(?FUNCTION_NAME, Config).

temporary_queue_delete(Config) ->
    ok = run_jms_test(?FUNCTION_NAME, Config).

message_selector_application_properties(Config) ->
    ok = run_jms_test(?FUNCTION_NAME, Config).

message_selector_header_fields(Config) ->
    ok = run_jms_test(?FUNCTION_NAME, Config).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

run_jms_test(TestName, Config) ->
    run(TestName, [{"-Dtest=JmsTest#~ts", [TestName]}], Config).

run(TestName, JavaProps, Config) ->
    TestProjectDir = ?config(data_dir, Config),

    Cmd = [filename:join([TestProjectDir, "mvnw"]),
           "test",
           {"-Drmq_broker_uri=~ts", [rabbit_ct_broker_helpers:node_uri(Config, 0)]},
           {"-Dnodename=~ts", [rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)]},
           {"-Drabbitmqctl.bin=~ts", [rabbit_ct_helpers:get_config(Config, rabbitmqctl_cmd)]}
          ] ++ JavaProps,
    case rabbit_ct_helpers:exec(Cmd, [{cd, TestProjectDir}]) of
        {ok, _Stdout_} ->
            ok;
        {error, _ExitCode, _Stdout} ->
            ct:fail(TestName)
    end.

declare_queue(Name, Type, Config) ->
    {_, _, LinkPair} = Init = init(Config),
    {ok, #{type := Type}} = rabbitmq_amqp_client:declare_queue(
                              LinkPair, Name,
                              #{arguments => #{<<"x-queue-type">> => {utf8, Type}}}),
    ok = close(Init).

delete_queue(Name, Config) ->
    {_, _, LinkPair} = Init = init(Config),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Name),
    ok = close(Init).
