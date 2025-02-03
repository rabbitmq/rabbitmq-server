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
       message_types_jms_to_jms,
       message_types_jms_to_amqp
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
    Config1 = rabbit_ct_helpers:set_config(Config, {rmq_nodename_suffix, Suffix}),
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

%% Send different message types from JMS client to JMS client.
message_types_jms_to_jms(Config) ->
    TestName = QName = atom_to_binary(?FUNCTION_NAME),
    ok = declare_queue(QName, <<"quorum">>, Config),
    ok = run(TestName, [{"-Dqueue=~ts", [rabbitmq_amqp_address:queue(QName)]}], Config),
    ok = delete_queue(QName, Config).

%% Send different message types from JMS client to Erlang AMQP 1.0 client.
message_types_jms_to_amqp(Config) ->
    TestName = QName = atom_to_binary(?FUNCTION_NAME),
    ok = declare_queue(QName, <<"quorum">>, Config),
    Address = rabbitmq_amqp_address:queue(QName),

    %% The JMS client sends messaegs.
    ok = run(TestName, [{"-Dqueue=~ts", [Address]}], Config),

    %% The Erlang AMQP 1.0 client receives messages.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, settled),
    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    ?assertEqual(
       #'v1_0.amqp_value'{content = {utf8, <<"msg1🥕"/utf8>>}},
       amqp10_msg:body(Msg1)),
    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    ?assertEqual(
       #'v1_0.amqp_value'{
          content = {map, [
                           {{utf8, <<"key1">>}, {utf8, <<"value">>}},
                           {{utf8, <<"key2">>}, true},
                           {{utf8, <<"key3">>}, {double, -1.1}},
                           {{utf8, <<"key4">>}, {long, -1}}
                          ]}},
       amqp10_msg:body(Msg2)),
    {ok, Msg3} = amqp10_client:get_msg(Receiver),
    ?assertEqual(
       [
        #'v1_0.amqp_sequence'{
           content = [{utf8, <<"value">>},
                      true,
                      {double, -1.1},
                      {long, -1}]}
       ],
       amqp10_msg:body(Msg3)),

    ok = detach_link_sync(Receiver),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    ok = delete_queue(QName, Config).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

run(TestName, JavaProps, Config) ->
    TestProjectDir = ?config(data_dir, Config),
    Cmd = [filename:join([TestProjectDir, "mvnw"]),
           "test",
           {"-Dtest=JmsTest#~ts", [TestName]},
           {"-Drmq_broker_uri=~ts", [rabbit_ct_broker_helpers:node_uri(Config, 0)]}
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
