%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(local_dynamic_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(shovel_test_utils, [with_amqp10_session/2, with_amqp10_session/3,
                            amqp10_expect_empty/2,
                            amqp10_publish/4, amqp10_expect_one/2,
                            amqp10_expect_count/3, amqp10_expect/3,
                            amqp10_publish_expect/5, amqp10_subscribe/2,
                            await_autodelete/2]).

-define(PARAM, <<"test">>).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  local_to_local_opt_headers,
                  local_to_local_stream_no_ack,
                  local_to_local_delete_dest_queue,
                  local_to_local_stream_credit_flow_no_ack,
                  local_to_local_simple_uri,
                  local_to_local_counters,
                  local_to_local_alarms
                 ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
        {rmq_nodename_suffix, ?MODULE},
      {ignored_crashes, [
          "server_initiated_close,404",
          "writer,send_failed,closed",
          "source_queue_down",
          "dest_queue_down"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    [Node] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    case rabbit_ct_broker_helpers:enable_feature_flag(
           Config, [Node], 'rabbitmq_4.0.0') of
        ok ->
            Config;
        _ ->
            {skip, "This suite requires rabbitmq_4.0.0 feature flag"}
    end.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    SrcQ = list_to_binary(atom_to_list(Testcase) ++ "_src"),
    DestQ = list_to_binary(atom_to_list(Testcase) ++ "_dest"),
    DestQ2 = list_to_binary(atom_to_list(Testcase) ++ "_dest2"),
    VHost = list_to_binary(atom_to_list(Testcase) ++ "_vhost"),
    Config = [{srcq, SrcQ}, {destq, DestQ}, {destq2, DestQ2},
              {alt_vhost, VHost} | Config0],

    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    shovel_test_utils:clear_param(Config, ?PARAM),
    rabbit_ct_broker_helpers:rpc(Config, 0, shovel_test_utils, delete_all_queues, []),
    _ = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(alt_vhost, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

local_to_local_opt_headers(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-add-forward-headers">>, true},
                                           {<<"dest-add-timestamp-header">>, true}
                                          ]),
              [Msg] = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ?assertMatch(#{<<"x-opt-shovel-name">> := ?PARAM,
                             <<"x-opt-shovel-type">> := <<"dynamic">>,
                             <<"x-opt-shovelled-by">> := _,
                             <<"x-opt-shovelled-timestamp">> := _},
                           amqp10_msg:message_annotations(Msg))
      end).

local_to_local_stream_no_ack(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, <<"/">>, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, <<"/">>, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-predeclared">>, true},
                                           {<<"dest-queue">>, Dest},
                                           {<<"ack-mode">>, <<"no-ack">>}
                                          ]),
              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, 10),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 10}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 10, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_delete_dest_queue(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              _ = amqp10_publish_expect(Sess, Src, Dest, <<"hello">>, 1),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queue,
                                           [Dest, <<"/">>]),
              ?awaitMatch([{_Name, dynamic, {terminated, dest_queue_down}, _, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000)
      end).

local_to_local_stream_credit_flow_no_ack(Config) ->
    local_to_local_stream_credit_flow(Config, <<"no-ack">>).

local_to_local_stream_credit_flow(Config, AckMode) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    VHost = <<"/">>,
    declare_queue(Config, VHost, Src, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    declare_queue(Config, VHost, Dest, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    with_amqp10_session(Config,
      fun (Sess) ->
             shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"src-predeclared">>, true},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest},
                                           {<<"dest-predeclared">>, true},
                                           {<<"ack-mode">>, AckMode}
                                          ]),

              Receiver = amqp10_subscribe(Sess, Dest),
              amqp10_publish(Sess, Src, <<"tag1">>, 1000),
              ?awaitMatch([{_Name, dynamic, {running, _}, #{forwarded := 1000}, _}],
                          rabbit_ct_broker_helpers:rpc(Config, 0,
                                                       rabbit_shovel_status, status, []),
                          30000),
              _ = amqp10_expect(Receiver, 1000, []),
              amqp10_client:detach_link(Receiver)
      end).

local_to_local_simple_uri(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Uri = <<"amqp://">>,
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set,
           [<<"/">>, <<"shovel">>, ?PARAM, [{<<"src-uri">>,  Uri},
                                            {<<"dest-uri">>, [Uri]},
                                            {<<"src-protocol">>, <<"local">>},
                                            {<<"src-queue">>, Src},
                                            {<<"dest-protocol">>, <<"local">>},
                                            {<<"dest-queue">>, Dest}],
            none]),
    shovel_test_utils:await_shovel(Config, ?PARAM).

local_to_local_counters(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    %% Let's restart the node so the counters are reset
    ok = rabbit_ct_broker_helpers:stop_node(Config, 0),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              ?awaitMatch(#{publishers := 0, consumers := 0},
                          get_global_counters(Config), 30_000),
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-queue">>, Dest}
                                          ]),
              ?awaitMatch(#{publishers := 1, consumers := 1},
                          get_global_counters(Config), 30_000),
              _ = amqp10_publish(Sess, Src, <<"tag1">>, 150),
              ?awaitMatch(#{consumers := 1, publishers := 1,
                            messages_received_total := 150,
                            messages_received_confirm_total := 150,
                            messages_routed_total := 150,
                            messages_unroutable_dropped_total := 0,
                            messages_unroutable_returned_total := 0,
                            messages_confirmed_total := 150},
                          get_global_counters(Config), 30_000)
      end).

local_to_local_alarms(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    ShovelArgs = [{<<"src-protocol">>, <<"local">>},
                  {<<"src-queue">>, Src},
                  {<<"dest-protocol">>, <<"local">>},
                  {<<"dest-queue">>, Dest}],
    with_amqp10_session(
      Config,
      fun (Sess) ->
              amqp10_publish(Sess, Src, <<"hello">>, 1000),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              shovel_test_utils:set_param(Config, ?PARAM, ShovelArgs),
              ?awaitMatch({running, blocked}, get_blocked_status(Config), 30000),
              amqp10_expect_empty(Sess, Dest),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, disk),
              ?awaitMatch({running, running}, get_blocked_status(Config), 30000),
              amqp10_expect_count(Sess, Dest, 1000),

              shovel_test_utils:clear_param(Config, ?PARAM),

              amqp10_publish(Sess, Src, <<"hello">>, 1000),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, disk),
              rabbit_ct_broker_helpers:set_alarm(Config, 0, memory),
              shovel_test_utils:set_param(Config, ?PARAM, ShovelArgs),
              ?awaitMatch({running, blocked}, get_blocked_status(Config), 30000),
              amqp10_expect_empty(Sess, Dest),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, disk),
              ?awaitMatch({running, blocked}, get_blocked_status(Config), 30000),
              amqp10_expect_empty(Sess, Dest),
              rabbit_ct_broker_helpers:clear_alarm(Config, 0, memory),
              ?awaitMatch({running, running}, get_blocked_status(Config), 30000),
              amqp10_expect_count(Sess, Dest, 1000)
      end).
%%----------------------------------------------------------------------------
declare_queue(Config, VHost, QName) ->
    declare_queue(Config, VHost, QName, []).

declare_queue(Config, VHost, QName, Args) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true, arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

declare_and_bind_queue(Config, VHost, Exchange, QName, RoutingKey) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true,
                              arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]})),
    ?assertMatch(
       #'queue.bind_ok'{},
       amqp_channel:call(Ch, #'queue.bind'{
                                queue = QName,
                                exchange = Exchange,
                                routing_key = RoutingKey
                               })),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

declare_exchange(Config, VHost, Exchange) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertMatch(
       #'exchange.declare_ok'{},
       amqp_channel:call(Ch, #'exchange.declare'{exchange = Exchange})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

delete_queue(Name, VHost) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

get_global_counters(Config) ->
    get_global_counters0(Config, #{protocol => 'local-shovel'}).

get_global_counters0(Config, Key) ->
    Overview = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_global_counters, overview, []),
    maps:get(Key, Overview).

get_blocked_status(Config) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status, status, []) of
        [{_, _, {Status, PropList}, _, _}] ->
            {Status, proplists:get_value(blocked_status, PropList)};
        _ ->
            empty
    end.
