%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_inter_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_broker_helpers, [rpc/5]).
-import(shovel_test_utils, [await_credit/1]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle],
      [
       old_to_new_on_old,
       old_to_new_on_new,
       new_to_old_on_old,
       new_to_old_on_new
      ]}
    ].

%% In mixed version tests:
%% * node 0 is the new version single node cluster
%% * node 1 is the old version single node cluster
-define(NEW, 0).
-define(OLD, 1).

init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{rmq_nodename_suffix, ?MODULE},
                 {rmq_nodes_count, 2},
                 {rmq_nodes_clustered, false}]),
    Config = rabbit_ct_helpers:run_setup_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    %% If node 1 runs 4.x, this is the new no-op plugin.
    %% If node 1 runs 3.x, this is the old real plugin.
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, ?OLD, rabbitmq_amqp1_0),
    Config.

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
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

old_to_new_on_old(Config) ->
    ok = shovel(?FUNCTION_NAME, ?OLD, ?NEW, ?OLD, Config).

old_to_new_on_new(Config) ->
    ok = shovel(?FUNCTION_NAME, ?OLD, ?NEW, ?NEW, Config).

new_to_old_on_old(Config) ->
    ok = shovel(?FUNCTION_NAME, ?NEW, ?OLD, ?OLD, Config).

new_to_old_on_new(Config) ->
    ok = shovel(?FUNCTION_NAME, ?NEW, ?OLD, ?NEW, Config).

shovel(Caller, SrcNode, DestNode, ShovelNode, Config) ->
    SrcUri = shovel_test_utils:make_uri(Config, SrcNode),
    DestUri = shovel_test_utils:make_uri(Config, DestNode),
    ShovelName = atom_to_binary(Caller),
    SrcQ = <<ShovelName/binary, " source">>,
    DestQ = <<ShovelName/binary, " destination">>,
    Definition = [
                  {<<"src-uri">>,  SrcUri},
                  {<<"src-protocol">>, <<"amqp10">>},
                  {<<"src-address">>, SrcQ},
                  {<<"dest-uri">>, [DestUri]},
                  {<<"dest-protocol">>, <<"amqp10">>},
                  {<<"dest-address">>, DestQ}
                 ],
    ok = rpc(Config, ShovelNode, rabbit_runtime_parameters, set,
             [<<"/">>, <<"shovel">>, ShovelName, Definition, none]),
    ok = shovel_test_utils:await_shovel(Config, ShovelNode, ShovelName),

    Hostname = ?config(rmq_hostname, Config),
    SrcPort = rabbit_ct_broker_helpers:get_node_config(Config, SrcNode, tcp_port_amqp),
    DestPort = rabbit_ct_broker_helpers:get_node_config(Config, DestNode, tcp_port_amqp),
    {ok, SrcConn} = amqp10_client:open_connection(Hostname, SrcPort),
    {ok, DestConn} = amqp10_client:open_connection(Hostname, DestPort),
    {ok, SrcSess} = amqp10_client:begin_session_sync(SrcConn),
    {ok, DestSess} = amqp10_client:begin_session_sync(DestConn),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     SrcSess, <<"my sender">>, <<"/amq/queue/", SrcQ/binary>>, settled),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       DestSess, <<"my receiver">>, <<"/amq/queue/", DestQ/binary>>, settled),

    ok = await_credit(Sender),
    NumMsgs = 20,
    lists:map(
      fun(N) ->
              Bin = integer_to_binary(N),
              Msg = amqp10_msg:new(Bin, Bin, true),
              ok = amqp10_client:send_msg(Sender, Msg)
      end, lists:seq(1, NumMsgs)),
    ok = amqp10_client:close_connection(SrcConn),

    ok = amqp10_client:flow_link_credit(Receiver, NumMsgs, never),
    Msgs = receive_messages(Receiver, NumMsgs),
    ct:pal("~b messages:~n~p", [length(Msgs), Msgs]),
    lists:map(
      fun(N) ->
              Msg = lists:nth(N, Msgs),
              ?assertEqual(integer_to_binary(N),
                           amqp10_msg:body_bin(Msg))
      end, lists:seq(1, NumMsgs)),
    ok = amqp10_client:close_connection(DestConn),

    ok = rpc(Config, ShovelNode, rabbit_runtime_parameters, clear,
             [<<"/">>, <<"shovel">>, ShovelName, none]),
    ExpectedQueueLen = 0,
    ?awaitMatch(
       [{_, ExpectedQueueLen}],
       begin
           Ret = rpc(Config, ?OLD, ?MODULE, queues_length, []),
           ct:pal("Queues on old: ~p", [Ret]),
           Ret
       end,
       30000),
    ?awaitMatch(
       [{_, ExpectedQueueLen}],
       begin
           Ret = rpc(Config, ?NEW, ?MODULE, queues_length, []),
           ct:pal("Queues on new: ~p", [Ret]),
           Ret
       end,
       30000),
    ?assertEqual(
       [ExpectedQueueLen],
       rpc(Config, ?OLD, ?MODULE, delete_queues, [])),
    ?assertEqual(
       [ExpectedQueueLen],
       rpc(Config, ?NEW, ?MODULE, delete_queues, [])).

receive_messages(Receiver, N) ->
    receive_messages0(Receiver, N, []).

receive_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} ->
            receive_messages0(Receiver, N - 1, [Msg | Acc])
    after 5000  ->
              ct:fail({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~p flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

queues_length() ->
    [begin
         #{<<"name">> := Name} = amqqueue:to_printable(Q),
         [{messages, N}] = rabbit_amqqueue:info(Q, [messages]),
         {Name, N}
     end || Q <- rabbit_amqqueue:list()].

delete_queues() ->
    [begin
         {ok, N} = rabbit_amqqueue:delete(Q, false, false, <<"tests">>),
         N
     end || Q <- rabbit_amqqueue:list()].
