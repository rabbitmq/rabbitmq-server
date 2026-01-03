%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp091_alarm_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, network_connection},
     {group, direct_connection}
    ].

groups() ->
    [
     {network_connection, [], [
                               dest_resource_alarm_on_confirm,
                               dest_resource_alarm_on_publish,
                               dest_resource_alarm_no_ack
                              ]},
     {direct_connection, [], [
                              dest_resource_alarm_on_confirm
                             ]}
    ].

all_tests() ->
    [
     dest_resource_alarm_on_confirm,
     dest_resource_alarm_on_publish,
     dest_resource_alarm_no_ack
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 2},
        {rmq_nodes_clustered, false}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(network_connection, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{shovel_source_uri, shovel_test_utils:make_uri(Config, 1)},
       {shovel_source_idx, 1},
       {shovel_dest_uri, shovel_test_utils:make_uri(Config, 0)},
       {shovel_dest_idx, 0}
      ]);
init_per_group(direct_connection, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{shovel_source_uri, shovel_test_utils:make_uri(Config, 1)},
       {shovel_source_idx, 1},
       {shovel_dest_uri, <<"amqp://">>},
       {shovel_dest_idx, 0}
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

dest_resource_alarm_on_confirm(Config) ->
    dest_resource_alarm(<<"on-confirm">>, Config).

dest_resource_alarm_on_publish(Config) ->
    dest_resource_alarm(<<"on-publish">>, Config).

dest_resource_alarm_no_ack(Config) ->
    dest_resource_alarm(<<"no-ack">>, Config).

dest_resource_alarm(AckMode, Config) ->
    SourceUri = ?config(shovel_source_uri, Config),
    SourceIdx = ?config(shovel_source_idx, Config),
    DestUri = ?config(shovel_dest_uri, Config),
    DestIdx = ?config(shovel_dest_idx, Config),

    {Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(
                     Config, SourceIdx),
    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:call(Ch1, #'queue.declare'{queue = <<"src">>}),
    publish(Ch1, <<>>, <<"src">>, <<"hello">>),
    true = amqp_channel:wait_for_confirms(Ch1),
    #{messages := 1} = message_count(Config, SourceIdx, <<"src">>),
    while_blocked(Config, DestIdx,
      fun() ->
              ok = rabbit_ct_broker_helpers:rpc(
                     Config, DestIdx,
                     rabbit_runtime_parameters, set,
                     [
                      <<"/">>, <<"shovel">>, <<"test">>,
                      [{<<"src-uri">>, SourceUri},
                       {<<"dest-uri">>, [DestUri]},
                       {<<"src-queue">>, <<"src">>},
                       {<<"dest-queue">>, <<"dest">>},
                       {<<"src-prefetch-count">>, 50},
                       {<<"ack-mode">>, AckMode},
                       {<<"src-delete-after">>, <<"never">>}], none]),
              %% The destination is blocked, so the shovel is blocked.
              ?awaitMatch(
                blocked,
                shovel_test_utils:get_shovel_status(Config, DestIdx,
                                                    <<"test">>),
                3_000),

              %% The shoveled message triggered a
              %% connection.blocked notification, but hasn't
              %% reached the dest queue because of the resource
              %% alarm
              InitialMsgCnt =
                  case AckMode of
                      <<"on-confirm">> -> 1;
                      _ -> 0
                  end,
              #{messages := InitialMsgCnt,
                messages_unacknowledged := InitialMsgCnt
               } = message_count(Config, SourceIdx, <<"src">>),
              #{messages := 0} = message_count(Config, DestIdx, <<"dest">>),

              %% Now publish more messages to "src" queue.
              publish_count(Ch1, <<>>, <<"src">>, <<"hello">>, 1000),
              true = amqp_channel:wait_for_confirms(Ch1),

              %% No messages reached the dest queue
              #{messages := 0} = message_count(Config, DestIdx, <<"dest">>),

              %% When the shovel sets a prefetch_count
              %% (on-confirm/on-publish mode), all messages are in
              %% the source queue, prefrech count are
              %% unacknowledged and buffered in the shovel
              MsgCnts =
                  case AckMode of
                      <<"on-confirm">> ->
                          #{messages => 1001,
                            messages_unacknowledged => 50};
                      <<"on-publish">> ->
                          #{messages => 1000,
                            messages_unacknowledged => 50};
                      <<"no-ack">> ->
                          %% no prefetch limit, all messages are
                          %% buffered in the shovel
                          #{messages => 0,
                            messages_unacknowledged => 0}
                  end,

              MsgCnts = message_count(Config, SourceIdx, <<"src">>),

              %% There should be no process with a message buildup
              ?awaitMatch(
                 0,
                 begin
                     Top = [{_, P, _}] = rabbit_ct_broker_helpers:rpc(
                         Config, 0, recon, proc_count, [message_queue_len, 1]),
                     ct:pal("Top process by message queue length: ~p", [Top]),
                     P
                 end, 5_000),

              ok
      end),

    %% After the alarm clears, all messages should arrive in the dest queue.
    ?awaitMatch(
      #{messages := 1001},
      message_count(Config, DestIdx, <<"dest">>),
      5_000),
    #{messages := 0} = message_count(Config, SourceIdx, <<"src">>),
    running = shovel_test_utils:get_shovel_status(Config, DestIdx, <<"test">>),

    rabbit_ct_client_helpers:close_connection_and_channel(Conn1, Ch1),
    cleanup(Config),
    ok.

%%----------------------------------------------------------------------------

conserve_resources(Pid, Source, {_, Conserve, AlarmedNode}) ->
    case Conserve of
        true ->
            ct:log("node ~w set alarm for resource ~ts",
                   [AlarmedNode, Source]),
            Pid ! {block, Source};
        false ->
            ct:log("node ~w cleared alarm for resource ~ts",
                   [AlarmedNode, Source]),
            Pid ! {unblock, Source}
    end,
    ok.

while_blocked(Config, Node, Fun) when is_function(Fun, 0) ->
    OrigLimit = rabbit_ct_broker_helpers:rpc(Config, Node,
                                             vm_memory_monitor,
                                             get_vm_memory_high_watermark, []),

    ok = rabbit_ct_broker_helpers:add_code_path_to_node(
           rabbit_ct_broker_helpers:get_node_config(Config, Node, nodename),
           ?MODULE),
    [] = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_alarm, register,
                                      [self(),
                                       {?MODULE, conserve_resources, []}]),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, vm_memory_monitor,
                                      set_vm_memory_high_watermark, [0]),
    Source = receive
                 {block, S} ->
                     S
             after
                 15_000 ->
                     ct:fail(alarm_set_timeout)
             end,
    try
        Fun()
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, Node, vm_memory_monitor,
                                          set_vm_memory_high_watermark,
                                          [OrigLimit]),
        receive
            {unblock, Source} ->
                ok
        after
            10_000 ->
                ct:fail(alarm_clear_timeout)
        end
    end.

publish(Ch, X, Key, Payload) when is_binary(Payload) ->
    publish(Ch, X, Key, #amqp_msg{payload = Payload});

publish(Ch, X, Key, Msg = #amqp_msg{}) ->
    amqp_channel:cast(Ch, #'basic.publish'{exchange    = X,
                                           routing_key = Key}, Msg).

publish_count(Ch, X, Key, M, Count) ->
    [begin

         publish(Ch, X, Key, M)
     end || _ <- lists:seq(1, Count)].

message_count(Config, Node, QueueName) ->
    Resource = rabbit_misc:r(<<"/">>, queue, QueueName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue,
                                           lookup, [Resource]),
    maps:from_list(
      rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue, info,
                                   [Q, [messages, messages_unacknowledged]])).

cleanup(Config) ->
    rabbit_ct_broker_helpers:rpc_all(Config, ?MODULE, cleanup1, []).

cleanup1() ->
    [rabbit_runtime_parameters:clear(rabbit_misc:pget(vhost, P),
                                     rabbit_misc:pget(component, P),
                                     rabbit_misc:pget(name, P),
                                     <<"acting-user">>) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>)
     || Q <- rabbit_amqqueue:list()].
