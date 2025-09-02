%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(local_dynamic_cluster_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(shovel_test_utils, [await_amqp10_event/3, await_credit/1,
                            with_amqp10_session/2]).

-define(PARAM, <<"test">>).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  local_to_local_dest_down,
                  local_to_local_multiple_all_dest_down,
                  local_to_local_multiple_some_dest_down,
                  local_to_local_no_destination
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
        {rmq_nodes_count, 3},
        {rmq_nodes_clustered, true},
        {ignored_crashes, [
          "server_initiated_close,404",
          "writer,send_failed,closed",
          "source_queue_down",
          "dest_queue_down"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
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

local_to_local_dest_down(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, 0, <<"/">>, Src),
    declare_queue(Config, 1, <<"/">>, Dest),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-exchange">>, <<>>},
                                           {<<"dest-exchange-key">>, Dest}
                                          ]),
              ok = rabbit_ct_broker_helpers:stop_node(Config, 1),
              publish_many(Sess, Src, Dest, <<"tag1">>, 10),
              ?awaitMatch([[<<"local_to_local_dest_down_dest">>, 0, 0, 0],
                           [<<"local_to_local_dest_down_src">>, 10, _, _]],
                          list_queue_messages(Config),
                          30000),
              ok = rabbit_ct_broker_helpers:start_node(Config, 1),
              ?awaitMatch([[<<"local_to_local_dest_down_dest">>, 10, 10, 0],
                           [<<"local_to_local_dest_down_src">>, 0, 0, 0]],
                          list_queue_messages(Config),
                          30000),
              expect_many(Sess, Dest, 10)
      end).

local_to_local_multiple_all_dest_down(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Dest2 = ?config(destq2, Config),
    declare_queue(Config, 0, <<"/">>, Src),
    declare_and_bind_queue(Config, 1, <<"/">>, <<"amq.fanout">>, Dest, Dest),
    declare_and_bind_queue(Config, 1, <<"/">>, <<"amq.fanout">>, Dest2, Dest2),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-exchange">>, <<"amq.fanout">>},
                                           {<<"dest-exchange-key">>, <<"">>}
                                          ]),
              ok = rabbit_ct_broker_helpers:stop_node(Config, 1),
              publish_many(Sess, Src, Dest, <<"tag1">>, 10),
              ?awaitMatch([[<<"local_to_local_multiple_all_dest_down_dest">>, 0, 0, 0],
                           [<<"local_to_local_multiple_all_dest_down_dest2">>, 0, 0, 0],
                           [<<"local_to_local_multiple_all_dest_down_src">>, 10, _, _]],
                          list_queue_messages(Config),
                          30000),
              ok = rabbit_ct_broker_helpers:start_node(Config, 1),
              ?awaitMatch([[<<"local_to_local_multiple_all_dest_down_dest">>, 10, 10, 0],
                           [<<"local_to_local_multiple_all_dest_down_dest2">>, 10, 10, 0],
                           [<<"local_to_local_multiple_all_dest_down_src">>, 0, 0, 0]],
                          list_queue_messages(Config),
                          30000),
              expect_many(Sess, Dest, 10)
      end).

local_to_local_multiple_some_dest_down(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    Dest2 = ?config(destq2, Config),
    declare_queue(Config, 0, <<"/">>, Src),
    %% Declare each destination queue in a different node. Just one of
    %% them will be down, but this still means the message can't be confirmed
    %% and should be requeued.
    declare_and_bind_queue(Config, 1, <<"/">>, <<"amq.fanout">>, Dest, Dest),
    declare_and_bind_queue(Config, 2, <<"/">>, <<"amq.fanout">>, Dest2, Dest2),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-exchange">>, <<"amq.fanout">>},
                                           {<<"dest-exchange-key">>, <<"">>}
                                          ]),
              ok = rabbit_ct_broker_helpers:stop_node(Config, 1),
              publish_many(Sess, Src, Dest, <<"tag1">>, 10),
              %% Messages won't be confirmed to source until all destination
              %% queues are able to confirm them, until them we keep retrying
              %% This generates multiple duplicates, but that's how publishing
              %% works.
              ?awaitMatch([[<<"local_to_local_multiple_some_dest_down_dest">>, 0, 0, 0],
                           [<<"local_to_local_multiple_some_dest_down_dest2">>, M, M, 0],
                           [<<"local_to_local_multiple_some_dest_down_src">>, 10, _, _]]
                          when (M > 10),
                          list_queue_messages(Config),
                          30000),
              ok = rabbit_ct_broker_helpers:start_node(Config, 1),
              ?awaitMatch([[<<"local_to_local_multiple_some_dest_down_dest">>, N, N, 0],
                           [<<"local_to_local_multiple_some_dest_down_dest2">>, M, M, 0],
                           [<<"local_to_local_multiple_some_dest_down_src">>, 0, 0, 0]]
                          when ((N == 10) and (M >= 10)),
                          list_queue_messages(Config),
                          30000),
              expect_many(Sess, Dest, 10)
      end).

local_to_local_no_destination(Config) ->
    Src = ?config(srcq, Config),
    Dest = ?config(destq, Config),
    declare_queue(Config, 0, <<"/">>, Src),
    with_amqp10_session(
      Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, ?PARAM,
                                          [{<<"src-protocol">>, <<"local">>},
                                           {<<"src-queue">>, Src},
                                           {<<"dest-protocol">>, <<"local">>},
                                           {<<"dest-exchange">>, <<"amq.fanout">>},
                                           {<<"dest-exchange-key">>, Dest}
                                          ]),
              publish_many(Sess, Src, Dest, <<"tag1">>, 10),
              ?awaitMatch([[<<"local_to_local_no_destination_src">>, 0, 0, 0]],
                          list_queue_messages(Config),
                          30000)
      end).

%%----------------------------------------------------------------------------
list_queue_messages(Config) ->
    [[N, to_int(M), to_int(MR), to_int(MU)]
     || [N, M, MR, MU] <- lists:sort(
                            rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0,
                              ["list_queues", "name", "messages", "messages_ready", "messages_unacknowledged", "--no-table-headers"]))].

to_int(<<>>) ->
    0;
to_int(Int) ->
    binary_to_integer(Int).

publish(Sender, Tag, Payload) when is_binary(Payload) ->
    Headers = #{durable => true},
    Msg = amqp10_msg:set_headers(Headers,
                                 amqp10_msg:new(Tag, Payload, false)),
    %% N.B.: this function does not attach a link and does not
    %%       need to use await_credit/1
    ok = amqp10_client:send_msg(Sender, Msg),
    receive
        {amqp10_disposition, {accepted, Tag}} -> ok
    after 3000 ->
              exit(publish_disposition_not_received)
    end.

publish(Session, Source, Dest, Tag, Payloads) ->
    LinkName = <<"dynamic-sender-", Dest/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName, Source,
                                                    unsettled, unsettled_state),
    ok = await_amqp10_event(link, Sender, attached),
    ok = await_credit(Sender),
    case is_list(Payloads) of
        true ->
            [publish(Sender, Tag, Payload) || Payload <- Payloads];
        false ->
            publish(Sender, Tag, Payloads)
    end,
    amqp10_client:detach_link(Sender).

publish_many(Session, Source, Dest, Tag, N) ->
    Payloads = [integer_to_binary(Payload) || Payload <- lists:seq(1, N)],
    publish(Session, Source, Dest, Tag, Payloads).

expect_many(Session, Dest, N) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest, settled,
                                                        unsettled_state),
    ok = amqp10_client:flow_link_credit(Receiver, 10, 1),
    Msgs = expect(Receiver, N, []),
    amqp10_client:detach_link(Receiver),
    Msgs.

expect(_, 0, Acc) ->
    Acc;
expect(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, InMsg} ->
            expect(Receiver, N - 1, [amqp10_msg:body(InMsg) | Acc])
    after 4000 ->
            throw({timeout_in_expect_waiting_for_delivery, N, Acc})
    end.

expect(Receiver) ->
    receive
        {amqp10_msg, Receiver, InMsg} ->
            InMsg
    after 4000 ->
            throw(timeout_in_expect_waiting_for_delivery)
    end.

declare_queue(Config, Node, VHost, QName) ->
    declare_queue(Config, Node, VHost, QName, []).

declare_queue(Config, Node, VHost, QName, Args) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch, #'queue.declare'{queue = QName, durable = true, arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn).

declare_and_bind_queue(Config, Node, VHost, Exchange, QName, RoutingKey) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node, VHost),
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
