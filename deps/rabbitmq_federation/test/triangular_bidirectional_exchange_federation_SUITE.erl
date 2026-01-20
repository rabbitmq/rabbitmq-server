%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(triangular_bidirectional_exchange_federation_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-import(rabbit_federation_test_util,
        [count_running_links/1, uri_for_vhost/2]).

-define(VHOST_1, <<"vh-fed.1">>).
-define(VHOST_2, <<"vh-fed.2">>).
-define(VHOST_3, <<"vh-fed.3">>).
-define(VHOSTS, [?VHOST_1, ?VHOST_2, ?VHOST_3]).

-define(EXCHANGE_NAME, <<"fed.x">>).
-define(QUEUE_PREFIX, <<"fed.q">>).
-define(QUEUES_PER_VHOST, 3).
-define(MESSAGE_COUNT, 500).

all() ->
    [
     {group, triangle_topology},
     {group, triangle_topology_multi_node}
    ].

groups() ->
    [
     {triangle_topology, [], [
                              triangle_federation_with_messages
                             ]},
     {triangle_topology_multi_node, [], [
                                         triangle_federation_with_messages_multi_node
                                        ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(triangle_topology, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps() ++
        [fun setup_vhosts/1,
         fun setup_federation_triangle/1]);
init_per_group(triangle_topology_multi_node, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Suffix},
                {rmq_nodes_count, 3},
                {rmq_nodes_clustered, false},
                %% Skip past the ports used by the triangle_topology group (1 node)
                %% to avoid port conflicts
                {tcp_ports_base, {skip_n_nodes, 1}}
            ]),
            rabbit_ct_helpers:run_steps(Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps() ++
                [fun setup_federation_triangle_multi_node/1])
    end;
init_per_group(_, Config) ->
    Config.

end_per_group(triangle_topology, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        [fun cleanup_federation_triangle/1] ++
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(triangle_topology_multi_node, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        [fun cleanup_federation_triangle_multi_node/1] ++
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Setup helpers
%% -------------------------------------------------------------------

setup_vhosts(Config) ->
    %% Be defensive: delete vhosts first in case they exist from previous failed runs.
    [rabbit_ct_broker_helpers:delete_vhost(Config, VHost) || VHost <- ?VHOSTS],
    [begin
         ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost),
         ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost)
     end || VHost <- ?VHOSTS],
    Config.

setup_federation_triangle(Config) ->
    Uri = rabbit_ct_broker_helpers:node_uri(Config, 0),

    lists:foreach(
      fun(LocalVHost) ->
              lists:foreach(
                fun(RemoteVHost) when LocalVHost =/= RemoteVHost ->
                        UpstreamName = upstream_name(RemoteVHost),
                        RemoteUri = uri_for_vhost(Uri, RemoteVHost),
                        rabbit_ct_broker_helpers:set_parameter(
                          Config, 0, LocalVHost,
                          <<"federation-upstream">>, UpstreamName,
                          [{<<"uri">>, RemoteUri},
                           {<<"prefetch-count">>, 1000},
                           {<<"reconnect-delay">>, 1},
                           {<<"max-hops">>, 2}]);
                   (_) ->
                        ok
                end, ?VHOSTS)
      end, ?VHOSTS),

    lists:foreach(
      fun(VHost) ->
              {ok, Ch} = open_channel_to_vhost(Config, VHost),
              try
                  declare_exchange(Ch, ?EXCHANGE_NAME, <<"fanout">>),
                  lists:foreach(
                    fun(N) ->
                            QName = queue_name(N),
                            declare_queue(Ch, QName),
                            bind_queue(Ch, QName, ?EXCHANGE_NAME, <<>>)
                    end, lists:seq(1, ?QUEUES_PER_VHOST))
              after
                  amqp_channel:close(Ch)
              end
      end, ?VHOSTS),

    lists:foreach(
      fun(VHost) ->
              rabbit_ct_broker_helpers:set_policy_in_vhost(
                Config, 0, VHost,
                <<"federate-exchanges">>, <<"^fed\\.">>, <<"exchanges">>,
                [{<<"federation-upstream-set">>, <<"all">>}])
      end, ?VHOSTS),

    await_federation_links_running(Config, 6, 60000),

    Config.

cleanup_federation_triangle(Config) ->
    lists:foreach(
      fun(VHost) ->
              catch rabbit_ct_broker_helpers:clear_policy(Config, 0, VHost, <<"federate-exchanges">>),
              lists:foreach(
                fun(RemoteVHost) when VHost =/= RemoteVHost ->
                        UpstreamName = upstream_name(RemoteVHost),
                        catch rabbit_ct_broker_helpers:clear_parameter(
                                Config, 0, VHost, <<"federation-upstream">>, UpstreamName);
                   (_) ->
                        ok
                end, ?VHOSTS),
              catch rabbit_ct_broker_helpers:delete_vhost(Config, VHost)
      end, ?VHOSTS),
    Config.

setup_federation_triangle_multi_node(Config) ->
    Nodes = [0, 1, 2],

    lists:foreach(
      fun(Node) ->
              {ok, _} = rabbit_ct_broker_helpers:rpc(
                          Config, Node, application, ensure_all_started,
                          [rabbitmq_federation])
      end, Nodes),

    lists:foreach(
      fun(LocalNode) ->
              lists:foreach(
                fun(RemoteNode) when LocalNode =/= RemoteNode ->
                        UpstreamName = upstream_name_for_node(RemoteNode),
                        RemoteUri = rabbit_ct_broker_helpers:node_uri(Config, RemoteNode),
                        rabbit_ct_broker_helpers:set_parameter(
                          Config, LocalNode, <<"/">>,
                          <<"federation-upstream">>, UpstreamName,
                          [{<<"uri">>, RemoteUri},
                           {<<"prefetch-count">>, 1000},
                           {<<"reconnect-delay">>, 1},
                           {<<"max-hops">>, 2}]);
                   (_) ->
                        ok
                end, Nodes)
      end, Nodes),

    lists:foreach(
      fun(Node) ->
              {ok, Ch} = open_channel_to_node(Config, Node),
              try
                  declare_exchange(Ch, ?EXCHANGE_NAME, <<"fanout">>),
                  lists:foreach(
                    fun(N) ->
                            QName = queue_name(N),
                            declare_queue(Ch, QName),
                            bind_queue(Ch, QName, ?EXCHANGE_NAME, <<>>)
                    end, lists:seq(1, ?QUEUES_PER_VHOST))
              after
                  amqp_channel:close(Ch)
              end
      end, Nodes),

    lists:foreach(
      fun(Node) ->
              rabbit_ct_broker_helpers:set_policy(
                Config, Node,
                <<"federate-exchanges">>, <<"^fed\\.">>, <<"exchanges">>,
                [{<<"federation-upstream-set">>, <<"all">>}])
      end, Nodes),

    await_federation_links_running_multi_node(Config, 6, 60000),

    Config.

cleanup_federation_triangle_multi_node(Config) ->
    Nodes = [0, 1, 2],
    lists:foreach(
      fun(Node) ->
              catch rabbit_ct_broker_helpers:clear_policy(Config, Node, <<"federate-exchanges">>),
              lists:foreach(
                fun(RemoteNode) when Node =/= RemoteNode ->
                        UpstreamName = upstream_name_for_node(RemoteNode),
                        catch rabbit_ct_broker_helpers:clear_parameter(
                                Config, Node, <<"/">>, <<"federation-upstream">>, UpstreamName);
                   (_) ->
                        ok
                end, Nodes)
      end, Nodes),
    Config.

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

triangle_federation_with_messages(Config) ->
    ?assertEqual(6, count_running_links(Config)),

    MessagesByVHost = publish_messages_with_confirms(Config, ?MESSAGE_COUNT),
    ct:pal("Published messages by vhost: ~p", [MessagesByVHost]),

    ExpectedTotal = ?MESSAGE_COUNT * 15,
    ct:pal("Waiting for ~p messages to be federated", [ExpectedTotal]),
    await_message_count(Config, ExpectedTotal, 120000),

    {TotalMessages, QueueCounts} = get_message_counts(Config),
    ct:pal("Total messages across all queues: ~p", [TotalMessages]),
    ct:pal("Per queue counts: ~p", [QueueCounts]),

    lists:foreach(
      fun({VHost, QName, Count}) ->
              ?assert(Count > 0,
                      lists:flatten(io_lib:format(
                                      "Queue ~s in ~s should have messages, got ~p",
                                      [QName, VHost, Count])))
      end, QueueCounts),

    %% Each message published to a fanout exchange goes to 3 local queues.
    %%
    %% With federation and max-hops = 2, the messages also propagate as follows:
    %% To 3 queues in the original virtual host A ("direct" routing, no federation links involved)
    %% 6 queues per virtual hosts B and C (3 from direct federation, 3 the 2nd hop, e.g. to C from A via B).
    %% So overall we are looking at 15 copies per message.
    %%
    %% Federation loop detection via the x-received-from header prevents messages from
    %% making rounds and accumulating indefinitely.
    ExpectedTotal = ?MESSAGE_COUNT * 15,
    ?assertEqual(ExpectedTotal, TotalMessages,
                 lists:flatten(io_lib:format(
                                 "Expected exactly ~p messages (500 * 15), got ~p",
                                 [ExpectedTotal, TotalMessages]))),

    ok.

triangle_federation_with_messages_multi_node(Config) ->
    ?assertEqual(6, count_running_links_multi_node(Config)),

    MessagesByNode = publish_messages_with_confirms_multi_node(Config, ?MESSAGE_COUNT),
    ct:pal("Published messages by node: ~p", [MessagesByNode]),

    ExpectedTotal = ?MESSAGE_COUNT * 15,
    ct:pal("Waiting for ~p messages to be federated", [ExpectedTotal]),
    await_message_count_multi_node(Config, ExpectedTotal, 120000),

    {TotalMessages, QueueCounts} = get_message_counts_multi_node(Config),
    ct:pal("Total messages across all queues: ~p", [TotalMessages]),
    ct:pal("Per queue counts: ~p", [QueueCounts]),

    lists:foreach(
      fun({Node, QName, Count}) ->
              ?assert(Count > 0,
                      lists:flatten(io_lib:format(
                                      "Queue ~s on node ~p should have messages, got ~p",
                                      [QName, Node, Count])))
      end, QueueCounts),

    ExpectedTotal = ?MESSAGE_COUNT * 15,
    ?assertEqual(ExpectedTotal, TotalMessages,
                 lists:flatten(io_lib:format(
                                 "Expected exactly ~p messages (500 * 15), got ~p",
                                 [ExpectedTotal, TotalMessages]))),

    ok.

%% -------------------------------------------------------------------
%% Helper functions
%% -------------------------------------------------------------------

upstream_name(VHost) ->
    <<"upstream-to-", VHost/binary>>.

queue_name(N) ->
    NBin = integer_to_binary(N),
    <<?QUEUE_PREFIX/binary, ".", NBin/binary>>.

open_channel_to_vhost(Config, VHost) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection_direct(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    put({connection, VHost}, Conn),
    {ok, Ch}.

close_vhost_connections() ->
    lists:foreach(
      fun(VHost) ->
              case get({connection, VHost}) of
                  undefined -> ok;
                  Conn ->
                      catch amqp_connection:close(Conn),
                      erase({connection, VHost})
              end
      end, ?VHOSTS).

declare_exchange(Ch, Name, Type) ->
    #'exchange.declare_ok'{} =
        amqp_channel:call(Ch, #'exchange.declare'{
                                 exchange = Name,
                                 type = Type,
                                 durable = true
                                }).

declare_queue(Ch, Name) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{
                                 queue = Name,
                                 durable = true,
                                 arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]
                                }).

bind_queue(Ch, Queue, Exchange, RoutingKey) ->
    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{
                                 queue = Queue,
                                 exchange = Exchange,
                                 routing_key = RoutingKey
                                }).

publish_messages_with_confirms(Config, Count) ->
    Channels = lists:map(
                 fun(VHost) ->
                         {ok, Ch} = open_channel_to_vhost(Config, VHost),
                         #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
                         amqp_channel:register_confirm_handler(Ch, self()),
                         {VHost, Ch}
                 end, ?VHOSTS),

    MessagesByVHost = lists:foldl(
                        fun(N, Acc) ->
                                VHostIdx = rand:uniform(length(?VHOSTS)),
                                VHost = lists:nth(VHostIdx, ?VHOSTS),
                                {VHost, Ch} = lists:keyfind(VHost, 1, Channels),
                                Payload = iolist_to_binary(
                                            io_lib:format("Message ~p from ~s", [N, VHost])),
                                ok = amqp_channel:cast(
                                       Ch,
                                       #'basic.publish'{
                                          exchange = ?EXCHANGE_NAME,
                                          routing_key = <<>>
                                         },
                                       #amqp_msg{payload = Payload}),
                                maps:update_with(VHost, fun(V) -> V + 1 end, 1, Acc)
                        end, #{}, lists:seq(1, Count)),

    lists:foreach(
      fun({_VHost, Ch}) ->
              true = amqp_channel:wait_for_confirms(Ch, 30000)
      end, Channels),

    lists:foreach(
      fun({_VHost, Ch}) ->
              catch amqp_channel:close(Ch)
      end, Channels),

    close_vhost_connections(),
    MessagesByVHost.

await_federation_links_running(Config, ExpectedCount, Timeout) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              count_running_links(Config) >= ExpectedCount
      end, Timeout).

await_message_count(Config, ExpectedCount, Timeout) ->
    await_message_count(Config, ExpectedCount, Timeout, 0).

await_message_count(Config, ExpectedCount, Timeout, LastLogged) ->
    {Total, _QueueCounts} = get_message_counts(Config),
    case Total >= ExpectedCount of
        true ->
            ct:pal("Message count reached: ~p (expected ~p)", [Total, ExpectedCount]),
            ok;
        false when Timeout =< 0 ->
            ct:pal("Message count timeout: ~p (expected ~p)", [Total, ExpectedCount]),
            ct:fail("Expected ~p messages but only got ~p", [ExpectedCount, Total]);
        false ->
            NewLastLogged = case Total =/= LastLogged of
                                true ->
                                    ct:pal("Current message count: ~p (waiting for ~p)", [Total, ExpectedCount]),
                                    Total;
                                false ->
                                    LastLogged
                            end,
            timer:sleep(200),
            await_message_count(Config, ExpectedCount, Timeout - 200, NewLastLogged)
    end.

get_message_counts(Config) ->
    QueueCounts =
        lists:flatmap(
          fun(VHost) ->
                  lists:map(
                    fun(N) ->
                            QName = queue_name(N),
                            Count = get_queue_message_count(Config, VHost, QName),
                            {VHost, QName, Count}
                    end, lists:seq(1, ?QUEUES_PER_VHOST))
          end, ?VHOSTS),
    TotalMessages = lists:sum([C || {_, _, C} <- QueueCounts]),
    {TotalMessages, QueueCounts}.

get_queue_message_count(Config, VHost, QName) ->
    Resource = rabbit_misc:r(VHost, queue, QName),
    case rabbit_ct_broker_helpers:rpc(Config, 0,
           rabbit_amqqueue, lookup, [Resource]) of
        {ok, Q} ->
            [{messages, Count}] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                    rabbit_amqqueue, info, [Q, [messages]]),
            Count;
        {error, not_found} ->
            0
    end.

%% -------------------------------------------------------------------
%% Multi-node helper functions
%% -------------------------------------------------------------------

upstream_name_for_node(Node) ->
    NodeBin = integer_to_binary(Node),
    <<"upstream-to-node-", NodeBin/binary>>.

open_channel_to_node(Config, Node) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    put({connection, Node}, Conn),
    {ok, Ch}.

close_node_connections() ->
    lists:foreach(
      fun(Node) ->
              case get({connection, Node}) of
                  undefined -> ok;
                  Conn ->
                      catch amqp_connection:close(Conn),
                      erase({connection, Node})
              end
      end, [0, 1, 2]).

count_running_links_multi_node(Config) ->
    lists:sum([count_running_links_on_node(Config, Node) || Node <- [0, 1, 2]]).

count_running_links_on_node(Config, Node) ->
    Status = rabbit_ct_broker_helpers:rpc(Config, Node,
               rabbit_federation_status, status, []),
    length([S || S <- Status, proplists:get_value(status, S) =:= running]).

publish_messages_with_confirms_multi_node(Config, Count) ->
    Nodes = [0, 1, 2],
    Channels = lists:map(
                 fun(Node) ->
                         {ok, Ch} = open_channel_to_node(Config, Node),
                         #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
                         amqp_channel:register_confirm_handler(Ch, self()),
                         {Node, Ch}
                 end, Nodes),

    MessagesByNode = lists:foldl(
                       fun(N, Acc) ->
                               NodeIdx = rand:uniform(length(Nodes)),
                               Node = lists:nth(NodeIdx, Nodes),
                               {Node, Ch} = lists:keyfind(Node, 1, Channels),
                               Payload = iolist_to_binary(
                                           io_lib:format("Message ~p from node ~p", [N, Node])),
                               ok = amqp_channel:cast(
                                      Ch,
                                      #'basic.publish'{
                                         exchange = ?EXCHANGE_NAME,
                                         routing_key = <<>>
                                        },
                                      #amqp_msg{payload = Payload}),
                               maps:update_with(Node, fun(V) -> V + 1 end, 1, Acc)
                       end, #{}, lists:seq(1, Count)),

    lists:foreach(
      fun({_Node, Ch}) ->
              true = amqp_channel:wait_for_confirms(Ch, 30000)
      end, Channels),

    lists:foreach(
      fun({_Node, Ch}) ->
              catch amqp_channel:close(Ch)
      end, Channels),

    close_node_connections(),
    MessagesByNode.

await_federation_links_running_multi_node(Config, ExpectedCount, Timeout) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              count_running_links_multi_node(Config) >= ExpectedCount
      end, Timeout).

await_message_count_multi_node(Config, ExpectedCount, Timeout) ->
    await_message_count_multi_node(Config, ExpectedCount, Timeout, 0).

await_message_count_multi_node(Config, ExpectedCount, Timeout, LastLogged) ->
    {Total, _QueueCounts} = get_message_counts_multi_node(Config),
    case Total >= ExpectedCount of
        true ->
            ct:pal("Message count reached: ~p (expected ~p)", [Total, ExpectedCount]),
            ok;
        false when Timeout =< 0 ->
            ct:pal("Message count timeout: ~p (expected ~p)", [Total, ExpectedCount]),
            ct:fail("Expected ~p messages but only got ~p", [ExpectedCount, Total]);
        false ->
            NewLastLogged = case Total =/= LastLogged of
                                true ->
                                    ct:pal("Current message count: ~p (waiting for ~p)", [Total, ExpectedCount]),
                                    Total;
                                false ->
                                    LastLogged
                            end,
            timer:sleep(200),
            await_message_count_multi_node(Config, ExpectedCount, Timeout - 200, NewLastLogged)
    end.

get_message_counts_multi_node(Config) ->
    Nodes = [0, 1, 2],
    QueueCounts =
        lists:flatmap(
          fun(Node) ->
                  lists:map(
                    fun(N) ->
                            QName = queue_name(N),
                            Count = get_queue_message_count_on_node(Config, Node, QName),
                            {Node, QName, Count}
                    end, lists:seq(1, ?QUEUES_PER_VHOST))
          end, Nodes),
    TotalMessages = lists:sum([C || {_, _, C} <- QueueCounts]),
    {TotalMessages, QueueCounts}.

get_queue_message_count_on_node(Config, Node, QName) ->
    Resource = rabbit_misc:r(<<"/">>, queue, QName),
    case rabbit_ct_broker_helpers:rpc(Config, Node,
           rabbit_amqqueue, lookup, [Resource]) of
        {ok, Q} ->
            [{messages, Count}] = rabbit_ct_broker_helpers:rpc(Config, Node,
                                    rabbit_amqqueue, info, [Q, [messages]]),
            Count;
        {error, not_found} ->
            0
    end.
