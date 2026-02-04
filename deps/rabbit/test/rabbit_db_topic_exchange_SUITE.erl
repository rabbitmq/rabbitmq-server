%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_topic_exchange_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         topic_trie_cleanup/1
        ]).

-define(VHOST, <<"/">>).

all() ->
    [
     {group, khepri_store}
    ].

groups() ->
    [
     {khepri_store, [], khepri_tests()}
    ].

khepri_tests() ->
    [
     topic_trie_cleanup
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(khepri_store = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{rmq_nodes_count, 3}]),
    init_per_group_common(Group, Config);
init_per_group(Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{rmq_nodes_count, 1}]),
    init_per_group_common(Group, Config).

init_per_group_common(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    XName = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    {ok, X} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, lookup, [XName]),
    Config1 = rabbit_ct_helpers:set_config(Config, [{exchange_name, XName},
                                                    {exchange, X}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_topic_exchange, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Khepri-specific Tests
%% ---------------------------------------------------------------------------

% https://github.com/rabbitmq/rabbitmq-server/issues/15024
topic_trie_cleanup(Config) ->
    [_, OldNode, NewNode] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% this test has to be isolated to avoid flakes
    VHost = <<"test-vhost-topic-trie">>,
    ok = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_vhost, add, [VHost, <<"test-user">>]),

    %% Create an exchange in the vhost
    ExchangeName = rabbit_misc:r(VHost, exchange, <<"test-topic-exchange">>),
    {ok, _Exchange} = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_exchange, declare,
                        [ExchangeName, topic, _Durable = true, _AutoDelete = false,
                         _Internal = false, _Args = [], <<"test-user">>]),

    %% List of routing keys that exercise topic exchange functionality
    RoutingKeys = [
                   %% Exact patterns with common prefixes
                   <<"a.b.c">>,
                   <<"a.b.d">>,
                   <<"a.b.e">>,
                   <<"a.c.d">>,
                   <<"a.c.e">>,
                   <<"b.c.d">>,
                   %% Patterns with a single wildcard
                   <<"a.*.c">>,
                   <<"a.*.d">>,
                   <<"*.b.c">>,
                   <<"*.b.d">>,
                   <<"a.b.*">>,
                   <<"a.c.*">>,
                   <<"*.*">>,
                   <<"a.*">>,
                   <<"*.b">>,
                   <<"*">>,
                   %% Patterns with multiple wildcards
                   <<"a.#">>,
                   <<"a.b.#">>,
                   <<"a.c.#">>,
                   <<"#.c">>,
                   <<"#.b.c">>,
                   <<"#.b.d">>,
                   <<"#">>,
                   <<"#.#">>,
                   %% Mixed patterns
                   <<"a.*.#">>,
                   <<"*.b.#">>,
                   <<"*.#">>,
                   <<"#.*">>,
                   <<"#.*.#">>,
                   %% More complex patterns with common prefixes
                   <<"orders.created.#">>,
                   <<"orders.updated.#">>,
                   <<"orders.*.confirmed">>,
                   <<"orders.#">>,
                   <<"events.user.#">>,
                   <<"events.system.#">>,
                   <<"events.#">>
                  ],

    %% Shuffle the routing keys to test in random order
    ShuffledRoutingKeys = [RK || {_, RK} <- lists:sort([{rand:uniform(), RK} || RK <- RoutingKeys])],

    %% Create bindings for all routing keys
    Bindings = [begin
                    QueueName = rabbit_misc:r(VHost, queue,
                                              list_to_binary("queue-" ++ integer_to_list(Idx))),
                    Ret = rabbit_ct_broker_helpers:rpc(
                            Config, OldNode,
                            rabbit_amqqueue, declare, [QueueName, true, false, [], self(), <<"test-user">>]),
                    case Ret of
                        {new, _Q} -> ok;
                        {existing, _Q} -> ok
                    end,
                    #binding{source = ExchangeName,
                             key = RoutingKey,
                             destination = QueueName,
                             args = []}
                end || {Idx, RoutingKey} <- lists:enumerate(ShuffledRoutingKeys)],

    %% Add all bindings
    [ok = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_binding, add, [B, <<"test-user">>])
     || B <- Bindings],

    %% Log entries that were added to the ETS table
    lists:foreach(
      fun(Node) ->
              VHostEntriesAfterAdd = read_topic_trie_table(Config, Node, VHost, rabbit_khepri_topic_trie_v3),
              ct:pal("Bindings added on node ~s: ~p, ETS entries after add: ~p~n",
                     [Node, length(Bindings), length(VHostEntriesAfterAdd)])
      end, Nodes),

    %% Shuffle bindings again for deletion in random order
    ShuffledBindings = [B || {_, B} <- lists:sort([{rand:uniform(), B} || B <- Bindings])],

    %% Delete all bindings in random order
    [ok = rabbit_ct_broker_helpers:rpc(Config, OldNode, rabbit_binding, remove, [B, <<"test-user">>])
     || B <- ShuffledBindings],

    %% Verify that the projection ETS table doesn't contain any entries related
    %% to this vhost
    try
        lists:foreach(
          fun(Node) ->
                  %% We read and check the new projection table only. It is
                  %% declared by the new node and is available everywhere. The
                  %% old projection table might be there in case of
                  %% mixed-version testing. This part will be tested in the
                  %% second part of the testcase.
                  VHostEntriesAfterDelete = read_topic_trie_table(Config, Node, VHost, rabbit_khepri_topic_trie_v3),
                  ct:pal("ETS entries after delete on node ~s: ~p~n", [Node, length(VHostEntriesAfterDelete)]),

                  % %% Assert that no entries were found for this vhost after deletion
                  % ?assertEqual([], VHostEntriesAfterDelete)
                  {ok, KhepriVersion0} = rabbit_ct_broker_helpers:rpc(
                                           Config, Node,
                                           application, get_key,
                                           [khepri, vsn]),
                  KhepriVersion1 = rabbit_misc:format(
                                     "~2..0s~2..0s~2..0s",
                                     string:split(KhepriVersion0, ".", all)),
                  KhepriVersion2 = list_to_integer(KhepriVersion1),
                  ct:pal("Khepri version: ~b / ~s", [KhepriVersion2, KhepriVersion1]),
                  if
                      KhepriVersion2 >= 1704 ->
                          %% Assert that no entries were found for this vhost
                          %% after deletion.
                          ?assertEqual([], VHostEntriesAfterDelete);
                      true ->
                          %% Assert that no entries were found for this vhost
                          %% after deletion. This node does not have the fixed
                          %% version of Khepri, so it won't delete the new
                          %% `#topic_trie_edge_v2{}' correctly, that's why we
                          %% filter them out.
                          ct:pal("Consider #topic_trie_edge{} records only"),
                          ?assertEqual(
                             [],
                             [E || #topic_trie_edge{} = E
                                   <- VHostEntriesAfterDelete])
                  end
          end, Nodes),

        %% If we reach this point, we know the new projection works as expected
        %% and the leaked ETS entries are no more.
        %%
        %% Now, we want to test that after an upgrade, the old projection is
        %% unregistered.
        HasOldProjection = try
                               VHostEntriesInOldTable = read_topic_trie_table(
                                                          Config, OldNode, VHost, rabbit_khepri_topic_trie),
                               ct:pal("Old ETS table entries after delete: ~p~n", [length(VHostEntriesInOldTable)]),
                               ?assertNotEqual([], VHostEntriesInOldTable),
                               true
                           catch
                               error:{exception, badarg, _} ->
                                   %% The old projection doesn't exist. The old
                                   %% node, if we are in a mixed-version test,
                                   %% also supports the new projection. There
                                   %% is nothing more to test.
                                   ct:pal("The old projection was not registered, nothing to test"),
                                   false
                           end,

        case HasOldProjection of
            true ->
                %% The old projection is registered. Simulate an update by removing
                %% node 1 (which is the old one in our mixed-version testing) from
                %% the cluster, then restart node 2. On restart, it should
                %% unregister the old projection.
                %%
                %% FIXME: The cluster is configured at the test group level.
                %% Therefore, if we add more testcases to this group, following
                %% testcases won't have the expected cluster.
                ?assertEqual(ok, rabbit_ct_broker_helpers:stop_broker(Config, OldNode)),
                ?assertEqual(ok, rabbit_ct_broker_helpers:forget_cluster_node(Config, NewNode, OldNode)),

                ct:pal("Restart new node (node 2)"),
                ?assertEqual(ok, rabbit_ct_broker_helpers:restart_broker(Config, NewNode)),

                ct:pal("Wait for projections to be restored"),
                ?awaitMatch(
                   Entries when is_list(Entries),
                   catch read_topic_trie_table(Config, NewNode, VHost, rabbit_khepri_topic_trie_v3),
                   60000),

                ct:pal("Check that the old projections are gone"),
                lists:foreach(
                  fun(ProjectionName) ->
                          ?assertError(
                             {exception, badarg, _},
                             read_topic_trie_table(Config, NewNode, VHost, ProjectionName))
                  end, [rabbit_khepri_topic_trie,
                        rabbit_khepri_topic_trie_v2]);
            false ->
                ok
        end
    after
        %% Clean up the vhost
        ok = rabbit_ct_broker_helpers:rpc(Config, NewNode, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,

    passed.

read_topic_trie_table(Config, Node, VHost, Table) ->
    Entries = rabbit_ct_broker_helpers:rpc(Config, Node, ets, tab2list, [Table]),
    [Entry || Entry <- Entries,
              case Entry of
                  #topic_trie_edge{trie_edge = TrieEdge} ->
                      case TrieEdge of
                          #trie_edge{exchange_name = #resource{virtual_host = V}} ->
                              V =:= VHost;
                          _ ->
                              false
                      end;
                  #topic_trie_edge_v2{trie_edge = TrieEdge} ->
                      case TrieEdge of
                          #trie_edge{exchange_name = #resource{virtual_host = V}} ->
                              V =:= VHost;
                          _ ->
                              false
                      end
              end].
