%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(clustering_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_broker_helpers, [get_node_config/3, restart_node/2]).
-import(rabbit_mgmt_test_util, [http_get/2, http_put/4, http_post/4, http_delete/3, http_delete/4]).
-import(rabbit_misc, [pget/2]).

-compile(nowarn_export_all).
-compile(export_all).

-define(STATS_INTERVAL, 250).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [{non_parallel_tests, [], [
                               list_cluster_nodes_test,
                               queue_on_other_node,
                               queue_with_multiple_consumers,
                               queue_consumer_cancelled,
                               queue_consumer_channel_closed,
                               queue,
                               queues_single,
                               queues_multiple,
                               queues_removed,
                               channels_multiple_on_different_nodes,
                               channel_closed,
                               channel,
                               channel_other_node,
                               channel_with_consumer_on_other_node,
                               channel_with_consumer_on_one_node,
                               consumers,
                               connections,
                               exchanges,
                               exchange,
                               vhosts,
                               nodes,
                               overview,
                               disable_plugin,
                               qq_replicas_add,
                               qq_replicas_delete,
                               qq_replicas_grow,
                               qq_replicas_shrink
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config, {rabbit, [
                                  {collect_statistics, fine},
                                  {collect_statistics_interval, ?STATS_INTERVAL},
                                  {core_metrics_gc_interval, 500}
                                 ]}),
    rabbit_ct_helpers:merge_app_env(Config1,
                                    {rabbitmq_management_agent, [
                                     {rates_mode, detailed},
                                     {sample_retention_policies,
                                          %% List of {MaxAgeInSeconds, SampleEveryNSeconds}
                                          [{global,   [{605, 5}, {3660, 60}, {29400, 600}, {86400, 1800}]},
                                           {basic,    [{605, 5}, {3600, 60}]},
                                           {detailed, [{10, 5}]}] }]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE},
                                                    {rmq_nodes_count, 2}
                                                   ]),
    Config2 = merge_app_env(Config1),
    rabbit_ct_helpers:run_setup_steps(Config2,
                                      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, clear_all_table_data, []),
    rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE, clear_all_table_data, []),
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"clustering_SUITE:init_per_testcase">>),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Config1 = rabbit_ct_helpers:set_config(Config, {conn, Conn}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:close_connection(?config(conn, Config)),
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"clustering_SUITE:end_per_testcase">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

list_cluster_nodes_test(Config) ->
    %% see rmq_nodes_count in init_per_suite
    ?assertEqual(2, length(http_get(Config, "/nodes"))),
    passed.

qq_replicas_add(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare_quorum(Chan, <<"qq.22">>),
    _ = wait_for_queue(Config, "/queues/%2F/qq.22"),

    Nodename1 = rabbit_data_coercion:to_binary(get_node_config(Config, 1, nodename)),
    Body = [{node, Nodename1}],
    http_post(Config, "/queues/quorum/%2F/qq.22/replicas/add", Body, ?NO_CONTENT),

    http_delete(Config, "/queues/%2F/qq.22", ?NO_CONTENT),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

qq_replicas_delete(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare_quorum(Chan, <<"qq.23">>),

    ?awaitMatch(#{members := [_, _]}, http_get(Config, "/queues/%2F/qq.23"), 30000),

    Nodename1 = rabbit_data_coercion:to_binary(get_node_config(Config, 1, nodename)),
    Body = [{node, Nodename1}],

    http_delete(Config, "/queues/quorum/%2F/qq.23/replicas/delete", ?ACCEPTED, Body),
    ?awaitMatch(#{members := [_]}, http_get(Config, "/queues/%2F/qq.23"), 30000),

    http_post(Config, "/queues/quorum/%2F/qq.23/replicas/add", Body, ?NO_CONTENT),
    ?awaitMatch(#{members := [_, _]}, http_get(Config, "/queues/%2F/qq.23"), 30000),

    http_delete(Config, "/queues/%2F/qq.23", ?NO_CONTENT),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

qq_replicas_grow(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare_quorum(Chan, <<"qq.24">>),

    Nodename1 = rabbit_data_coercion:to_list(get_node_config(Config, 1, nodename)),
    ?awaitMatch(#{members := [_, _]}, http_get(Config, "/queues/%2F/qq.24"), 30000),
    http_delete(Config, "/queues/quorum/%2F/qq.24/replicas/delete", ?ACCEPTED,
                [{node, Nodename1}]),
    ?awaitMatch(#{members := [_]}, http_get(Config, "/queues/%2F/qq.24"), 30000),

    Body = [{strategy, <<"all">>},
            {queue_pattern, <<"qq.24">>},
            {vhost_pattern, <<".*">>}],
    http_post(Config, "/queues/quorum/replicas/on/" ++ Nodename1 ++ "/grow",
              Body, ?NO_CONTENT),
    ?awaitMatch(#{members := [_, _]}, http_get(Config, "/queues/%2F/qq.24"), 30000),

    http_delete(Config, "/queues/%2F/qq.24", ?NO_CONTENT),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

qq_replicas_shrink(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare_quorum(Chan, <<"qq.25">>),
    ?awaitMatch(#{members := [_, _]}, http_get(Config, "/queues/%2F/qq.25"), 30000),

    ?awaitMatch(#{members := [_, _]}, http_get(Config, "/queues/%2F/qq.25"), 30000),
    Nodename1 = rabbit_data_coercion:to_list(get_node_config(Config, 1, nodename)),

    http_delete(Config, "/queues/quorum/replicas/on/" ++ Nodename1 ++ "/shrink",
                ?ACCEPTED),
    ?awaitMatch(#{members := [_]}, http_get(Config, "/queues/%2F/qq.25"), 30000),

    http_delete(Config, "/queues/%2F/qq.25", ?NO_CONTENT),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

queue_on_other_node(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    {ok, Chan2} = amqp_connection:open_channel(?config(conn, Config)),
    consume(Chan2, <<"some-queue">>),

    force_stats(Config),
    ?awaitMatch([_], maps:get(consumer_details, http_get(Config, "/queues/%2F/some-queue")), 60000),

    Res = http_get(Config, "/queues/%2F/some-queue"),
    % assert some basic data is present
    [Cons] = maps:get(consumer_details, Res),
    #{} = maps:get(channel_details, Cons), % channel details proplist must not be empty
    0 = maps:get(prefetch_count, Cons), % check one of the augmented properties
    <<"some-queue">> = maps:get(name, Res),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

queue_with_multiple_consumers(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Q = <<"multi-consumer-queue1">>,
    _ = queue_declare(Chan, Q),
    _ = wait_for_queue(Config, "/queues/%2F/multi-consumer-queue1"),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    consume(Chan,  Q),
    consume(Chan2, Q),
    publish(Chan2, Q),
    publish(Chan,  Q),
    % ensure a message has been consumed and acked
    receive
        {#'basic.deliver'{delivery_tag = T}, _} ->
            amqp_channel:cast(Chan, #'basic.ack'{delivery_tag = T})
    end,

    force_stats(Config),

    Res = http_get(Config, "/queues/%2F/multi-consumer-queue1"),
    http_delete(Config, "/queues/%2F/multi-consumer-queue1", ?NO_CONTENT),

    % assert some basic data is there
    [C1, C2] = maps:get(consumer_details, Res),
    % channel details proplist must not be empty
    #{} = maps:get(channel_details, C1),
    #{} = maps:get(channel_details, C2),
    % check one of the augmented properties
    0 = maps:get(prefetch_count, C1),
    0 = maps:get(prefetch_count, C2),
    Q = maps:get(name, Res),

    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

queue_consumer_cancelled(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    Tag = consume(Chan, <<"some-queue">>),

    #'basic.cancel_ok'{} =
         amqp_channel:call(Chan, #'basic.cancel'{consumer_tag = Tag}),
    force_stats(Config),
    Res = http_get(Config, "/queues/%2F/some-queue"),

    amqp_channel:close(Chan),

    % assert there are no consumer details
    [] = maps:get(consumer_details, Res),
    <<"some-queue">> = maps:get(name, Res),
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    ok.

queue_consumer_channel_closed(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    consume(Chan, <<"some-queue">>),
    force_stats(Config), % ensure channel stats have been written

    amqp_channel:close(Chan),
    force_stats(Config),

    ?awaitMatch([],
                %% assert there are no consumer details
                maps:get(consumer_details,
                         http_get(Config, "/queues/%2F/some-queue")),
                30000),
    ?awaitMatch(<<"some-queue">>,
                maps:get(name,
                         http_get(Config, "/queues/%2F/some-queue")),
                30000),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    ok.

queue(Config) ->
    http_put(Config, "/queues/%2F/some-queue", none, [?CREATED, ?NO_CONTENT]),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    {ok, Chan}  = amqp_connection:open_channel(?config(conn, Config)),
    {ok, Chan2} = amqp_connection:open_channel(?config(conn, Config)),

    publish(Chan, <<"some-queue">>),
    basic_get(Chan, <<"some-queue">>),
    publish(Chan2, <<"some-queue">>),
    basic_get(Chan2, <<"some-queue">>),
    force_stats(Config),

    % assert single queue is returned
    ?awaitMatch([#{} | _],
                maps:get(deliveries,
                         http_get(Config, "/queues/%2F/some-queue")),
                30000),
    
    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    ok.

queues_single(Config) ->
    http_put(Config, "/queues/%2F/some-queue", none, [?CREATED, ?NO_CONTENT]),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    force_stats(Config),
    Res = http_get(Config, "/queues/%2F"),
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    % assert at least one queue is returned
    ?assert(length(Res) >= 1),

    ok.

queues_multiple(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = queue_declare(Chan, <<"some-other-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),
    _ = wait_for_queue(Config, "/queues/%2F/some-other-queue"),

    force_stats(Config),

    Res = http_get(Config, "/queues/%2F"),
    [Q1, Q2 | _] = Res,

    % assert some basic data is present
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    http_delete(Config, "/queues/%2F/some-other-queue", ?NO_CONTENT),

    false = (maps:get(name, Q1) =:= maps:get(name, Q2)),
    amqp_channel:close(Chan),

    ok.

queues_removed(Config) ->
    http_put(Config, "/queues/%2F/some-queue", none, [?CREATED, ?NO_CONTENT]),
    force_stats(Config),
    N = length(http_get(Config, "/queues/%2F")),
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    force_stats(Config),
    ?assertEqual(N - 1, length(http_get(Config, "/queues/%2F"))),
    ok.

channels_multiple_on_different_nodes(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn2),
    consume(Chan, <<"some-queue">>),

    force_stats(Config),

    % assert two channels are present
    ?awaitMatch([_,_],
                http_get(Config, "/channels"),
                30000),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    rabbit_ct_client_helpers:close_connection(Conn),
    rabbit_ct_client_helpers:close_connection(Conn2),

    ok.

channel_closed(Config) ->
    {ok, Chan}  = amqp_connection:open_channel(?config(conn, Config)),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    {ok, Chan2} = amqp_connection:open_channel(?config(conn, Config)),
    force_stats(Config),

    consume(Chan2, <<"some-queue">>),
    amqp_channel:close(Chan),

    force_stats(Config),

    rabbit_ct_helpers:await_condition(
      fun() ->
              %% assert one channel is present
              length(http_get(Config, "/channels")) == 1
      end,
      60000),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    amqp_channel:close(Chan2),

    ok.

channel(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    [{_, ChData}] = rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list, [channel_created]),

    ChName = uri_string:recompose(#{path => binary_to_list(pget(name, ChData))}),

    force_stats(Config),
    Res = http_get(Config, "/channels/" ++ ChName ),
    % assert channel is non empty
    #{} = Res,

    amqp_channel:close(Chan),
    ok.

channel_other_node(Config) ->
    Q = <<"some-queue">>,
    http_put(Config, "/queues/%2F/some-queue", none, [?CREATED, ?NO_CONTENT]),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    [{_, ChData}] = rabbit_ct_broker_helpers:rpc(Config, 1, ets, tab2list,
                                                 [channel_created]),
    ChName = uri_string:recompose(#{path => binary_to_list(pget(name, ChData))}),
    consume(Chan, Q),
    publish(Chan, Q),

    wait_for_collect_statistics_interval(),
    force_stats(Config),

    Res = http_get(Config, "/channels/" ++ ChName ),
    % assert channel is non empty
    #{} = Res,
    [#{}] = maps:get(deliveries, Res),
    #{} = maps:get(connection_details, Res),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    amqp_connection:close(Conn),

    ok.

channel_with_consumer_on_other_node(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Q = <<"some-queue">>,
    _ = queue_declare(Chan, Q),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    ChName = get_channel_name(Config, 0),
    consume(Chan, Q),
    publish(Chan, Q),

    force_stats(Config),

    Res = http_get(Config, "/channels/" ++ ChName),
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    % assert channel is non empty
    #{} = Res,
    [#{}] = maps:get(consumer_details, Res),

    amqp_channel:close(Chan),

    ok.

channel_with_consumer_on_one_node(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Q = <<"some-queue">>,
    _ = queue_declare(Chan, Q),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    ChName = get_channel_name(Config, 0),
    consume(Chan, Q),

    force_stats(Config),

    Res = http_get(Config, "/channels/" ++ ChName),
    amqp_channel:close(Chan),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    % assert channel is non empty
    #{} = Res,
    [#{}] = maps:get(consumer_details, Res),
    ok.

consumers(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn2),
    consume(Chan, <<"some-queue">>),
    consume(Chan2, <<"some-queue">>),

    force_stats(Config),
    Res = http_get(Config, "/consumers"),

    % assert there are two non-empty consumer records
    [#{} = C1, #{} = C2] = Res,
    #{} = maps:get(channel_details, C1),
    #{} = maps:get(channel_details, C2),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    rabbit_ct_client_helpers:close_connection(Conn2),

    ok.


connections(Config) ->
    %% one connection is maintained by CT helpers
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),

    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, _Chan2} = amqp_connection:open_channel(Conn2),

    %% channel count needs a bit longer for 2nd chan
    wait_for_collect_statistics_interval(),
    force_stats(Config),

    Res = http_get(Config, "/connections"),

    % assert there are two non-empty connection records
    [#{} = C1, #{} = C2] = Res,
    1 = maps:get(channels, C1),
    1 = maps:get(channels, C2),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn2),

    ok.


exchanges(Config) ->
    {ok, _Chan0} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    QName = <<"exchanges-test">>,
    XName = <<"some-exchange">>,
    Q = queue_declare(Chan, QName),
    exchange_declare(Chan, XName),
    queue_bind(Chan, XName, Q, <<"some-key">>),
    consume(Chan, QName),
    publish_to(Chan, XName, <<"some-key">>),

    force_stats(Config),
    Res = http_get(Config, "/exchanges"),
    [X] = [X || X <- Res, maps:get(name, X) =:= XName],

    ?assertEqual(<<"direct">>, maps:get(type, X)),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.


exchange(Config) ->
    {ok, _Chan0} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    QName = <<"exchanges-test">>,
    XName = <<"some-other-exchange">>,
    Q = queue_declare(Chan, QName),
    exchange_declare(Chan, XName),
    queue_bind(Chan, XName, Q, <<"some-key">>),
    consume(Chan, QName),
    publish_to(Chan, XName, <<"some-key">>),

    force_stats(Config),
    force_stats(Config),
    Res = http_get(Config, "/exchanges/%2F/some-other-exchange"),

    ?assertEqual(<<"direct">>, maps:get(type, Res)),

    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

vhosts(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn2),
    publish(Chan2, <<"some-queue">>),

    wait_for_collect_statistics_interval(),
    force_stats(Config),
    Res = http_get(Config, "/vhosts"),

    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),
    % default vhost
    [#{} = Vhost] = Res,
    % assert vhost has some message stats
    #{} = maps:get(message_stats, Vhost),

    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

nodes(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare(Chan, <<"some-queue">>),
    _ = wait_for_queue(Config, "/queues/%2F/some-queue"),

    {ok, Chan2} = amqp_connection:open_channel(Conn),
    publish(Chan2, <<"some-queue">>),

    wait_for_collect_statistics_interval(),
    force_stats(Config),
    Res = http_get(Config, "/nodes"),
    http_delete(Config, "/queues/%2F/some-queue", ?NO_CONTENT),

    [#{} = N1 , #{} = N2] = Res,
    ?assert(is_binary(maps:get(name, N1))),
    ?assert(is_binary(maps:get(name, N2))),
    [#{} | _] = maps:get(cluster_links, N1),
    [#{} | _] = maps:get(cluster_links, N2),

    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    rabbit_ct_client_helpers:close_connection(Conn),

    ok.

overview(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    _ = queue_declare(Chan, <<"queue-n1">>),
    _ = queue_declare(Chan, <<"queue-n2">>),
    _ = wait_for_queue(Config, "/queues/%2F/queue-n1"),
    _ = wait_for_queue(Config, "/queues/%2F/queue-n2"),

    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn2),
    publish(Chan, <<"queue-n1">>),
    publish(Chan2, <<"queue-n2">>),
    wait_for_collect_statistics_interval(),
    force_stats(Config), % channel count needs a bit longer for 2nd chan
    Res = http_get(Config, "/overview"),

    http_delete(Config, "/queues/%2F/queue-n1", ?NO_CONTENT),
    http_delete(Config, "/queues/%2F/queue-n2", ?NO_CONTENT),
    % assert there are two non-empty connection records
    ObjTots = maps:get(object_totals, Res),
    ?assert(maps:get(connections, ObjTots) >= 2),
    ?assert(maps:get(channels, ObjTots) >= 2),
    #{} = QT = maps:get(queue_totals, Res),
    ?assert(maps:get(messages_ready, QT) >= 2),
    MS = maps:get(message_stats, Res),
    ?assert(maps:get(publish, MS) >= 2),
    ChurnRates = maps:get(churn_rates, Res),
    ?assertEqual(maps:get(queue_declared, ChurnRates), 2),
    ?assertEqual(maps:get(queue_created, ChurnRates), 2),
    ?assertEqual(maps:get(queue_deleted, ChurnRates), 0),
    ?assertEqual(maps:get(channel_created, ChurnRates), 2),
    ?assertEqual(maps:get(channel_closed, ChurnRates), 0),
    ?assertEqual(maps:get(connection_closed, ChurnRates), 0),

    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    rabbit_ct_client_helpers:close_connection(Conn),
    rabbit_ct_client_helpers:close_connection(Conn2),

    ok.

disable_plugin(Config) ->
    Node = get_node_config(Config, 0, nodename),
    Status0 = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit, status, []),
    Listeners0 = proplists:get_value(listeners, Status0),
    ?assert(lists:member(http, listener_protos(Listeners0))),
    rabbit_ct_broker_helpers:disable_plugin(Config, Node, 'rabbitmq_web_dispatch'),
    Status = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit, status, []),
    Listeners = proplists:get_value(listeners, Status),
    ?assert(not lists:member(http, listener_protos(Listeners))),
    rabbit_ct_broker_helpers:enable_plugin(Config, Node, 'rabbitmq_management').

%%----------------------------------------------------------------------------
%%

clear_all_table_data() ->
    [ets:delete_all_objects(T) || {T, _} <- ?CORE_TABLES],
    rabbit_mgmt_storage:reset(),
    [gen_server:call(P, purge_cache)
     || {_, P, _, _} <- supervisor:which_children(rabbit_mgmt_db_cache_sup)],
    send_to_all_collectors(purge_old_stats).

get_channel_name(Config, Node) ->
    [{_, ChData}|_] = rabbit_ct_broker_helpers:rpc(Config, Node, ets, tab2list,
                                                 [channel_created]),
    uri_string:recompose(#{path => binary_to_list(pget(name, ChData))}).

consume(Channel, Queue) ->
    #'basic.consume_ok'{consumer_tag = Tag} =
         amqp_channel:call(Channel, #'basic.consume'{queue = Queue}),
    Tag.

publish(Channel, Key) ->
    Payload = <<"foobar">>,
    Publish = #'basic.publish'{routing_key = Key},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}).

basic_get(Channel, Queue) ->
    Publish = #'basic.get'{queue = Queue},
    amqp_channel:call(Channel, Publish).

publish_to(Channel, Exchange, Key) ->
    Payload = <<"foobar">>,
    Publish = #'basic.publish'{routing_key = Key,
                               exchange = Exchange},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}).

exchange_declare(Chan, Name) ->
    Declare = #'exchange.declare'{exchange = Name},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare).

queue_declare(Chan) ->
    Declare = #'queue.declare'{},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Chan, Declare),
    Q.

queue_declare(Chan, Name) ->
    Declare = #'queue.declare'{queue = Name},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Chan, Declare),
    Q.

queue_declare_durable(Chan, Name) ->
    Declare = #'queue.declare'{queue = Name, durable = true, exclusive = false},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Chan, Declare),
    Q.

queue_declare_quorum(Chan, Name) ->
    Declare = #'queue.declare'{
        queue = Name,
        durable = true,
        arguments = [
            {<<"x-queue-type">>, longstr, <<"quorum">>}
        ]
    },
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Chan, Declare),
    Q.


queue_bind(Chan, Ex, Q, Key) ->
    Binding = #'queue.bind'{queue = Q,
                            exchange = Ex,
                            routing_key = Key},
    #'queue.bind_ok'{} = amqp_channel:call(Chan, Binding).

wait_for_queue(Config, Path) ->
    wait_for_queue(Config, Path, []).

wait_for_queue(Config, Path, Keys) ->
    wait_for_queue(Config, Path, Keys, 1000).

wait_for_queue(_Config, Path, Keys, 0) ->
    exit({timeout, {Path, Keys}});

wait_for_queue(Config, Path, Keys, Count) ->
    Res = http_get(Config, Path),
    case present(Keys, Res) of
        false -> timer:sleep(10),
                 wait_for_queue(Config, Path, Keys, Count - 1);
        true  -> Res
    end.

present([], _Res) ->
    true;
present(Keys, Res) ->
    lists:all(fun (Key) ->
                      X = maps:get(Key, Res, undefined),
                      X =/= [] andalso X =/= undefined
              end, Keys).

extract_node(N) ->
    list_to_atom(hd(string:tokens(binary_to_list(N), "@"))).

%% debugging utilities

trace_fun(Config, MFs) ->
    Nodename1 = get_node_config(Config, 0, nodename),
    Nodename2 = get_node_config(Config, 1, nodename),
    dbg:tracer(process, {fun(A,_) ->
                                 ct:pal(?LOW_IMPORTANCE,
                                        "TRACE: ~tp", [A])
                         end, ok}),
    dbg:n(Nodename1),
    dbg:n(Nodename2),
    dbg:p(all,c),
    [ dbg:tpl(M, F, cx) || {M, F} <- MFs],
    [ dbg:tpl(M, F, A, cx) || {M, F, A} <- MFs].

dump_table(Config, Table) ->
    Data = rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list, [Table]),
    ct:pal(?LOW_IMPORTANCE, "Node 0: Dump of table ~tp:~n~tp~n", [Table, Data]),
    Data0 = rabbit_ct_broker_helpers:rpc(Config, 1, ets, tab2list, [Table]),
    ct:pal(?LOW_IMPORTANCE, "Node 1: Dump of table ~tp:~n~tp~n", [Table, Data0]).

force_stats(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    force_all(Nodes),
    ok.

force_all(Nodes) ->
    lists:append(
      [begin
           ExtStats = {rabbit_mgmt_external_stats, N},
           ExtStats ! emit_update,
           [ExtStats |
            [begin
                 Name = {rabbit_mgmt_metrics_collector:name(Table), N},
                 Name ! collect_metrics,
                 Name
             end
             || {Table, _} <- ?CORE_TABLES]]
       end || N <- Nodes]).

send_to_all_collectors(Msg) ->
    [begin
          [{rabbit_mgmt_metrics_collector:name(Table), N} ! Msg
           || {Table, _} <- ?CORE_TABLES]
     end || N <- [node() | nodes()]].

listener_protos(Listeners) ->
  [listener_proto(L) || L <- Listeners].

listener_proto(#listener{protocol = Proto}) ->
  Proto;
listener_proto(Proto) when is_atom(Proto) ->
  Proto;
%% rabbit:status/0 used this formatting before rabbitmq/rabbitmq-cli#340
listener_proto({Proto, _Port, _Interface}) ->
  Proto.

wait_for_collect_statistics_interval() ->
    timer:sleep(?STATS_INTERVAL * 2).
