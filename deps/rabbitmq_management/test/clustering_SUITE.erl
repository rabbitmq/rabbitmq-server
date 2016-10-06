%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(clustering_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_core_metrics.hrl").
-include("rabbit_mgmt_metrics.hrl").
-include("rabbit_mgmt_test.hrl").

-import(rabbit_ct_broker_helpers, [get_node_config/3, restart_node/2]).
-import(rabbit_mgmt_test_util, [http_get/2, http_put/4, http_delete/3]).
-import(rabbit_misc, [pget/2]).

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [{non_parallel_tests, [], [
                               list_cluster_nodes_test,
                               multi_node_case1_test,
                               ha_queue_hosted_on_other_node,
                               queue_on_other_node,
                               ha_queue_with_multiple_consumers,
                               queue_with_multiple_consumers,
                               queue_consumer_cancelled,
                               queue_consumer_channel_closed,
                               queue,
                               queues_single,
                               queues_multiple,
                               queues_removed,
                               channels_multiple_on_different_nodes,
                               channels_cancelled,
                               channel,
                               channel_other_node,
                               channel_with_consumer_on_other_node,
                               channel_with_consumer_on_same_node,
                               consumers,
                               connections,
                               exchanges,
                               exchange,
                               vhosts,
                               nodes,
                               overview,
                               overview_perf
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics, fine},
                                              {collect_statistics_interval, 500}
                                             ]}),
    rabbit_ct_helpers:merge_app_env(Config1,
                                    {rabbitmq_management, [
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

init_per_testcase(multi_node_case1_test = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, clear_all_table_data, []),
    rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE, clear_all_table_data, []),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Config1 = rabbit_ct_helpers:set_config(Config, {conn, Conn}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(multi_node_case1_test = Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:close_connection(?config(conn, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

list_cluster_nodes_test(Config) ->
    %% see rmq_nodes_count in init_per_suite
    ?assertEqual(2, length(http_get(Config, "/nodes"))),
    passed.

multi_node_case1_test(Config) ->
    Nodename1 = get_node_config(Config, 0, nodename),
    Nodename2 = get_node_config(Config, 1, nodename),
    Policy = [{pattern,    <<".*">>},
              {definition, [{'ha-mode', <<"all">>}]}],
    http_put(Config, "/policies/%2f/HA", Policy, ?CREATED),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/ha-queue", QArgs, ?CREATED),
    Q = wait_for(Config, "/queues/%2f/ha-queue"),
    assert_node(Nodename2, pget(node, Q)),
    assert_single_node(Nodename1, pget(slave_nodes, Q)),
    assert_single_node(Nodename1, pget(synchronised_slave_nodes, Q)),
    %% restart node2
    restart_node(Config, 1),

    Q2 = wait_for(Config, "/queues/%2f/ha-queue"),
    http_delete(Config, "/queues/%2f/ha-queue", ?NO_CONTENT),
    http_delete(Config, "/policies/%2f/HA", ?NO_CONTENT),
    assert_node(Nodename1, pget(node, Q2)),
    assert_single_node(Nodename2, pget(slave_nodes, Q2)),
    assert_single_node(Nodename2, pget(synchronised_slave_nodes, Q2)),

    passed.

ha_queue_hosted_on_other_node(Config) ->
    % create ha queue on node 2
    Nodename2 = get_node_config(Config, 1, nodename),
    Policy = [{pattern,    <<".*">>},
              {definition, [{'ha-mode', <<"all">>}]}],
    http_put(Config, "/policies/%2f/HA", Policy, ?CREATED),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/ha-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    consume(Chan, <<"ha-queue">>),

    force_stats(),
    Res = http_get(Config, "/queues/%2f/ha-queue"),
    amqp_channel:close(Chan),
    http_delete(Config, "/queues/%2f/ha-queue", ?NO_CONTENT),
    http_delete(Config, "/policies/%2f/HA", ?NO_CONTENT),
    % assert some basic data is there
    [Cons] = pget(consumer_details, Res),
    [_|_] = pget(channel_details, Cons), % channel details proplist must not be empty
    0 = pget(prefetch_count, Cons), % check one of the augmented properties
    <<"ha-queue">> = pget(name, Res),
    ok.

ha_queue_with_multiple_consumers(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    Policy = [{pattern,    <<".*">>},
              {definition, [{'ha-mode', <<"all">>}]}],
    http_put(Config, "/policies/%2f/HA", Policy, ?CREATED),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/ha-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    consume(Chan, <<"ha-queue">>),

    force_stats(),
    consume(Chan, <<"ha-queue">>),
    force_stats(),
    Res = http_get(Config, "/queues/%2f/ha-queue"),
    amqp_channel:close(Chan),
    http_delete(Config, "/queues/%2f/ha-queue", ?NO_CONTENT),
    http_delete(Config, "/policies/%2f/HA", ?NO_CONTENT),
    % assert some basic data is there
    [C1, C2] = pget(consumer_details, Res),
    % channel details proplist must not be empty
    [_|_] = pget(channel_details, C1),
    [_|_] = pget(channel_details, C2),
    % check one of the augmented properties
    0 = pget(prefetch_count, C1),
    0 = pget(prefetch_count, C2),
    <<"ha-queue">> = pget(name, Res),
    ok.

queue_on_other_node(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    consume(Chan, <<"some-queue">>),

    force_stats(),
    Res = http_get(Config, "/queues/%2f/some-queue"),
    amqp_channel:close(Chan),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert some basic data is present
    [Cons] = pget(consumer_details, Res),
    [_|_] = pget(channel_details, Cons), % channel details proplist must not be empty
    0 = pget(prefetch_count, Cons), % check one of the augmented properties
    <<"some-queue">> = pget(name, Res),
    ok.

queue_with_multiple_consumers(Config) ->
    Nodename1 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename1))}],
    http_put(Config, "/queues/%2f/ha-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    consume(Chan, <<"ha-queue">>),
    consume(Chan2, <<"ha-queue">>),
    publish(Chan2, <<"ha-queue">>),
    publish(Chan, <<"ha-queue">>),
    % ensure a message has been consumed and acked
    receive
        {#'basic.deliver'{delivery_tag = T}, _} ->
            amqp_channel:cast(Chan, #'basic.ack'{delivery_tag = T})
    end,
    force_stats(),
    Res = http_get(Config, "/queues/%2f/ha-queue"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/ha-queue", ?NO_CONTENT),
    % assert some basic data is there
    [C1, C2] = pget(consumer_details, Res),
    % channel details proplist must not be empty
    [_|_] = pget(channel_details, C1),
    [_|_] = pget(channel_details, C2),
    % check one of the augmented properties
    0 = pget(prefetch_count, C1),
    0 = pget(prefetch_count, C2),
    <<"ha-queue">> = pget(name, Res),
    ok.

force_stats() ->
    force_all(),
    timer:sleep(1000).

force_all() ->
    [begin
          {rabbit_mgmt_external_stats, N} ! emit_update,
          timer:sleep(100),
          [{rabbit_mgmt_metrics_collector:name(Table), N} ! collect_metrics
           || {Table, _} <- ?CORE_TABLES]
     end
     || N <- [node() | nodes()]].

queue_consumer_cancelled(Config) ->
    % create queue on node 2
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Tag = consume(Chan, <<"some-queue">>),

    #'basic.cancel_ok'{} =
         amqp_channel:call(Chan, #'basic.cancel'{consumer_tag = Tag}),

    force_stats(),
    Res = http_get(Config, "/queues/%2f/some-queue"),
    % assert there are no consumer details
    [] = pget(consumer_details, Res),
    <<"some-queue">> = pget(name, Res),
    amqp_channel:close(Chan),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    ok.

queue_consumer_channel_closed(Config) ->
    % create queue on node 2
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    consume(Chan, <<"some-queue">>),
    force_stats(), % ensure channel stats have been written
    amqp_channel:close(Chan),
    force_stats(),
    Res = http_get(Config, "/queues/%2f/some-queue"),
    % assert there are no consumer details
    [] = pget(consumer_details, Res),
    <<"some-queue">> = pget(name, Res),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    ok.

queue(Config) ->
    % Nodename2 = get_node_config(Config, 1, nodename),
    % QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", [], ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    publish(Chan, <<"some-queue">>),
    basic_get(Chan, <<"some-queue">>),
    publish(Chan2, <<"some-queue">>),
    basic_get(Chan2, <<"some-queue">>),
    force_stats(),
    timer:sleep(10000),
    dump_table(Config, channel_queue_metrics),
    Res = http_get(Config, "/queues/%2f/some-queue"),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert single queue is returned
    [_|_] = pget(deliveries, Res),
    ok.

queues_single(Config) ->
    http_put(Config, "/queues/%2f/some-queue", [], ?CREATED),
    trace_fun(Config, [{delegate, invoke},
                       {rabbit_mgmt_db, cached_delegate_invoke},
                       {rabbit_mgmt_db, cache_lookup}]),
    force_stats(),
    Res = http_get(Config, "/queues/%2f"),
    _Res = http_get(Config, "/queues/%2f"),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert single queue is returned
    [_] = Res,
    ok.

queues_multiple(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", [], ?CREATED),
    http_put(Config, "/queues/%2f/some-other-queue", QArgs, ?CREATED),

    trace_fun(Config, [{rabbit_mgmt_db, submit_cached}]),
    force_stats(),
    Res = http_get(Config, "/queues/%2f"),
    % assert some basic data is present
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/some-other-queue", ?NO_CONTENT),
    % assert two non-empty queues are returned
    [[_|_] = Q1, [_|_] = Q2] = Res,
    false = (rabbit_misc:pget(name, Q1) =:= rabbit_misc:pget(name,Q2)),
    ok.

queues_removed(Config) ->
    http_put(Config, "/queues/%2f/some-queue", [], ?CREATED),
    force_stats(),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    force_stats(),
    Res = http_get(Config, "/queues/%2f"),
    % assert single queue is returned
    [] = Res,
    ok.

channels_multiple_on_different_nodes(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    consume(Chan, <<"some-queue">>),

    force_stats(),
    Res = http_get(Config, "/channels"),
    amqp_channel:close(Chan),
    amqp_channel:close(Chan2),
    amqp_connection:close(Conn),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert two channels are present
    [_,_] = Res,
    ok.

channels_cancelled(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    consume(Chan, <<"some-queue">>),
    force_stats(),
    amqp_channel:close(Chan2),
    amqp_connection:close(Conn),
    force_stats(),

    Res = http_get(Config, "/channels"),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    amqp_channel:close(Chan),
    % assert one channel is present
    [_] = Res,
    ok.

channel(Config) ->
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    [{_, ChData}] = rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list, [channel_created]),

    ChName = http_uri:encode(binary_to_list(pget(name, ChData))),
    force_stats(),
    Res = http_get(Config, "/channels/" ++ ChName ),
    amqp_channel:close(Chan),
    % assert channel is non empty
    [_|_] = Res,
    ok.

channel_other_node(Config) ->
    http_put(Config, "/queues/%2f/some-queue", [], ?CREATED),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    [{_, ChData}] = rabbit_ct_broker_helpers:rpc(Config, 1, ets, tab2list,
                                                 [channel_created]),
    ChName = http_uri:encode(binary_to_list(pget(name, ChData))),
    consume(Chan, <<"some-queue">>),
    publish(Chan, <<"some-queue">>),
    force_stats(),
    force_stats(),
    Res = http_get(Config, "/channels/" ++ ChName ),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    amqp_connection:close(Conn),
    % assert channel is non empty
    [_|_] = Res,
    [_|_] = pget(deliveries, Res),
    ok.

channel_with_consumer_on_other_node(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    ChName = get_channel_name(Config, 0),
    consume(Chan, <<"some-queue">>),
    publish(Chan, <<"some-queue">>),
    force_stats(),
    Res = http_get(Config, "/channels/" ++ ChName ),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    amqp_channel:close(Chan),
    % assert channel is non empty
    [_|_] = Res,
    [_] = pget(consumer_details, Res),
    ok.

channel_with_consumer_on_same_node(Config) ->
    Nodename1 = get_node_config(Config, 0, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename1))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    ChName = get_channel_name(Config, 0),
    consume(Chan, <<"some-queue">>),
    force_stats(),
    Res = http_get(Config, "/channels/" ++ ChName ),
    amqp_channel:close(Chan),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert channel is non empty
    [_|_] = Res,
    [_] = pget(consumer_details, Res),
    ok.

consumers(Config) ->
    Nodename2 = get_node_config(Config, 0, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    consume(Chan, <<"some-queue">>),
    consume(Chan2, <<"some-queue">>),
    force_stats(),
    Res = http_get(Config, "/consumers"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert there are two non-empty consumer records
    [[_|_] = C1,[_|_] = C2] = Res,
    [_|_] = pget(channel_details, C1),
    [_|_] = pget(channel_details, C2),
    ok.


connections(Config) ->
    Nodename2 = get_node_config(Config, 0, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, _Chan2} = amqp_connection:open_channel(Conn),
    force_stats(),
    force_stats(), % channel count needs a bit longer for 2nd chan
    Res = http_get(Config, "/connections"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % assert there are two non-empty connection records
    [[_|_] = Q1,[_|_] = Q2] = Res,
    1 = pget(channels, Q1),
    1 = pget(channels, Q2),
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

    force_stats(),
    Res = http_get(Config, "/exchanges"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    [X] = [X || X <- Res, pget(name, X) =:= XName],

    % assert exchange has some message stats
    [_|_] = pget(message_stats, X),
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

    force_stats(),
    force_stats(),
    Res = http_get(Config, "/exchanges/%2F/some-other-exchange"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),

    % assert exchange has some message stats
    [_|_] = pget(message_stats, Res),
    [_|_] = pget(incoming, Res),
    ok.

vhosts(Config) ->
    Nodename2 = get_node_config(Config, 0, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    publish(Chan2, <<"some-queue">>),
    force_stats(),
    % TODO:  force channels to emit stats instead of waiting
    force_stats(),
    Res = http_get(Config, "/vhosts"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),
    % default vhost
    [[_|_] = Vhost] = Res,
    % assert vhost has some message stats
    [_|_] = pget(message_stats, Vhost),
    ok.

nodes(Config) ->
    Nodename2 = get_node_config(Config, 0, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/some-queue", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    publish(Chan2, <<"some-queue">>),
    force_stats(),
    Res = http_get(Config, "/nodes"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/some-queue", ?NO_CONTENT),

    [[_|_] = N1 , [_|_] = N2] = Res,
    % assert each node has some processors
    ?assert(undefined =/= pget(processors, N1)),
    ?assert(undefined =/= pget(processors, N2)),
    [_|_] = pget(cluster_links, N1),
    [_|_] = pget(cluster_links, N2),
    ok.

overview_perf(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/queue-n1", [], ?CREATED),
    http_put(Config, "/queues/%2f/queue-n2", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    [ queue_declare(Chan) || _ <- lists:seq(0,999) ],
    [ queue_declare(Chan2) || _ <- lists:seq(0,999) ],
    publish(Chan, <<"queue-n1">>),
    publish(Chan2, <<"queue-n2">>),
    timer:sleep(5000), % TODO force stat emission
    force_stats(), % channel count needs a bit longer for 2nd chan

    % {Time, _Res} = timer:tc(fun () ->
    %                                [ http_get(Config, "/queues?page=1&page_size=100&name=&use_regex=false&pagination=true") || _ <- lists:seq(0, 99) ] end),
    {Time, _Res} = timer:tc(fun () ->
                                   [ http_get(Config, "/overview") || _ <- lists:seq(0, 99) ] end),
    ct:pal("Time: ~p", [Time]).


overview(Config) ->
    Nodename2 = get_node_config(Config, 1, nodename),
    QArgs = [{node, list_to_binary(atom_to_list(Nodename2))}],
    http_put(Config, "/queues/%2f/queue-n1", [], ?CREATED),
    http_put(Config, "/queues/%2f/queue-n2", QArgs, ?CREATED),
    {ok, Chan} = amqp_connection:open_channel(?config(conn, Config)),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1),
    {ok, Chan2} = amqp_connection:open_channel(Conn),
    publish(Chan, <<"queue-n1">>),
    publish(Chan2, <<"queue-n2">>),
    timer:sleep(5000), % TODO force stat emission
    force_stats(), % channel count needs a bit longer for 2nd chan
    Res = http_get(Config, "/overview"),
    amqp_channel:close(Chan),
    rabbit_ct_client_helpers:close_connection(Conn),
    http_delete(Config, "/queues/%2f/queue-n1", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/queue-n2", ?NO_CONTENT),
    % assert there are two non-empty connection records
    ObjTots = pget(object_totals, Res),
    2 = pget(connections, ObjTots),
    2 = pget(channels, ObjTots),
    [_|_] = QT = pget(queue_totals, Res),
    2 = pget(messages_ready, QT),
    MS = pget(message_stats, Res),
    2 = pget(publish, MS),
    ok.

%%----------------------------------------------------------------------------
%%

clear_all_table_data() ->
    [ets:delete_all_objects(T) || {T, _} <- ?CORE_TABLES],
    [ets:delete_all_objects(T) || {T, _} <- ?TABLES],
    ets:delete_all_objects(rabbit_mgmt_db_cache),
    N = rabbit_mgmt_db_cache:process_name(queues),
    gen_server:call(N, purge_cache).

get_channel_name(Config, Node) ->
    [{_, ChData}|_] = rabbit_ct_broker_helpers:rpc(Config, Node, ets, tab2list,
                                                 [channel_created]),
    http_uri:encode(binary_to_list(pget(name, ChData))).

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

queue_bind(Chan, Ex, Q, Key) ->
    Binding = #'queue.bind'{queue = Q,
                            exchange = Ex,
                            routing_key = Key},
    #'queue.bind_ok'{} = amqp_channel:call(Chan, Binding).

wait_for(Config, Path) ->
    wait_for(Config, Path, [slave_nodes, synchronised_slave_nodes]).

wait_for(Config, Path, Keys) ->
    wait_for(Config, Path, Keys, 1000).

wait_for(_Config, Path, Keys, 0) ->
    exit({timeout, {Path, Keys}});

wait_for(Config, Path, Keys, Count) ->
    Res = http_get(Config, Path),
    case present(Keys, Res) of
        false -> timer:sleep(10),
                 wait_for(Config, Path, Keys, Count - 1);
        true  -> Res
    end.

present(Keys, Res) ->
    lists:all(fun (Key) ->
                      X = pget(Key, Res),
                      X =/= [] andalso X =/= undefined
              end, Keys).

assert_single_node(Exp, Act) ->
    ?assertEqual(1, length(Act)),
    assert_node(Exp, hd(Act)).

assert_nodes(Exp, Act0) ->
    Act = [extract_node(A) || A <- Act0],
    ?assertEqual(length(Exp), length(Act)),
    [?assert(lists:member(E, Act)) || E <- Exp].

assert_node(Exp, Act) ->
    ?assertEqual(Exp, list_to_atom(binary_to_list(Act))).

extract_node(N) ->
    list_to_atom(hd(string:tokens(binary_to_list(N), "@"))).

%% debugging utilities

trace_fun(Config, MFs) ->
    Nodename1 = get_node_config(Config, 0, nodename),
    Nodename2 = get_node_config(Config, 1, nodename),
    dbg:tracer(process, {fun(A,_) ->
                                 ct:pal(?LOW_IMPORTANCE,
                                        "TRACE: ~p", [A])
                         end, ok}),
    dbg:n(Nodename1),
    dbg:n(Nodename2),
    dbg:p(all,c),
    [ dbg:tpl(M, F, cx) || {M, F} <- MFs],
    [ dbg:tpl(M, F, A, cx) || {M, F, A} <- MFs].

dump_table(Config, Table) ->
    Data = rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list, [Table]),
    ct:pal(?LOW_IMPORTANCE, "Node 0: Dump of table ~p:~n~p~n", [Table, Data]),
    Data0 = rabbit_ct_broker_helpers:rpc(Config, 1, ets, tab2list, [Table]),
    ct:pal(?LOW_IMPORTANCE, "Node 1: Dump of table ~p:~n~p~n", [Table, Data0]).

