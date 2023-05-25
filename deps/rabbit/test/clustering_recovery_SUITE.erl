%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(clustering_recovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, mnesia_store}
    ].

groups() ->
    [{mnesia_store, [], [
                         {clustered_3_nodes, [],
                          [{cluster_size_3, [], [
                                                 force_shrink_quorum_queue,
                                                 force_shrink_all_quorum_queues
                                                ]}
                          ]}
                        ]}
    ].

suite() ->
    [
      %% If a testcase hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 5}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store, Config) ->
    Config;
init_per_group(clustered_3_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
        {keep_pid_file_on_exit, true}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

force_shrink_all_quorum_queues(Config) ->
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    
    QName1 = quorum_test_queue(1),
    QName2 = quorum_test_queue(2),
    QName3 = quorum_test_queue(3),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    declare_and_publish_to_queue(Config, Rabbit, QName1, Args),
    declare_and_publish_to_queue(Config, Rabbit, QName2, Args),
    declare_and_publish_to_queue(Config, Rabbit, QName3, Args),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertExit(
       {{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName1,
                                                   consumer_tag = <<"ctag">>},
                              self())),
    
    ok = rabbit_ct_broker_helpers:rpc(Config, Rabbit, rabbit_quorum_queue, force_all_queues_shrink_member_to_current_member, []),

    ok = consume_from_queue(Config, Rabbit, QName1),
    ok = consume_from_queue(Config, Rabbit, QName2),
    ok = consume_from_queue(Config, Rabbit, QName3),

    ok.

force_shrink_quorum_queue(Config) ->
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    
    QName1 = quorum_test_queue(1),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    declare_and_publish_to_queue(Config, Rabbit, QName1, Args),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertExit(
       {{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName1,
                                                   consumer_tag = <<"ctag">>},
                              self())),
    
    ok = rabbit_ct_broker_helpers:rpc(Config, Rabbit, rabbit_quorum_queue, force_shrink_member_to_current_member, [<<"/">>, QName1]),

    ok = consume_from_queue(Config, Rabbit, QName1),

    ok.

%% -------------------------------------------------------------------
%% Internal utils
%% -------------------------------------------------------------------
declare_and_publish_to_queue(Config, Node, QName, Args) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Node),
    declare(Ch, QName, Args),
    publish_many(Ch, QName, 10),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

quorum_test_queue(Number) ->
    list_to_binary(io_lib:format("quorum_queue_~p", [Number])).

declare(Ch, Name, Args) ->
    Res = amqp_channel:call(Ch, #'queue.declare'{durable = true,
                                                 queue   = Name,
                                                 arguments = Args}),
    amqp_channel:call(Ch, #'queue.bind'{queue    = Name,
                                        exchange = <<"amq.fanout">>}),
    Res.

consume_from_queue(Config, Node, QName) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Node),
    subscribe(Ch, QName),
    consume(10),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

publish_many(Ch, QName, N) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:cast(Ch, #'basic.publish'{routing_key = QName},
                       #amqp_msg{props = #'P_basic'{delivery_mode = 2}})
     || _ <- lists:seq(1, N)],
    amqp_channel:wait_for_confirms(Ch).

subscribe(Ch, QName) ->
    CTag = <<"ctag">>,
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName,
                                                consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            ok
    after 10000 ->
            exit(consume_ok_timeout)
    end.

consume(0) ->
    ok;
consume(N) ->
    receive
        {#'basic.deliver'{consumer_tag = <<"ctag">>}, _} ->
            consume(N - 1)
    after 10000 ->
            exit(deliver_timeout)
    end.
