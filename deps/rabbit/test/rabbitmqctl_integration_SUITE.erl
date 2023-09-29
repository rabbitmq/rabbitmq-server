%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbitmqctl_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-export([all/0
        ,groups/0
        ,init_per_suite/1
        ,end_per_suite/1
        ,init_per_group/2
        ,end_per_group/2
        ,init_per_testcase/2
        ,end_per_testcase/2
        ]).

-export([list_queues_local/1
        ,list_queues_offline/1
        ,list_queues_online/1
        ,list_queues_stopped/1
        ]).

all() ->
    [
        {group, list_queues}
    ].

groups() ->
    [
        {list_queues, [],
            [list_queues_local
            ,list_queues_online
            ,list_queues_offline
            ,list_queues_stopped
            ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(list_queues, Config0) ->
    NumNodes = 3,
    Config = create_n_node_cluster(Config0, NumNodes),
    case Config of
        {skip, _Reason} = Skip ->
            Skip;
        _ ->
            Config1 = declare_some_queues(Config),
            rabbit_ct_broker_helpers:stop_node(Config1, NumNodes - 1),
            Config1
    end;
init_per_group(_, Config) ->
    Config.

create_n_node_cluster(Config0, NumNodes) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodes_count, NumNodes},
                          {rmq_nodes_clustered, true}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

declare_some_queues(Config) ->
    Nodes = rabbit_ct_helpers:get_config(Config, rmq_nodes),
    PerNodeQueues = [ declare_some_queues(Config, NodeNum)
                      || NodeNum <- lists:seq(0, length(Nodes)-1) ],
    rabbit_ct_helpers:set_config(Config, {per_node_queues, PerNodeQueues}).

declare_some_queues(Config, NodeNum) ->
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, NodeNum),
    NumQueues = 5,
    Queues = [ list_to_binary(io_lib:format("queue-~b-on-node-~b", [QueueNum, NodeNum]))
               || QueueNum <- lists:seq(1, NumQueues) ],
    lists:foreach(fun (QueueName) ->
                          #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = QueueName, durable = true})
                  end, Queues),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan),
    Queues.

end_per_group(list_queues, Config0) ->
    Config1 = case rabbit_ct_helpers:get_config(Config0, save_config) of
        undefined -> Config0;
        C         -> C
    end,
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(list_queues_stopped, Config0) ->
    %% Start node 3 to crash it's queues
    rabbit_ct_broker_helpers:start_node(Config0, 2),
    %% Make vhost "down" on nodes 2 and 3
    ok = rabbit_ct_broker_helpers:force_vhost_failure(Config0, 1, <<"/">>),
    ok = rabbit_ct_broker_helpers:force_vhost_failure(Config0, 2, <<"/">>),

    rabbit_ct_broker_helpers:stop_node(Config0, 2),
    rabbit_ct_helpers:testcase_started(Config0, list_queues_stopped);

init_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_started(Config0, Testcase).

end_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_finished(Config0, Testcase).

%%----------------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------------
list_queues_local(Config) ->
    Node1Queues = lists:nth(1, ?config(per_node_queues, Config)),
    Node2Queues = lists:nth(2, ?config(per_node_queues, Config)),
    assert_ctl_queues(Config, 0, ["--local"], Node1Queues),
    assert_ctl_queues(Config, 1, ["--local"], Node2Queues),
    ok.

list_queues_online(Config) ->
    Node1Queues = lists:nth(1, ?config(per_node_queues, Config)),
    Node2Queues = lists:nth(2, ?config(per_node_queues, Config)),
    OnlineQueues = Node1Queues ++ Node2Queues,
    assert_ctl_queues(Config, 0, ["--online"], OnlineQueues),
    assert_ctl_queues(Config, 1, ["--online"], OnlineQueues),
    ok.

list_queues_offline(Config) ->
    Node3Queues = lists:nth(3, ?config(per_node_queues, Config)),
    OfflineQueues = Node3Queues,
    assert_ctl_queues(Config, 0, ["--offline"], OfflineQueues),
    assert_ctl_queues(Config, 1, ["--offline"], OfflineQueues),
    ok.

list_queues_stopped(Config) ->
    Node1Queues = lists:nth(1, ?config(per_node_queues, Config)),
    Node2Queues = lists:nth(2, ?config(per_node_queues, Config)),
    Node3Queues = lists:nth(3, ?config(per_node_queues, Config)),

    Expected =
        lists:sort([ {Q, <<"running">>} || Q <- Node1Queues ] ++
                       %% Node is running. Vhost is down
                       [ {Q, <<"stopped">>} || Q <- Node2Queues ] ++
                       %% Node is not running. Vhost is down
                       [ {Q, <<"down">>} || Q <- Node3Queues ]),

    ?awaitMatch(
       Expected,
       lists:sort(
         [ {Name, State}
           || [Name, State] <- rabbit_ct_broker_helpers:rabbitmqctl_list(Config, 0, ["list_queues", "name", "state", "--no-table-headers"]) ]),
       30_000).

%%----------------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------------
assert_ctl_queues(Config, Node, Args, Expected0) ->
    Expected = lists:sort(Expected0),
    ?awaitMatch(
       Expected,
       begin
           Got0 = run_list_queues(Config, Node, Args),
           lists:sort(lists:map(fun hd/1, Got0))
       end,
       30_000).

run_list_queues(Config, Node, Args) ->
    rabbit_ct_broker_helpers:rabbitmqctl_list(Config, Node, ["list_queues"] ++ Args ++ ["name", "--no-table-headers"]).
