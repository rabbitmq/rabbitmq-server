%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbitmqctl_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

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
        ,manage_global_parameters/1
        ]).

all() ->
    [
        {group, list_queues},
        {group, global_parameters}
    ].

groups() ->
    [
        {list_queues, [],
            [list_queues_local
            ,list_queues_online
            ,list_queues_offline
            ]},
        {global_parameters, [], [manage_global_parameters]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(list_queues, Config0) ->
    NumNodes = 3,
    Config = create_n_node_cluster(Config0, NumNodes),
    Config1 = declare_some_queues(Config),
    rabbit_ct_broker_helpers:stop_node(Config1, NumNodes - 1),
    Config1;
init_per_group(global_parameters,Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps());
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
end_per_group(global_parameters, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_started(Config0, Testcase).

end_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_finished(Config0, Testcase).

%%----------------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------------
list_queues_local(Config) ->
    Node1Queues = lists:sort(lists:nth(1, ?config(per_node_queues, Config))),
    Node2Queues = lists:sort(lists:nth(2, ?config(per_node_queues, Config))),
    assert_ctl_queues(Config, 0, ["--local"], Node1Queues),
    assert_ctl_queues(Config, 1, ["--local"], Node2Queues),
    ok.

list_queues_online(Config) ->
    Node1Queues = lists:sort(lists:nth(1, ?config(per_node_queues, Config))),
    Node2Queues = lists:sort(lists:nth(2, ?config(per_node_queues, Config))),
    OnlineQueues = Node1Queues ++ Node2Queues,
    assert_ctl_queues(Config, 0, ["--online"], OnlineQueues),
    assert_ctl_queues(Config, 1, ["--online"], OnlineQueues),
    ok.

list_queues_offline(Config) ->
    Node3Queues = lists:sort(lists:nth(3, ?config(per_node_queues, Config))),
    OfflineQueues = Node3Queues,
    assert_ctl_queues(Config, 0, ["--offline"], OfflineQueues),
    assert_ctl_queues(Config, 1, ["--offline"], OfflineQueues),
    ok.

manage_global_parameters(Config) ->
    0 = length(global_parameters(Config)),
    Parameter1Key = global_param1,
    GlobalParameter1ValueAsString = "{\"a\":\"b\", \"c\":\"d\"}",
    ok = control_action(Config, set_global_parameter,
        [atom_to_list(Parameter1Key),
            GlobalParameter1ValueAsString
    ]),

    1 = length(global_parameters(Config)),

    GlobalParameter1Value = rabbit_ct_broker_helpers:rpc(
        Config, 0,
        rabbit_runtime_parameters, value_global,
        [Parameter1Key]
    ),

    [{<<"a">>,<<"b">>}, {<<"c">>,<<"d">>}] = GlobalParameter1Value,

    Parameter2Key = global_param2,
    GlobalParameter2ValueAsString = "{\"e\":\"f\", \"g\":\"h\"}",
    ok = control_action(Config, set_global_parameter,
        [atom_to_list(Parameter2Key),
            GlobalParameter2ValueAsString
        ]),

    2 = length(global_parameters(Config)),

    GlobalParameter2Value = rabbit_ct_broker_helpers:rpc(
        Config, 0,
        rabbit_runtime_parameters, value_global,
        [Parameter2Key]
    ),

    [{<<"e">>,<<"f">>}, {<<"g">>,<<"h">>}] = GlobalParameter2Value,


    GlobalParameter1Value2AsString = "{\"a\":\"z\", \"c\":\"d\"}",
    ok = control_action(Config, set_global_parameter,
        [atom_to_list(Parameter1Key),
            GlobalParameter1Value2AsString
        ]),

    2 = length(global_parameters(Config)),

    GlobalParameter1Value2 = rabbit_ct_broker_helpers:rpc(
        Config, 0,
        rabbit_runtime_parameters, value_global,
        [Parameter1Key]
    ),

    [{<<"a">>,<<"z">>}, {<<"c">>,<<"d">>}] = GlobalParameter1Value2,

    ok = control_action(Config, clear_global_parameter,
        [atom_to_list(Parameter1Key)]
    ),

    1 = length(global_parameters(Config)),

    not_found = rabbit_ct_broker_helpers:rpc(
        Config, 0,
        rabbit_runtime_parameters, value_global,
        [Parameter1Key]
    ),

    ok = control_action(Config, list_global_parameters, []),

    ok.

%%----------------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------------
assert_ctl_queues(Config, Node, Args, Expected0) ->
    Expected = lists:sort(Expected0),
    Got0 = run_list_queues(Config, Node, Args),
    Got = lists:sort(lists:map(fun hd/1, Got0)),
    case Got of
        Expected ->
            ok;
        _ ->
            ct:pal(error, "Listing queues on node ~p failed. Expected:~n~p~n~nGot:~n~p~n~n",
                   [Node, Expected, Got]),
            exit({list_queues_unexpected_on, Node, Expected, Got})
    end.

run_list_queues(Config, Node, Args) ->
    rabbit_ct_broker_helpers:rabbitmqctl_list(Config, Node, ["list_queues"] ++ Args ++ ["name"]).

control_action(Config, Command, Args) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_control_main:action(
        Command, Node, Args, [],
        fun (Format, Args1) ->
            io:format(Format ++ " ...~n", Args1)
        end).

global_parameters(Config) ->
    rabbit_ct_broker_helpers:rpc(
        Config, 0,
        rabbit_runtime_parameters, list_global, []
    ).