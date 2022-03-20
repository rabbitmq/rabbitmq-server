%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbitmqctl_shutdown_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
        {group, running_node},
        {group, non_running_node}
    ].

groups() ->
    [
        {running_node, [], [
            successful_shutdown
        ]},
        {non_running_node, [], [
            nothing_to_shutdown
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(running_node, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {need_start, true}
    ]);
init_per_group(non_running_node, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]).

end_per_group(running_node, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, []);
end_per_group(non_running_node, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, []).

init_per_testcase(Testcase, Config0) ->
    Config1 = case ?config(need_start, Config0) of
        true ->
            rabbit_ct_helpers:run_setup_steps(Config0,
                rabbit_ct_broker_helpers:setup_steps() ++
                [fun save_node/1]);
        _ ->
            rabbit_ct_helpers:set_config(Config0,
                [{node, non_existent_node@localhost}])
    end,
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config0) ->
    Config1 = case ?config(need_start, Config0) of
        true ->
            rabbit_ct_helpers:run_teardown_steps(Config0,
                rabbit_ct_broker_helpers:teardown_steps());
        _ -> Config0
    end,
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

save_node(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_helpers:set_config(Config, [{node, Node}]).

successful_shutdown(Config) ->
    Node = ?config(node, Config),
    Pid = node_pid(Node),
    ok = shutdown_ok(Node),
    false = erlang_pid_is_running(Pid),
    false = node_is_running(Node).


nothing_to_shutdown(Config) ->
    Node = ?config(node, Config),

    { error, 69, _ } =
        rabbit_ct_broker_helpers:control_action(shutdown, Node, []).

node_pid(Node) ->
    Val = rpc:call(Node, os, getpid, []),
    true = is_list(Val),
    list_to_integer(Val).

erlang_pid_is_running(Pid) ->
    rabbit_misc:is_os_process_alive(integer_to_list(Pid)).

node_is_running(Node) ->
    net_adm:ping(Node) == pong.

shutdown_ok(Node) ->
    %% Start a command
    {stream, Stream} = rabbit_ct_broker_helpers:control_action(shutdown, Node, []),
    %% Execute command steps. Each step will output a binary string
    Lines = 'Elixir.Enum':to_list(Stream),
    ct:pal("Command output ~p ~n", [Lines]),
    [true = is_binary(Line) || Line <- Lines],
    ok.
