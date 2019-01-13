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
%% Copyright (c) 2016-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(health_check_SUITE).

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

-export([ignores_remote_dead_channel/1
        ,detects_local_dead_channel/1
        ,ignores_remote_dead_queue/1
        ,detects_local_dead_queue/1
        ,ignores_remote_alarms/1
        ,detects_local_alarm/1
        ,honors_timeout_argument/1
        ,detects_stuck_local_node_monitor/1
        ,ignores_stuck_remote_node_monitor/1
        ]).

all() ->
    [{group, all_cases}].

groups() ->
    [{all_cases, [],
      [ignores_remote_dead_queue
      ,detects_local_dead_queue
      ,ignores_remote_dead_channel
      ,detects_local_dead_channel
      ,ignores_remote_alarms
      ,detects_local_alarm
      ,honors_timeout_argument
      ,detects_stuck_local_node_monitor
      ,ignores_stuck_remote_node_monitor
      ]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 2,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, ClusterSize},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%%----------------------------------------------------------------------------
%% Test cases
%%----------------------------------------------------------------------------
ignores_remote_dead_channel(Config) ->
    [A, B] = open_channel_and_declare_queue_everywhere(Config),
    CPid = suspend_single_channel(Config, B),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    resume_sys_process(Config, B, CPid),
    ok.

detects_local_dead_channel(Config) ->
    [A|_] = open_channel_and_declare_queue_everywhere(Config),
    CPid = suspend_single_channel(Config, A),
    {error, 75, Str} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    {match, _} = re:run(Str, "operation node_health_check.*timed out"),
    resume_sys_process(Config, A, CPid),
    ok.

ignores_remote_dead_queue(Config) ->
    [A, B] = open_channel_and_declare_queue_everywhere(Config),
    QPid = suspend_single_queue(Config, B),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    resume_sys_process(Config, B, QPid),
    ok.

detects_local_dead_queue(Config) ->
    [A|_] = open_channel_and_declare_queue_everywhere(Config),
    QPid = suspend_single_queue(Config, A),
    {error, 75, Str} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    {match, _} = re:run(Str, "operation node_health_check.*timed out"),
    resume_sys_process(Config, A, QPid),
    ok.

ignores_remote_alarms(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, B,
                                         ["set_vm_memory_high_watermark", "0.000000001"]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    ok.

detects_local_alarm(Config) ->
    [A|_] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, A,
                                         ["set_vm_memory_high_watermark", "0.000000001"]),
    {error, 70, Str} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    {match, _} = re:run(Str, "resource alarm.*in effect"),
    ok.

detects_stuck_local_node_monitor(Config) ->
    [A|_] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:rpc(Config, A, sys, suspend, [rabbit_node_monitor]),
    {error, 75, Str} = rabbit_ct_broker_helpers:rabbitmqctl(Config, A, ["-t", "5", "node_health_check"]),
    {match, _} = re:run(Str, "operation node_health_check.*timed out"),
    resume_sys_process(Config, A, rabbit_node_monitor),
    ok.

ignores_stuck_remote_node_monitor(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:rpc(Config, A, sys, suspend, [rabbit_node_monitor]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, B, ["-t", "5", "node_health_check"]),
    resume_sys_process(Config, A, rabbit_node_monitor),
    ok.

honors_timeout_argument(Config) ->
    [A|_] = open_channel_and_declare_queue_everywhere(Config),
    QPid = suspend_single_queue(Config, A),

    case timer:tc(rabbit_ct_broker_helpers, rabbitmqctl, [Config, A, ["-t", "5", "node_health_check"]]) of
        {TimeSpent, {error, 75, _}} ->
            if TimeSpent < 5000000  -> exit({too_fast, TimeSpent});
               TimeSpent > 10000000 -> exit({too_slow, TimeSpent}); %% +5 seconds for rabbitmqctl overhead
               true -> ok
            end;
        {_, Unexpected} ->
            exit({unexpected, Unexpected})
    end,
    resume_sys_process(Config, A, QPid),
    ok.

%%----------------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------------
open_channel_and_declare_queue_everywhere(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    lists:foreach(fun(Node) ->
                      Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
                      #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{})
                  end,
                  Nodes),
    Nodes.

suspend_single_queue(Config, Node) ->
    [QPid|_] = [rabbit_amqqueue:pid_of(Q) || Q <- rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue, list, []),
                                             Node == node(rabbit_amqqueue:pid_of(Q))],
    rabbit_ct_broker_helpers:rpc(Config, Node, sys, suspend, [QPid]),
    QPid.

suspend_single_channel(Config, Node) ->
    [CPid|_] = [Pid || Pid <- rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_channel, list_local, []),
                       Node == node(Pid)],
    rabbit_ct_broker_helpers:rpc(Config, Node, sys, suspend, [CPid]),
    CPid.

resume_sys_process(Config, Node, Pid) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, sys, resume, [Pid]).
