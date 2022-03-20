%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_gc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mgmt_metrics.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [],
      [ queue_stats,
        quorum_queue_stats,
        connection_stats,
        channel_stats,
        vhost_stats,
        exchange_stats,
        node_stats,
        consumer_stats
      ]
     }
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbitmq_management_agent, [
                                             {metrics_gc_interval, 6000000},
                                             {rates_mode, detailed},
                                             {sample_retention_policies,
                                              %% List of {MaxAgeInSeconds, SampleEveryNSeconds}
                                              [{global,   [{605, 1}, {3660, 60}]},
                                               {basic,    [{605, 1}, {3600, 60}]},
                                               {detailed, [{605, 1}]}] }]}),
    rabbit_ct_helpers:merge_app_env(Config1,
                                    {rabbit, [
                                              {collect_statistics_interval, 100},
                                              {collect_statistics, fine}
                                             ]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE},
                                                    {rmq_nodes_count, 2},
                                                    {rmq_nodes_clustered, true}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      [ fun merge_app_env/1 ] ++ rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(quorum_queue_stats = Testcase, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            case rabbit_ct_broker_helpers:enable_feature_flag(Config, quorum_queue) of
                ok ->
                    rabbit_ct_helpers:testcase_started(Config, Testcase),
                    rabbit_ct_helpers:run_steps(
                    Config, rabbit_ct_client_helpers:setup_steps());
                {skip, _} = Skip ->
                    Skip;
                Other ->
                    {skip, Other}
            end
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps()).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

queue_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_stats">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_stats">>},
                      #amqp_msg{payload = <<"hello">>}),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Ch, #'basic.get'{queue = <<"queue_stats">>,
                                                                no_ack = true}),
    timer:sleep(1150),
    
    Q = q(<<"myqueue">>),
    X = x(<<"">>),    

    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_stats, {Q, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_stats_publish, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_msg_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_process_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_msg_rates, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_stats_deliver_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_exchange_stats_publish,
                                  {{{Q, X}, 5}, slide}]),
    
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_stats, Q]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_stats_publish, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_msg_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_process_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_msg_rates, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_stats_deliver_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_exchange_stats_publish, {{Q, X}, 5}]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),    

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_stats_publish]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_msg_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_process_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_msg_rates]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_stats_deliver_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_exchange_stats_publish]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_stats, Q]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_stats_publish, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_msg_stats, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_process_stats, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_msg_rates, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_stats_deliver_stats, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_exchange_stats_publish, {{Q, X}, 5}]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

quorum_queue_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    B = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(Ch,
                      #'queue.declare'{queue = <<"quorum_queue_stats">>,
                                       durable = true,
                                       arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    timer:sleep(1150),

    Q = q(<<"quorum_queue_stats">>),

    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_stats, {Q, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_msg_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_process_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [queue_msg_rates, {{Q, 5}, slide}]),

    rabbit_ct_broker_helpers:rpc(Config, B, ets, insert,
                                 [queue_stats, {Q, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, B, ets, insert,
                                 [queue_msg_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, B, ets, insert,
                                 [queue_process_stats, {{Q, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, B, ets, insert,
                                 [queue_msg_rates, {{Q, 5}, slide}]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_stats, Q]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_msg_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_process_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_msg_rates, {Q, 5}]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, B, ets, lookup,
                                       [queue_stats, Q]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, B, ets, lookup,
                                       [queue_msg_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, B, ets, lookup,
                                       [queue_process_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, B, ets, lookup,
                                       [queue_msg_rates, {Q, 5}]),

    {ok, _, {_, Leader}} = rabbit_ct_broker_helpers:rpc(Config, B, ra, members,
                                                        [{'%2F_quorum_queue_stats', B}]),
    [Follower] = [A, B] -- [Leader],

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, Leader, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, Leader, gen_server, call, [rabbit_mgmt_gc, test]),
    rabbit_ct_broker_helpers:rpc(Config, Follower, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, Follower, gen_server, call, [rabbit_mgmt_gc, test]),

    [] = rabbit_ct_broker_helpers:rpc(Config, Follower, ets, lookup,
                                      [queue_stats, Q]),
    [] = rabbit_ct_broker_helpers:rpc(Config, Follower, ets, lookup,
                                      [queue_msg_stats, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, Follower, ets, lookup,
                                      [queue_process_stats, {Q, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, Follower, ets, lookup,
                                      [queue_msg_rates, {Q, 5}]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, Leader, ets, lookup,
                                       [queue_stats, Q]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, Leader, ets, lookup,
                                       [queue_msg_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, Leader, ets, lookup,
                                       [queue_process_stats, {Q, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, Leader, ets, lookup,
                                       [queue_msg_rates, {Q, 5}]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"quorum_queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

connection_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_stats">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_stats">>},
                      #amqp_msg{payload = <<"hello">>}),
    timer:sleep(1150),
    
    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),

    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [connection_stats_coarse_conn_stats,
                                  {{DeadPid, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [connection_created_stats, {DeadPid, name, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [connection_stats, {DeadPid, infos}]),
    
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_stats_coarse_conn_stats, {DeadPid, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_created_stats, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_stats, DeadPid]),
    
    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),    

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_stats_coarse_conn_stats, {DeadPid, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_created_stats, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_stats, DeadPid]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [connection_stats_coarse_conn_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [connection_created_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [connection_stats]),
        
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

channel_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_stats">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_stats">>},
                      #amqp_msg{payload = <<"hello">>}),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Ch, #'basic.get'{queue = <<"queue_stats">>,
                                                                no_ack=true}),
    timer:sleep(1150),
    
    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),
    
    X = x(<<"myexchange">>),    
    
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_created_stats, {DeadPid, name, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_stats, {DeadPid, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_stats_fine_stats, {{DeadPid, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_exchange_stats_fine_stats,
                                  {{{DeadPid, X}, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_queue_stats_deliver_stats,
                                  {{{DeadPid, X}, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_stats_deliver_stats,
                                  {{DeadPid, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [channel_process_stats,
                                  {{DeadPid, 5}, slide}]),
    
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_created_stats, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_stats, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_stats_fine_stats, {DeadPid, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_exchange_stats_fine_stats,
                                        {{DeadPid, X}, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_queue_stats_deliver_stats,
                                        {{DeadPid, X}, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_stats_deliver_stats,
                                        {DeadPid, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_process_stats,
                                        {DeadPid, 5}]),
    
    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),    

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_created_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_stats_fine_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_exchange_stats_fine_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_queue_stats_deliver_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_stats_deliver_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [channel_process_stats]),
    
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_created_stats, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_stats, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_stats_fine_stats, {DeadPid, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_exchange_stats_fine_stats,
                                       {{DeadPid, X}, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_queue_stats_deliver_stats,
                                       {{DeadPid, X}, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_stats_deliver_stats,
                                       {DeadPid, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_process_stats,
                                       {DeadPid, 5}]),
    
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

vhost_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_stats">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_stats">>},
                      #amqp_msg{payload = <<"hello">>}),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Ch, #'basic.get'{queue = <<"queue_stats">>,
                                                                no_ack=true}),
    timer:sleep(1150),

    VHost = <<"myvhost">>,

    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [vhost_stats_coarse_conn_stats,
                                  {{VHost, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [vhost_stats_fine_stats,
                                  {{VHost, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [vhost_stats_deliver_stats,
                                  {{VHost, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [vhost_msg_stats,
                                                          {{VHost, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [vhost_msg_rates,
                                                          {{VHost, 5}, slide}]),
    
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_stats_coarse_conn_stats, {VHost, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_stats_fine_stats, {VHost, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_stats_deliver_stats, {VHost, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_msg_stats, {VHost, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_msg_rates, {VHost, 5}]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),    

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_stats_coarse_conn_stats, {VHost, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_stats_fine_stats, {VHost, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_stats_deliver_stats, {VHost, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_msg_stats, {VHost, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [vhost_msg_rates, {VHost, 5}]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [vhost_stats_coarse_conn_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [vhost_stats_fine_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [vhost_stats_deliver_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [vhost_msg_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [vhost_msg_rates]),
        
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

exchange_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_stats">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_stats">>},
                      #amqp_msg{payload = <<"hello">>}),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Ch, #'basic.get'{queue = <<"queue_stats">>,
                                                                no_ack=true}),
    timer:sleep(1150),

    Exchange = x(<<"myexchange">>),

    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [exchange_stats_publish_out,
                                  {{Exchange, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [exchange_stats_publish_in,
                                  {{Exchange, 5}, slide}]),
    
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [exchange_stats_publish_out, {Exchange, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [exchange_stats_publish_in, {Exchange, 5}]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),    

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [exchange_stats_publish_out, {Exchange, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [exchange_stats_publish_in, {Exchange, 5}]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [exchange_stats_publish_out]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [exchange_stats_publish_in]),
        
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

node_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    timer:sleep(150),

    Node = 'mynode',

    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [node_stats, {Node, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [node_coarse_stats,
                                                          {{Node, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [node_persister_stats,
                                                          {{Node, 5}, slide}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [node_node_stats,
                                                          {{A, Node}, infos}]),
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert, [node_node_coarse_stats,
                                                          {{{A, Node}, 5}, slide}]),
    
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_stats, Node]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_coarse_stats, {Node, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_persister_stats, {Node, 5}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_node_stats, {A, Node}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_node_coarse_stats, {{A, Node}, 5}]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),    

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [node_stats, Node]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [node_coarse_stats, {Node, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [node_persister_stats, {Node, 5}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_node_stats, {A, Node}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [node_node_coarse_stats, {{A, Node}, 5}]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [node_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [node_coarse_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [node_persister_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [node_node_stats]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [node_node_coarse_stats]),

    ok.

consumer_stats(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_stats">>}),
    amqp_channel:call(Ch, #'basic.consume'{queue = <<"queue_stats">>}),
    timer:sleep(1150),

    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),
    Q = q(<<"queue_stats">>),

    Id = {Q, DeadPid, tag},
    rabbit_ct_broker_helpers:rpc(Config, A, ets, insert,
                                 [consumer_stats, {Id, infos}]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup, [consumer_stats, Id]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_mgmt_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_mgmt_gc, test]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [consumer_stats]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup, [consumer_stats, Id]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_stats">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

dead_pid() ->
    spawn(fun() -> ok end).

q(Name) ->
    #resource{ virtual_host = <<"/">>,
               kind = queue,
               name = Name }.

x(Name) ->
    #resource{ virtual_host = <<"/">>,
               kind = exchange,
               name = Name }.
