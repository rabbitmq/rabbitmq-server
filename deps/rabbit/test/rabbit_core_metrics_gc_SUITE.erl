%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_core_metrics_gc_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [],
      [ queue_metrics,
        connection_metrics,
        channel_metrics,
        node_metrics,
        gen_server2_metrics,
        consumer_metrics
      ]
     }
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    AppEnv = {rabbit, [{core_metrics_gc_interval, 6000000},
                       {collect_statistics_interval, 100},
                       {collect_statistics, fine}]},
    rabbit_ct_helpers:merge_app_env(Config, AppEnv).

init_per_group(non_parallel_tests, Config) ->
    rabbit_ct_helpers:log_environment(),
    Conf = [{rmq_nodename_suffix, non_parallel_tests}],
    Config1 = rabbit_ct_helpers:set_config(Config, Conf),
    rabbit_ct_helpers:run_setup_steps(Config1, setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps()).

setup_steps() ->
    [ fun merge_app_env/1 ] ++ rabbit_ct_broker_helpers:setup_steps().

%% -------------------------------------------------------------------
%% Single-node Testcases.
%% -------------------------------------------------------------------

queue_metrics(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_metrics">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_metrics">>},
                      #amqp_msg{payload = <<"hello">>}),
    timer:sleep(150),

    Q = q(<<"myqueue">>),

    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics, queue_stats,
                                 [Q, infos]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics, queue_stats,
                                 [Q, 1, 1, 1, 1]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_metrics, Q]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [queue_coarse_metrics, Q]),
    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_core_metrics_gc, test]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_metrics]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list,
                                         [queue_coarse_metrics]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_metrics, Q]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [queue_coarse_metrics, Q]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_metrics">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

connection_metrics(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_metrics">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_metrics">>},
                      #amqp_msg{payload = <<"hello">>}),
    timer:sleep(200),

    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),

    Infos = [{info0, foo}, {info1, bar}, {info2, baz},
             {authz_backends, [rabbit_auth_backend_oauth2,rabbit_auth_backend_http]}],

    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 connection_created, [DeadPid, Infos]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 connection_stats, [DeadPid, Infos]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 connection_stats, [DeadPid, 1, 1, 1]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_created, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_metrics, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_coarse_metrics, DeadPid]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_core_metrics_gc, test]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_created, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_metrics, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [connection_coarse_metrics, DeadPid]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [connection_created]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [connection_metrics]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [connection_coarse_metrics]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_metrics">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

channel_metrics(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_metrics">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"queue_metrics">>},
                      #amqp_msg{payload = <<"hello">>}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = <<"won't route $¢% anywhere">>},
                      #amqp_msg{payload = <<"hello">>}),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Ch, #'basic.get'{queue = <<"queue_metrics">>,
                                                                no_ack=true}),
    timer:sleep(150),

    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),

    Q = q(<<"myqueue">>),
    X = x(<<"myexchange">>),


    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 channel_created, [DeadPid, infos]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 channel_stats, [DeadPid, infos]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 channel_stats, [reductions, DeadPid, 1]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 channel_stats, [exchange_stats, publish,
                                                 {DeadPid, X}, 1]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 channel_stats, [queue_stats, get,
                                                 {DeadPid, Q}, 1]),
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 channel_stats, [queue_exchange_stats, publish,
                                                 {DeadPid, {Q, X}}, 1]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_created, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_metrics, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_process_metrics, DeadPid]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_exchange_metrics, {DeadPid, X}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_queue_metrics, {DeadPid, Q}]),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [channel_queue_exchange_metrics, {DeadPid, {Q, X}}]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_core_metrics_gc, test]),


    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [channel_created]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [channel_metrics]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [channel_process_metrics]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [channel_exchange_metrics]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [channel_queue_metrics]),
    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [channel_queue_exchange_metrics]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_created, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_metrics, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_process_metrics, DeadPid]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_exchange_metrics, {DeadPid, X}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_queue_metrics, {DeadPid, Q}]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [channel_queue_exchange_metrics, {DeadPid, {Q, X}}]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_metrics">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    ok.

node_metrics(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics, node_node_stats,
                                 [{node(), 'deer@localhost'}, infos]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                       [node_node_metrics, {node(), 'deer@localhost'}]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_core_metrics_gc, test]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [node_node_metrics, {node(), 'deer@localhost'}]),

    ok.

gen_server2_metrics(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),

    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics, gen_server2_stats,
                                 [DeadPid, 1]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [gen_server2_metrics, DeadPid]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_core_metrics_gc, test]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [gen_server2_metrics]),

    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup,
                                      [gen_server2_metrics, DeadPid]),

    ok.

consumer_metrics(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(Ch, #'queue.declare'{queue = <<"queue_metrics">>}),
    amqp_channel:call(Ch, #'basic.consume'{queue = <<"queue_metrics">>}),
    timer:sleep(200),

    DeadPid = rabbit_ct_broker_helpers:rpc(Config, A, ?MODULE, dead_pid, []),

    QName = q(<<"queue_metrics">>),
    CTag = <<"tag">>,
    rabbit_ct_broker_helpers:rpc(Config, A, rabbit_core_metrics,
                                 consumer_created, [DeadPid, CTag, true, true,
                                                    QName, 1, false, waiting, []]),
    Id = {QName, DeadPid, CTag},
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup, [consumer_created, Id]),

    %% Trigger gc. When the gen_server:call returns, the gc has already finished.
    rabbit_ct_broker_helpers:rpc(Config, A, erlang, send, [rabbit_core_metrics_gc, start_gc]),
    rabbit_ct_broker_helpers:rpc(Config, A, gen_server, call, [rabbit_core_metrics_gc, test]),

    [_|_] = rabbit_ct_broker_helpers:rpc(Config, A, ets, tab2list, [consumer_created]),
    [] = rabbit_ct_broker_helpers:rpc(Config, A, ets, lookup, [consumer_created, Id]),

    amqp_channel:call(Ch, #'queue.delete'{queue = <<"queue_metrics">>}),
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
