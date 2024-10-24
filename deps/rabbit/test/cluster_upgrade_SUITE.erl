%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(cluster_upgrade_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, all_tests}
    ].

groups() ->
    [
     {all_tests, [], all_tests()}
    ].

all_tests() ->
    [
     queue_upgrade
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config);
        false ->
            {skip, "cluster upgrade tests must be run in mixed versions "
                   "testing only"}
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 3},
        {force_secondary, true}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, Testcase).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
                rabbit_ct_client_helpers:teardown_steps() ++
                rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

queue_upgrade(Config) ->
    ok = print_cluster_versions(Config),

    %% Declare some resources before upgrading.
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    ClassicQName = <<"classic-q">>,
    QQName = <<"quorum-q">>,
    StreamQName = <<"stream-q">>,
    declare(Ch, ClassicQName, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    declare(Ch, QQName, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare(Ch, StreamQName, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    [begin
         #'queue.bind_ok'{} = amqp_channel:call(
                                Ch,
                                #'queue.bind'{queue = Name,
                                              exchange = <<"amq.fanout">>,
                                              routing_key = Name})
     end || Name <- [ClassicQName, QQName, StreamQName]],
    Msgs = [<<"msg">>, <<"msg">>, <<"msg">>],
    publish_confirm(Ch, <<"amq.fanout">>, <<>>, Msgs),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    %% Restart the servers
    Config1 = upgrade_cluster(Config),
    ok = print_cluster_versions(Config1),

    %% Check that the resources are still there
    queue_utils:wait_for_messages(Config, [[ClassicQName, <<"3">>, <<"3">>, <<"0">>],
                                           [QQName,       <<"3">>, <<"3">>, <<"0">>],
                                           [StreamQName,  <<"3">>, <<"3">>, <<"0">>]]),

    ok.

%% ----------------------------------------------------------------------------
%% Internal utils
%% ----------------------------------------------------------------------------

declare(Ch, Q, Args) ->
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue     = Q,
                                                   durable   = true,
                                                   auto_delete = false,
                                                   arguments = Args}).

publish(Ch, X, RK, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{exchange = X,
                                            routing_key = RK},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

publish_confirm(Ch, X, RK, Msgs) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    [publish(Ch, X, RK, Msg) || Msg <- Msgs],
    amqp_channel:wait_for_confirms(Ch, 5).

cluster_members(Config) ->
    rabbit_ct_broker_helpers:get_node_configs(Config, nodename).

upgrade_cluster(Config) ->
    Cluster = cluster_members(Config),
    ct:pal(?LOW_IMPORTANCE, "Stopping cluster ~p", [Cluster]),
    [ok = rabbit_ct_broker_helpers:stop_node(Config, N)
     || N <- Cluster],
    ct:pal(?LOW_IMPORTANCE, "Restarting cluster ~p", [Cluster]),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {force_secondary, false}),
    [ok = rabbit_ct_broker_helpers:async_start_node(Config1, N)
     || N <- Cluster],
    [ok = rabbit_ct_broker_helpers:wait_for_async_start_node(N)
     || N <- Cluster],
    Config1.

print_cluster_versions(Config) ->
    Cluster = cluster_members(Config),
    Versions = [begin
                    Version = rabbit_ct_broker_helpers:rpc(
                                Config, N,
                                rabbit, product_version, []),
                    {N, Version}
                end || N <- Cluster],
    ct:pal("Cluster versions: ~p", [Versions]),
    ok.
