%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(peer_discovery_cleanup_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

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
     cleanup_queues,
     backend_errors_do_not_trigger_cleanup
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------


init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    Hostname = <<(atom_to_binary(Testcase, utf8))/binary,
                 ("." ?MODULE_STRING ".local")>>,
    Env = [{rabbit,
            [{cluster_formation,
              [{peer_discovery_backend, rabbit_peer_discovery_dns},
               {peer_discovery_dns, [{hostname, Hostname}]},
               %% We are going to cluster nodes after setting up meck so
               %% limit the number of times it tries to cluster during boot.
               {discovery_retry_limit, 0},
               %% Enable cleanup but set the interval high. Test cases should
               %% call rabbit_peer_discovery_cleanup:check_cluster/0 to force
               %% cleanup evaluation.
               {node_cleanup, [{cleanup_interval, 3600},
                               {cleanup_only_log_warning, false}]}]}]}],
    Config1 = rabbit_ct_helpers:merge_app_env(Config, Env),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [
                                            {rmq_nodes_count, 3},
                                            {rmq_nodes_clustered, false},
                                            {rmq_nodename_suffix, Testcase},
                                            {net_ticktime, 5}
                                           ]),
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      [fun setup_meck/1,
       fun mock_list_nodes/1,
       fun rabbit_ct_broker_helpers:cluster_nodes/1] ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

cleanup_queues(Config) ->
    %% Happy path: unreachable nodes not recognized by the backend are cleaned
    %% up.
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),

    QQ = <<"quorum-queue">>,
    declare_queue(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),

    %% Remove node C from peer discovery responses.
    mock_list_nodes(Config, {ok, [A, B]}),
    %% Make node C unreachable.
    rabbit_ct_broker_helpers:block_traffic_between(A, C),
    rabbit_ct_broker_helpers:block_traffic_between(B, C),
    Ts1 = erlang:system_time(millisecond),
    ?awaitMatch([C],
                rabbit_ct_broker_helpers:rpc(Config, A,
                                             rabbit_nodes,
                                             list_unreachable, []),
                30_000, 1_000),
    ct:log(?LOW_IMPORTANCE, "Node C became unreachable in ~bms",
           [erlang:system_time(millisecond) - Ts1]),

    ok = rabbit_ct_broker_helpers:rpc(Config, A,
                                      rabbit_peer_discovery_cleanup,
                                      check_cluster, []),

    %% Node C should be removed from the quorum queue members.
    ?assertEqual(
      lists:sort([A, B]),
      begin
          Info = rpc:call(A, rabbit_quorum_queue, infos,
                          [rabbit_misc:r(<<"/">>, queue, QQ)]),
          lists:sort(proplists:get_value(members, Info))
      end),

    %% Cleanup.
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

backend_errors_do_not_trigger_cleanup(Config) ->
    %% The backend could have some transient failures. While the backend is
    %% not giving reliable peer information, skip cleanup.
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),

    QQ = <<"quorum-queue">>,
    declare_queue(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),

    %% Have the backend return an error.
    mock_list_nodes(Config, {error, "...internal server error..."}),
    %% Make node C unreachable.
    rabbit_ct_broker_helpers:block_traffic_between(A, C),
    rabbit_ct_broker_helpers:block_traffic_between(B, C),
    Ts1 = erlang:system_time(millisecond),
    ?awaitMatch([C],
                rabbit_ct_broker_helpers:rpc(Config, A,
                                             rabbit_nodes,
                                             list_unreachable, []),
                30_000, 1_000),
    ct:log(?LOW_IMPORTANCE, "Node C became unreachable in ~bms",
           [erlang:system_time(millisecond) - Ts1]),

    ok = rabbit_ct_broker_helpers:rpc(Config, A,
                                      rabbit_peer_discovery_cleanup,
                                      check_cluster, []),

    %% Node C should remain in the quorum queue members.
    ?assertEqual(
      lists:sort([A, B, C]),
      begin
          Info = rpc:call(A, rabbit_quorum_queue, infos,
                          [rabbit_misc:r(<<"/">>, queue, QQ)]),
          lists:sort(proplists:get_value(members, Info))
      end),

    %% Cleanup.
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

%%%
%%% Implementation
%%%

setup_meck(Config) ->
    rabbit_ct_broker_helpers:setup_meck(Config),
    rabbit_ct_broker_helpers:rpc_all(Config, meck, new,
                                     [rabbit_peer_discovery_dns,
                                      [no_link, passthrough]]),
    Config.

mock_list_nodes(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    mock_list_nodes(Config, {ok, Nodes}).

mock_list_nodes(Config, Response) ->
    rabbit_ct_broker_helpers:rpc_all(Config, meck, expect,
                                     [rabbit_peer_discovery_dns,
                                      list_nodes, 0, Response]),
    Config.

declare_queue(Ch, QueueName, Args)
  when is_pid(Ch), is_binary(QueueName), is_list(Args) ->
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QueueName,
                                     durable = true,
                                     arguments = Args}).
