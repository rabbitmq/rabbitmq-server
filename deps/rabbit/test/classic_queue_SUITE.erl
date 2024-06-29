%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(classic_queue_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).


all() ->
    [
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_3, [], [
                           leader_locator_client_local,
                           leader_locator_balanced,
                           locator_deprecated
                          ]
     }].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [
                                            {rmq_nodename_suffix, Group},
                                            {rmq_nodes_count, 3},
                                            {rmq_nodes_clustered, true},
                                            {tcp_ports_base, {skip_n_nodes, 3}}
                                           ]),
    Config2 = rabbit_ct_helpers:run_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    Config2.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

leader_locator_client_local(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "x-queue-leader-locator is only supported by CQs in 4.0+"};
        false ->

            Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
            Q = <<"q1">>,

            [begin
                 Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
                 ?assertEqual({'queue.declare_ok', Q, 0, 0},
                              declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                              {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
                 {ok, Leader0} = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, Q)]),
                 Leader = amqqueue:qnode(Leader0),
                 ?assertEqual(Server, Leader),
                 ?assertMatch(#'queue.delete_ok'{},
                              amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
             end || Server <- Servers] end.

leader_locator_balanced(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "x-queue-leader-locator is only supported by CQs in 4.0+"};
        false ->
            test_leader_locator(Config, <<"x-queue-leader-locator">>, [<<"balanced">>])
    end.

%% This test can be delted once we remove x-queue-master-locator support
locator_deprecated(Config) ->
    test_leader_locator(Config, <<"x-queue-master-locator">>, [<<"least-leaders">>,
                                                               <<"random">>,
                                                               <<"min-masters">>]).

test_leader_locator(Config, Argument, Strategies) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Qs = [<<"q1">>, <<"q2">>, <<"q3">>],

    [begin
         Leaders = [begin
                        ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                     declare(Ch, Q,
                                             [{<<"x-queue-type">>, longstr, <<"classic">>},
                                              {Argument, longstr, Strategy}])),

                        {ok, Leader0} = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, Q)]),
                        Leader = amqqueue:qnode(Leader0),
                        Leader
                    end || Q <- Qs],
         ?assertEqual(3, sets:size(sets:from_list(Leaders))),

         [?assertMatch(#'queue.delete_ok'{},
                       amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
          || Q <- Qs]
     end || Strategy <- Strategies ].

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

