%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Covers permission checks for passive queue, exchange declarations.

-module(passive_declare_permission_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(VHOST, <<"passive-declare-permissions-vhost">>).
-define(USER, <<"passive-declare-permissions-user">>).
-define(QUEUE, <<"passive-declare-permissions-queue">>).
-define(EXCHANGE, <<"passive-declare-permissions-exchange">>).
-define(TIMEOUT, 30_000).
-define(PROPER_ITERATIONS, 64).

all() ->
    [{group, single_node}].

groups() ->
    [{single_node, [],
      [enumerate_all_subsets_queue,
       enumerate_all_subsets_exchange,
       prop_passive_queue_declare_iff_any_perm,
       prop_passive_exchange_declare_iff_any_perm]}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(single_node, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{rmq_nodename_suffix, single_node},
                 {rmq_nodes_count, 1}]),
    Config = rabbit_ct_helpers:run_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
                   rabbit_ct_client_helpers:setup_steps()),
    case Config of
        _ when is_list(Config) ->
            ok = rabbit_ct_broker_helpers:add_vhost(Config, ?VHOST),
            ok = rabbit_ct_broker_helpers:add_user(Config, ?USER),
            ok = rabbit_ct_broker_helpers:set_full_permissions(
                   Config, ?USER, ?VHOST),
            declare_topology(Config),
            Config;
        {skip, _} = Skip ->
            Skip
    end.

end_per_group(single_node, Config) ->
    ok = rabbit_ct_broker_helpers:delete_user(Config, ?USER),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?VHOST),
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
          rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

enumerate_all_subsets_queue(Config) ->
    [verify_passive_queue_declare(Config, Subset, expected_outcome(Subset))
     || Subset <- all_subsets()],
    ok.

enumerate_all_subsets_exchange(Config) ->
    [verify_passive_exchange_declare(Config, Subset, expected_outcome(Subset))
     || Subset <- all_subsets()],
    ok.

prop_passive_queue_declare_iff_any_perm(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> property_for(Config, queue) end,
      [], ?PROPER_ITERATIONS).

prop_passive_exchange_declare_iff_any_perm(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> property_for(Config, exchange) end,
      [], ?PROPER_ITERATIONS).

property_for(Config, Kind) ->
    ?FORALL(Subset, perm_subset_gen(),
            begin
                Outcome =
                    case Kind of
                        queue ->
                            attempt_passive_queue_declare(Config, Subset);
                        exchange ->
                            attempt_passive_exchange_declare(Config, Subset)
                    end,
                Outcome =:= expected_outcome(Subset)
            end).

perm_subset_gen() ->
    list(oneof([configure, write, read])).

declare_topology(Config) ->
    Conn = open_admin_conn(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue = ?QUEUE, durable = true}),
    #'exchange.declare_ok'{} =
        amqp_channel:call(Ch, #'exchange.declare'{exchange = ?EXCHANGE,
                                                   type = <<"fanout">>,
                                                   durable = true}),
    ok = amqp_channel:close(Ch),
    ok = amqp_connection:close(Conn).

open_admin_conn(Config) ->
    {ok, Conn} = amqp_connection:start(
                   #amqp_params_network{
                      host = ?config(rmq_hostname, Config),
                      port = rabbit_ct_broker_helpers:get_node_config(
                               Config, 0, tcp_port_amqp),
                      virtual_host = ?VHOST,
                      username = ?USER,
                      password = ?USER}),
    Conn.

all_subsets() ->
    [[],
     [configure],
     [write],
     [read],
     [configure, write],
     [configure, read],
     [write, read],
     [configure, write, read]].

expected_outcome([]) -> refused;
expected_outcome(_)  -> permitted.

set_perms_for(Config, Subset, ResourceName) ->
    {Cfg, Wr, Rd} = perm_patterns(Subset, ResourceName),
    ok = rabbit_ct_broker_helpers:set_permissions(
           Config, ?USER, ?VHOST, Cfg, Wr, Rd).

perm_patterns(Subset, Name) ->
    {pattern_for(configure, Subset, Name),
     pattern_for(write,     Subset, Name),
     pattern_for(read,      Subset, Name)}.

pattern_for(Perm, Subset, Name) ->
    case lists:member(Perm, Subset) of
        true  -> Name;
        false -> <<>>
    end.

verify_passive_queue_declare(Config, Subset, Expected) ->
    ?assertEqual(Expected, attempt_passive_queue_declare(Config, Subset),
                 {queue_subset, Subset}).

verify_passive_exchange_declare(Config, Subset, Expected) ->
    ?assertEqual(Expected, attempt_passive_exchange_declare(Config, Subset),
                 {exchange_subset, Subset}).

attempt_passive_queue_declare(Config, Subset) ->
    set_perms_for(Config, Subset, ?QUEUE),
    Conn = open_admin_conn(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Mref = monitor(process, Ch),
    Outcome =
        try
            #'queue.declare_ok'{} =
                amqp_channel:call(Ch,
                                  #'queue.declare'{queue = ?QUEUE,
                                                   passive = true}),
            permitted
        catch _:_ ->
                refused
        end,
    drain_channel(Ch, Mref, Outcome),
    catch amqp_connection:close(Conn),
    Outcome.

attempt_passive_exchange_declare(Config, Subset) ->
    set_perms_for(Config, Subset, ?EXCHANGE),
    Conn = open_admin_conn(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Mref = monitor(process, Ch),
    Outcome =
        try
            #'exchange.declare_ok'{} =
                amqp_channel:call(Ch,
                                  #'exchange.declare'{exchange = ?EXCHANGE,
                                                       passive = true}),
            permitted
        catch _:_ ->
                refused
        end,
    drain_channel(Ch, Mref, Outcome),
    catch amqp_connection:close(Conn),
    Outcome.

drain_channel(_Ch, Mref, permitted) ->
    erlang:demonitor(Mref, [flush]),
    ok;
drain_channel(_Ch, Mref, refused) ->
    receive
        {'DOWN', Mref, process, _, _} -> ok
    after ?TIMEOUT ->
              erlang:demonitor(Mref, [flush]),
              ct:fail(channel_did_not_close_on_refusal)
    end.
