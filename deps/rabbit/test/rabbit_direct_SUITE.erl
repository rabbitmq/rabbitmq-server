%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_direct_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-export([all/0, groups/0]).
-export([init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).
-export([direct_connection_registered/1,
         blocked_by_a_remote_node_alarm/1]).

all() ->
    [{group, tests}].

groups() ->
    [{tests, [], [direct_connection_registered,
                  blocked_by_a_remote_node_alarm]}].

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
    NodesCount = case Testcase of
                     %% Needs a second node to raise the alarm on.
                     blocked_by_a_remote_node_alarm -> 2;
                     _                              -> 1
                 end,
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Testcase},
                         {rmq_nodes_count, NodesCount}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------

direct_connection_registered(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    BeforeLocal = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list_local, []),
    BeforeList = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list, []),
    ?assertEqual([], BeforeLocal),
    ?assertEqual([], BeforeList),

    Params = #amqp_params_direct{node         = Node,
                               virtual_host = <<"/">>,
                               username     = <<"guest">>,
                               password     = <<"guest">>},
    {ok, Conn} = amqp_connection:start(Params),
    true = is_pid(Conn),

    AfterLocal = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list_local, []),
    AfterList = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list, []),
    ?assertEqual([Conn], AfterLocal),
    ?assertEqual([Conn], AfterList),

    ok = amqp_connection:close(Conn),

    FinalLocal = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list_local, []),
    FinalList = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list, []),
    ?assertEqual([], FinalLocal),
    ?assertEqual([], FinalList),
    ok.

%% A resource alarm blocks publishers cluster-wide, because maybe_alert/5 calls
%% alert_local/3 unconditionally. But rabbit_alarm:internal_register/3 replays
%% only this node's alarms to a newly registered alertee, so a connection
%% opened while a *different* node is alarmed has to be seeded from the
%% cluster-wide set that register/2 returns, as rabbit_reader does.
blocked_by_a_remote_node_alarm(Config) ->
    Node0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    %% A real watermark alarm rather than rabbit_ct_broker_helpers:set_alarm/3,
    %% which embeds its Node argument in the alarm key: given an index rather
    %% than a node name, maybe_alert/5 finds node() =/= Node and skips
    %% alert_remote/3, so the alarm never leaves the node it was set on.
    OrigLimit = rabbit_ct_broker_helpers:rpc(
                  Config, 1, vm_memory_monitor,
                  get_vm_memory_high_watermark, []),
    try
        ok = rabbit_ct_broker_helpers:rpc(
               Config, 1, vm_memory_monitor,
               set_vm_memory_high_watermark, [0]),
        %% Wait for node 1's alarm to reach node 0, which happens through
        %% rabbit_alarm:remote_conserve_resources/3.
        ?awaitMatch([_ | _],
                    rabbit_ct_broker_helpers:rpc(
                      Config, 0, rabbit_alarm, get_alarms, []),
                    30_000),
        Params = #amqp_params_direct{node         = Node0,
                                     virtual_host = <<"/">>,
                                     username     = <<"guest">>,
                                     password     = <<"guest">>},
        {ok, Conn} = amqp_connection:start(Params),
        try
            amqp_connection:register_blocked_handler(Conn, self()),
            receive
                #'connection.blocked'{} -> ok
            after 10_000 ->
                      ct:fail(not_blocked_by_remote_node_alarm)
            end
        after
            ok = amqp_connection:close(Conn)
        end
    after
        ok = rabbit_ct_broker_helpers:rpc(
               Config, 1, vm_memory_monitor,
               set_vm_memory_high_watermark, [OrigLimit])
    end.
