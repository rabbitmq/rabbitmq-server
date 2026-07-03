%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(metrics_SUITE).
-compile(export_all).

-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                node,
                                storage_reset,
                                collector_survives_non_integer_metric,
                                sum_entry_and_difference_unit_checks
                               ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    _Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                              {rabbit, [
                                                        {collect_statistics, fine},
                                                        {collect_statistics_interval, 500}
                                                       ]}).
    %% rabbit_ct_helpers:merge_app_env(
    %%   Config1, {rabbitmq_management_agent, [{sample_retention_policies,
    %%                                          [{global,   [{605, 500}]},
    %%                                           {basic,    [{605, 500}]},
    %%                                           {detailed, [{10, 500}]}] }]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 2}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      [ fun merge_app_env/1 ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

read_table_rpc(Config, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, read_table, [Table]).

read_table(Table) ->
    ets:tab2list(Table).

force_stats() ->
    rabbit_mgmt_external_stats ! emit_update.

node(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    % force multipe stats refreshes
    [ rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, force_stats, [])
      || _ <- lists:seq(0, 10)],
    [_] = read_table_rpc(Config, node_persister_metrics),
    [_] = read_table_rpc(Config, node_coarse_metrics),
    [_] = read_table_rpc(Config, node_metrics),
    true = wait_until(
             fun() ->
                     Tab = read_table_rpc(Config, node_node_metrics),
                     lists:keymember({A, B}, 1, Tab)
             end, 10).


%% Regression test for rabbitmq/rabbitmq-server#12815: a non-integer
%% sentinel in a core_metrics row crashed the collector with `badarith`.
collector_survives_non_integer_metric(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, ?MODULE, do_collector_survives_non_integer_metric, []).

do_collector_survives_non_integer_metric() ->
    Collector = rabbit_mgmt_metrics_collector:name(connection_coarse_metrics),
    CollectorPid = whereis(Collector),
    true = is_pid(CollectorPid),
    %% Use a freshly-allocated synthetic Pid so we cannot collide with
    %% real broker connections being scraped at the same time.
    SyntheticPid = c:pid(0, 16#7fffff, 0),
    %% Baseline: a well-formed row populates old_aggr_stats on the next scrape.
    ets:insert(connection_coarse_metrics,
               {SyntheticPid, 100, 200, 1000, 0}),
    ok = gen_server:call(Collector, force_collect),
    %% Corrupt the row to mimic the observed race (one field becomes '').
    ets:insert(connection_coarse_metrics,
               {SyntheticPid, '', 200, 1000, 0}),
    %% Second scrape: with the defensive arithmetic this returns ok;
    %% without the fix the gen_server:call exits with a badarith error.
    ok = gen_server:call(Collector, force_collect),
    %% Collector pid must not have been restarted in between.
    CollectorPid = whereis(Collector),
    true = is_process_alive(CollectorPid),
    %% Cleanup so the synthetic row does not leak into sibling test cases.
    true = ets:delete(connection_coarse_metrics, SyntheticPid),
    ok.

%% Direct unit-style assertions on the helpers themselves to lock in the
%% "non-integer is coerced to 0" contract that aggregate_entry relies on.
sum_entry_and_difference_unit_checks(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, ?MODULE, do_sum_entry_and_difference_unit_checks, []).

do_sum_entry_and_difference_unit_checks() ->
    %% Happy path: integer math is unchanged.
    {5} = rabbit_mgmt_metrics_collector:sum_entry({2}, {3}),
    {1} = rabbit_mgmt_metrics_collector:difference({2}, {3}),
    {5, 7} = rabbit_mgmt_metrics_collector:sum_entry({2, 3}, {3, 4}),
    {1, 1} = rabbit_mgmt_metrics_collector:difference({2, 3}, {3, 4}),
    %% Sentinel '' on either side coerces to 0 and avoids badarith.
    %% sum_entry/difference compute B (+|-) A, with A the first arg.
    {3, 7} = rabbit_mgmt_metrics_collector:sum_entry({'', 3}, {3, 4}),
    {5, 3} = rabbit_mgmt_metrics_collector:sum_entry({2, 3}, {3, ''}),
    {3, 1} = rabbit_mgmt_metrics_collector:difference({'', 3}, {3, 4}),
    {-2, -3} = rabbit_mgmt_metrics_collector:difference({2, 3}, {'', ''}),
    ok.

storage_reset(Config) ->
    %% Ensures that core stats are reset, otherwise consume generates negative values
    %% Doesn't really test if the reset does anything!
    {Ch, Q} = publish_msg(Config),
    wait_until(fun() ->
                       {1, 0, 1} == get_vhost_stats(Config)
               end),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_storage, reset, []),
    wait_until(fun() ->
                       {1, 0, 1} == get_vhost_stats(Config)
               end),
    consume_msg(Ch, Q),
    wait_until(fun() ->
                       {0, 0, 0} == get_vhost_stats(Config)
               end),
    rabbit_ct_client_helpers:close_channel(Ch).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

publish_msg(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                payload = Q}),
    {Ch, Q}.

consume_msg(Ch, Q) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue    = Q,
                                                no_ack   = true}, self()),
    receive #'basic.consume_ok'{} -> ok
    end,
    receive {#'basic.deliver'{}, #amqp_msg{payload = Q}} ->
            ok
    end.

wait_until(Fun) ->
    wait_until(Fun, 120).

wait_until(_, 0) ->
    false;
wait_until(Fun, N) ->
    case Fun() of
        true ->
            true;
        false ->
            timer:sleep(1000),
            wait_until(Fun, N-1)
    end.

get_vhost_stats(Config) ->
    Dict = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_data, overview_data,
                                        [none, all ,{no_range, no_range, no_range,no_range},
                                         [<<"/">>]]),
    {ok, {VhostMsgStats, _}} = maps:find(vhost_msg_stats, Dict),
    exometer_slide:last(VhostMsgStats).
