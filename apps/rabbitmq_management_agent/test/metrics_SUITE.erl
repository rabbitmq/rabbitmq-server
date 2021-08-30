%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(metrics_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                node,
                                storage_reset
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
