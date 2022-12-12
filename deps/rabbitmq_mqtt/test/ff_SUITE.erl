%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module(ff_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_broker_helpers, [rpc/5]).
-import(rabbit_ct_helpers, [eventually/1]).
-import(util, [expect_publishes/2,
               get_global_counters/4,
               connect/2,
               connect/4]).

-define(PROTO_VER, v4).

all() ->
    [
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_3, [], [delete_ra_cluster_mqtt_node,
                           rabbit_mqtt_qos0_queue]}
    ].

suite() ->
    [
     {timetrap, {minutes, 2}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group = cluster_size_3, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 3},
                                                     {rmq_nodename_suffix, Group}]),
    Config = rabbit_ct_helpers:merge_app_env(
               Config1, {rabbit, [{forced_feature_flags_on_init, []}]}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(TestCase, Config) ->
    case rabbit_ct_broker_helpers:is_feature_flag_supported(Config, TestCase) of
        true ->
            Config;
        false ->
            {skip, io_lib:format("feature flag ~s is unsupported",
                                 [TestCase])}
    end.

end_per_testcase(_TestCase, Config) ->
    Config.

delete_ra_cluster_mqtt_node(Config) ->
    FeatureFlag = ?FUNCTION_NAME,
    C = connect(<<"my-client">>, Config, 1, []),
    timer:sleep(500),
    %% old client ID tracking works
    ?assertEqual(1, length(util:all_connection_pids(Config))),
    %% Ra processes are alive
    ?assert(lists:all(fun erlang:is_pid/1,
                      rabbit_ct_broker_helpers:rpc_all(Config, erlang, whereis, [mqtt_node]))),

    ?assertEqual(ok,
                 rabbit_ct_broker_helpers:enable_feature_flag(Config, FeatureFlag)),

    %% Ra processes should be gone
    rabbit_ct_helpers:eventually(
      ?_assert(lists:all(fun(Pid) -> Pid =:= undefined end,
                         rabbit_ct_broker_helpers:rpc_all(Config, erlang, whereis, [mqtt_node])))),
    %% new client ID tracking works
    ?assertEqual(1, length(util:all_connection_pids(Config))),
    ?assert(erlang:is_process_alive(C)),
    ok = emqtt:disconnect(C).

rabbit_mqtt_qos0_queue(Config) ->
    FeatureFlag = ?FUNCTION_NAME,
    Msg = Topic = ClientId = atom_to_binary(?FUNCTION_NAME),

    C1 = connect(ClientId, Config),
    {ok, _, [0]} = emqtt:subscribe(C1, Topic, qos0),
    ok = emqtt:publish(C1, Topic, Msg, qos0),
    ok = expect_publishes(Topic, [Msg]),
    ?assertEqual(1,
                 length(rpc(Config, 0, rabbit_amqqueue, list_by_type, [rabbit_classic_queue]))),

    ?assertEqual(ok,
                 rabbit_ct_broker_helpers:enable_feature_flag(Config, FeatureFlag)),

    %% Queue type does not chanage for existing connection.
    ?assertEqual(1,
                 length(rpc(Config, 0, rabbit_amqqueue, list_by_type, [rabbit_classic_queue]))),
    ok = emqtt:publish(C1, Topic, Msg, qos0),
    ok = expect_publishes(Topic, [Msg]),
    ?assertMatch(#{messages_delivered_total := 2,
                   messages_delivered_consume_auto_ack_total := 2},
                 get_global_counters(Config, ?PROTO_VER, 0, [{queue_type, rabbit_classic_queue}])),

    %% Reconnecting with the same client ID will terminate the old connection.
    true = unlink(C1),
    C2 = connect(ClientId, Config),
    {ok, _, [0]} = emqtt:subscribe(C2, Topic, qos0),
    %% This time, we get the new queue type.
    eventually(
      ?_assertEqual(0,
                    length(rpc(Config, 0, rabbit_amqqueue, list_by_type, [rabbit_classic_queue])))),
    ?assertEqual(1,
                 length(rpc(Config, 0, rabbit_amqqueue, list_by_type, [FeatureFlag]))),
    ok = emqtt:publish(C2, Topic, Msg, qos0),
    ok = expect_publishes(Topic, [Msg]),
    ?assertMatch(#{messages_delivered_total := 1,
                   messages_delivered_consume_auto_ack_total := 1},
                 get_global_counters(Config, ?PROTO_VER, 0, [{queue_type, FeatureFlag}])),
    ok = emqtt:disconnect(C2).
