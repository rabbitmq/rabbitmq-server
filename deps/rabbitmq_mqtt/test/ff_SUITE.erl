%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module(ff_SUITE).

%% Test suite for the feature flag delete_ra_cluster_mqtt_node

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(FEATURE_FLAG, delete_ra_cluster_mqtt_node).

all() ->
    [
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_3, [], [enable_feature_flag]}
    ].

suite() ->
    [
     {timetrap, {minutes, 5}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group = cluster_size_3, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 3},
                                                     {rmq_nodename_suffix, Group}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1, {rabbit, [{forced_feature_flags_on_init, []}]}),
    Config = rabbit_ct_helpers:run_steps(Config2,
                                         rabbit_ct_broker_helpers:setup_steps() ++
                                         rabbit_ct_client_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:is_feature_flag_supported(Config, ?FEATURE_FLAG) of
        true ->
            Config;
        false ->
            end_per_group(Group, Config),
            {skip, io_lib:format("feature flag ~s is unsupported", [?FEATURE_FLAG])}
    end.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

enable_feature_flag(Config) ->
    C = connect_to_node(Config, 1, <<"my-client">>),
    timer:sleep(500),
    %% old client ID tracking works
    ?assertMatch([{<<"my-client">>, _ConnectionPid}],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mqtt_collector, list, [])),
    %% Ra processes are alive
    ?assert(lists:all(fun erlang:is_pid/1,
                      rabbit_ct_broker_helpers:rpc_all(Config, erlang, whereis, [mqtt_node]))),
    %% new client ID tracking works
    ?assertEqual(1,
                 length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mqtt_clientid, list_all, []))),

    ?assertEqual(ok, rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG)),

    %% Ra processes should be gone
    ?assert(lists:all(fun(Pid) -> Pid =:= undefined end,
                      rabbit_ct_broker_helpers:rpc_all(Config, erlang, whereis, [mqtt_node]))),
    %% new client ID tracking still works
    ?assertEqual(1,
                 length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mqtt_clientid, list_all, []))),
    ?assert(erlang:is_process_alive(C)),
    ok = emqtt:disconnect(C).

connect_to_node(Config, Node, ClientID) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_mqtt),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, Port},
                                {clientid, ClientID},
                                {proto_ver, v4},
                                {connect_timeout, 1},
                                {ack_timeout, 1}]),
    {ok, _Properties} = emqtt:connect(C),
    C.
