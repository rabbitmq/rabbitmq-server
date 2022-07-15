-module(direct_exchange_routing_v2_post_3_11_SUITE).

%% Test suite for the feature flag direct_exchange_routing_v2

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(FEATURE_FLAG, direct_exchange_routing_v2).
-define(INDEX_TABLE_NAME, rabbit_index_route).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, unclustered_cluster_size_2}
    ].

groups() ->
    [
     {unclustered_cluster_size_2, [], [cluster_formation]}
    ].

suite() ->
    [
     %% If a test hangs, no need to wait for 30 minutes.
     {timetrap, {minutes, 8}}
    ].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "This test suite won't work in mixed mode with pre 3.11 releases"};
        false ->
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config, [])
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unclustered_cluster_size_2 = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 2},
                                                    {rmq_nodes_clustered, false}]),
    Config1 = start_broker(Group, Config),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config1, ?FEATURE_FLAG) of
        ok ->
            Config1;
        {skip, _} = Skip ->
            end_per_group(Group, Config1),
            Skip
    end.

start_broker(Group, Config0) ->
    Size = rabbit_ct_helpers:get_config(Config0, rmq_nodes_count),
    Config = rabbit_ct_helpers:set_config(Config0, {rmq_nodename_suffix,
                                                    io_lib:format("cluster_size_~b-~s", [Size, Group])}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    %% Test that all bindings got removed from the database.
    ?assertEqual([0,0,0,0,0],
                 lists:map(fun(Table) ->
                                   table_size(Config, Table)
                           end, [rabbit_durable_route,
                                 rabbit_semi_durable_route,
                                 rabbit_route,
                                 rabbit_reverse_route,
                                 ?INDEX_TABLE_NAME])
                ).

%%%===================================================================
%%% Test cases
%%%===================================================================

cluster_formation(Config) ->
    Servers0 = [Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ?assertMatch([Server1], rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, table_info,
                                                         [?INDEX_TABLE_NAME, ram_copies])),
    ?assertMatch([Server2], rabbit_ct_broker_helpers:rpc(Config, 1, mnesia, table_info,
                                                         [?INDEX_TABLE_NAME, ram_copies])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server2)], []),
    rabbit_control_helper:command(start_app, Server1),
    Servers = lists:sort(Servers0),
    ?assertMatch(Servers, lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, table_info,
                                                                  [?INDEX_TABLE_NAME, ram_copies]))),
    ?assertMatch(Servers, lists:sort(rabbit_ct_broker_helpers:rpc(Config, 1, mnesia, table_info,
                                                                  [?INDEX_TABLE_NAME, ram_copies]))).

%%%===================================================================
%%% Helpers
%%%===================================================================
table_size(Config, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ets, info, [Table, size], 5000).
