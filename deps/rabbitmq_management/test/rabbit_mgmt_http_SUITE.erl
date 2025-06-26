%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_http_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_filter.hrl").
-include_lib("amqp10_client/include/amqp10_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/1]).
-import(rabbit_ct_broker_helpers, [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1,
         eventually/3]).
-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2,
                                assert_keys/2, assert_no_keys/2,
                                decode_body/1,
                                http_get/2, http_get/3, http_get/5,
                                http_get_no_auth/3,
                                http_get_no_decode/5,
                                http_put/4, http_put/6,
                                http_post/4, http_post/6,
                                http_post_json/4,
                                http_upload_raw/8,
                                http_delete/3, http_delete/4, http_delete/5,
                                http_put_raw/4, http_post_accept_json/4,
                                req/4, auth_header/2,
                                assert_permanent_redirect/3,
                                uri_base_from/2, format_for_upload/1,
                                amqp_port/1, req/6]).

-import(rabbit_misc, [pget/2]).

-define(COLLECT_INTERVAL, 256).
-define(PATH_PREFIX, "/custom-prefix").

-define(AWAIT(Body),
        await_condition(fun () -> Body end)).

-compile([export_all, nowarn_export_all]).

all() ->
    [
        {group, all_tests_with_prefix},
        {group, all_tests_without_prefix},
        {group, definitions_group1_without_prefix},
        {group, definitions_group2_without_prefix},
        {group, definitions_group3_without_prefix},
        {group, definitions_group4_without_prefix},
        {group, default_queue_type_without_prefix}
    ].

groups() ->
    [
        {all_tests_with_prefix, [], some_tests() ++ all_tests()},
        {all_tests_without_prefix, [], some_tests()},
        %% We have several groups because their interference is
        %% way above average. It is easier to use separate groups
        %% that get a blank node each than to try to untangle multiple
        %% definitions-related tests. MK.
        {definitions_group1_without_prefix, [], definitions_group1_tests()},
        {definitions_group2_without_prefix, [], definitions_group2_tests()},
        {definitions_group3_without_prefix, [], definitions_group3_tests()},
        {definitions_group4_without_prefix, [], definitions_group4_tests()},
        {default_queue_type_without_prefix, [], default_queue_type_group_tests()}
    ].

some_tests() ->
    [
        users_test,
        exchanges_test,
        queues_test,
        bindings_test,
        policy_test,
        policy_permissions_test
    ].

definitions_group1_tests() ->
    [
        definitions_test,
        definitions_password_test,
        long_definitions_test,
        long_definitions_multipart_test
    ].

definitions_group2_tests() ->
    [
        definitions_default_queue_type_test,
        definitions_vhost_metadata_test,
        definitions_file_metadata_test
    ].

definitions_group3_tests() ->
    [
        definitions_server_named_queue_test,
        definitions_with_charset_test
    ].

definitions_group4_tests() ->
    [
        definitions_vhost_test
    ].

default_queue_type_group_tests() ->
    [
        default_queue_type_fallback_in_overview_test,
        default_queue_type_with_value_configured_in_overview_test,
        default_queue_types_in_vhost_list_test,
        default_queue_type_of_one_vhost_test
    ].

all_tests() -> [
    cli_redirect_test,
    api_redirect_test,
    stats_redirect_test,
    overview_test,
    auth_test,
    cluster_name_test,
    nodes_test,
    memory_test,
    ets_tables_memory_test,
    vhosts_test,
    vhosts_description_test,
    vhosts_trace_test,
    users_legacy_administrator_test,
    adding_a_user_with_password_test,
    adding_a_user_with_password_hash_test,
    adding_a_user_with_generated_password_hash_test,
    adding_a_user_with_permissions_in_single_operation_test,
    adding_a_user_without_tags_fails_test,
    adding_a_user_without_password_or_hash_test,
    adding_a_user_with_both_password_and_hash_fails_test,
    updating_a_user_without_password_or_hash_clears_password_test,
    user_credential_validation_accept_everything_succeeds_test,
    user_credential_validation_min_length_succeeds_test,
    user_credential_validation_min_length_fails_test,
    updating_tags_of_a_passwordless_user_test,
    permissions_validation_test,
    permissions_list_test,
    permissions_test,
    multiple_invalid_connections_test,
    quorum_queues_test,
    stream_queues_have_consumers_field,
    bindings_post_test,
    bindings_null_routing_key_test,
    bindings_e2e_test,
    permissions_administrator_test,
    permissions_vhost_test,
    permissions_amqp_test,
    permissions_queue_delete_test,
    permissions_connection_channel_consumer_test,
    consumers_cq_test,
    consumers_qq_test,
    arguments_test,
    arguments_table_test,
    queue_purge_test,
    queue_actions_test,
    exclusive_consumer_test,
    exclusive_queue_test,
    connections_channels_pagination_test,
    exchanges_pagination_test,
    exchanges_pagination_permissions_test,
    queues_detailed_test,
    queue_pagination_test,
    queue_pagination_columns_test,
    queues_pagination_permissions_test,
    samples_range_test,
    sorting_test,
    format_output_test,
    columns_test,
    get_test,
    get_encoding_test,
    get_fail_test,
    publish_test,
    publish_large_message_test,
    publish_large_message_exceeding_http_request_body_size_test,
    publish_accept_json_test,
    publish_fail_test,
    publish_base64_test,
    publish_unrouted_test,
    if_empty_unused_test,
    parameters_test,
    global_parameters_test,
    disabled_operator_policy_test,
    operator_policy_test,
    issue67_test,
    extensions_test,
    cors_test,
    vhost_limits_list_test,
    vhost_limit_set_test,
    rates_test,
    single_active_consumer_cq_test,
    single_active_consumer_qq_test,
    %% This test needs the OAuth 2 plugin to be enabled
    %% oauth_test,
    disable_basic_auth_test,
    login_test,
    csp_headers_test,
    auth_attempts_test,
    user_limits_list_test,
    user_limit_set_test,
    config_environment_test,
    disabled_qq_replica_opers_test,
    qq_status_test,
    list_deprecated_features_test,
    list_used_deprecated_features_test,
    connections_amqpl,
    connections_amqp,
    amqp_sessions,
    amqpl_sessions,
    enable_plugin_amqp,
    cluster_and_node_tags_test,
    version_test
].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
merge_app_env(Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                              {rabbit,
                                               [
                                                {collect_statistics_interval,
                                                 ?COLLECT_INTERVAL},
                                                {quorum_tick_interval, 256},
                                                {stream_tick_interval, 256}
                                               ]}),
    rabbit_ct_helpers:merge_app_env(Config1,
                                    {rabbitmq_management, [
                                     {sample_retention_policies,
                                          [{global,   [{605, 1}]},
                                           {basic,    [{605, 1}]},
                                           {detailed, [{10, 1}]}]
                                     }]}).

start_broker(Config) ->
    Setup0 = rabbit_ct_broker_helpers:setup_steps(),
    Setup1 = rabbit_ct_client_helpers:setup_steps(),
    Steps = Setup0 ++ Setup1,
    case rabbit_ct_helpers:run_setup_steps(Config, Steps) of
        {skip, _} = Skip ->
            Skip;
        Config1 ->
            Ret = rabbit_ct_broker_helpers:enable_feature_flag(
                    Config1, 'rabbitmq_4.0.0'),
            case Ret of
                ok -> Config1;
                _  -> Ret
            end
    end.

finish_init(Group, Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    NodeConf = [{rmq_nodename_suffix, Group}],
    Config1 = rabbit_ct_helpers:set_config(Config, NodeConf),
    merge_app_env(Config1).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(rabbitmq_amqp_client),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(all_tests_with_prefix=Group, Config0) ->
    PathConfig = {rabbitmq_management, [{path_prefix, ?PATH_PREFIX}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    Config2 = finish_init(Group, Config1),
    start_broker(Config2);
init_per_group(Group, Config0) ->
    Config1 = finish_init(Group, Config0),
    start_broker(Config1).

end_per_group(_, Config) ->
    inets:stop(),
    Teardown0 = rabbit_ct_client_helpers:teardown_steps(),
    Teardown1 = rabbit_ct_broker_helpers:teardown_steps(),
    Steps = Teardown0 ++ Teardown1,
    rabbit_ct_helpers:run_teardown_steps(Config, Steps).

init_per_testcase(Testcase = permissions_vhost_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost1">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost2">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase = stream_queues_have_consumers_field, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            rabbit_ct_helpers:testcase_started(Config, Testcase)
    end;
init_per_testcase(Testcase = disabled_operator_policy_test, Config) ->
    Restrictions = [{operator_policy_changes, [{disabled, true}]}],
    rabbit_ct_broker_helpers:rpc_all(Config,
      application, set_env, [rabbitmq_management, restrictions, Restrictions]),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase = disabled_qq_replica_opers_test, Config) ->
    Restrictions = [{quorum_queue_replica_operations, [{disabled, true}]}],
    rabbit_ct_broker_helpers:rpc_all(Config,
      application, set_env, [rabbitmq_management, restrictions, Restrictions]),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase = cluster_and_node_tags_test, Config) ->
    Tags = [{<<"az">>, <<"us-east-3">>}, {<<"region">>,<<"us-east">>}, {<<"environment">>,<<"production">>}],
    rpc(Config,
        application, set_env, [rabbit, node_tags, Tags]),
    rpc(
      Config, rabbit_runtime_parameters, set_global,
      [cluster_tags, Tags, none]),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(queues_detailed_test, Config) ->
    IsEnabled = rabbit_ct_broker_helpers:is_feature_flag_enabled(
                  Config, detailed_queues_endpoint),
    case IsEnabled of
        true  -> Config;
        false -> {skip, "The detailed queues endpoint is not available."}
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"rabbit_mgmt_SUITE:init_per_testcase">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"rabbit_mgmt_SUITE:end_per_testcase">>),
    rpc(Config, application, set_env, [rabbitmq_management, disable_basic_auth, false]),
    Config1 = end_per_testcase0(Testcase, Config),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

end_per_testcase0(T, Config)
  when T =:= long_definitions_test; T =:= long_definitions_multipart_test ->
    Vhosts = long_definitions_vhosts(T),
    [rabbit_ct_broker_helpers:delete_vhost(Config, Name)
     || #{name := Name} <- Vhosts],
    Config;
end_per_testcase0(definitions_password_test, Config) ->
    rpc(Config, application, unset_env, [rabbit, password_hashing_module]),
    Config;
end_per_testcase0(queues_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"downvhost">>),
    Config;
end_per_testcase0(vhost_limits_list_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"limit_test_vhost_1">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"limit_test_vhost_2">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_vhost_1_user">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_vhost_2_user">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"no_vhost_user">>),
    Config;
end_per_testcase0(vhost_limit_set_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"limit_test_vhost_1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_vhost_1_user">>),
    Config;
end_per_testcase0(user_limits_list_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"limit_test_vhost_1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_user_1_user">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_user_2_user">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"no_vhost_user">>),
    Config;
end_per_testcase0(user_limit_set_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"limit_test_vhost_1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_user_1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_vhost_1_user">>),
    Config;
end_per_testcase0(permissions_vhost_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost1">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost2">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"myuser1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"myuser2">>),
    Config;
end_per_testcase0(config_environment_test, Config) ->
    rpc(Config, application, unset_env, [rabbit, config_environment_test_env]),
    Config;
end_per_testcase0(disabled_operator_policy_test, Config) ->
    rpc(Config, application, unset_env, [rabbitmq_management, restrictions]),
    Config;
end_per_testcase0(disabled_qq_replica_opers_test, Config) ->
    rpc(Config, application, unset_env, [rabbitmq_management, restrictions]),
    Config;
end_per_testcase0(cluster_and_node_tags_test, Config) ->
    rpc(
      Config, application, unset_env, [rabbit, node_tags]),
    rpc(
      Config, rabbit_runtime_parameters, clear_global,
      [cluster_tags, none]),
    Config;
end_per_testcase0(Testcase, Config)
  when Testcase == list_deprecated_features_test;
       Testcase == list_used_deprecated_features_test ->
    ok = rpc(Config, rabbit_feature_flags, clear_injected_test_feature_flags, []),
    Config;
end_per_testcase0(_, Config) -> Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

overview_test(Config) ->
    %% Rather crude, but this req doesn't say much and at least this means it
    %% didn't blow up.
    true = 0 < length(maps:get(listeners, http_get(Config, "/overview"))),
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags,     <<"management">>}], {group, '2xx'}),
    http_get(Config, "/overview", "myuser", "myuser", ?OK),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

cluster_name_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags,     <<"management">>}], {group, '2xx'}),
    http_put(Config, "/cluster-name", [{name, "foo"}], "myuser", "myuser", ?NOT_AUTHORISED),
    http_put(Config, "/cluster-name", [{name, "foo"}], {group, '2xx'}),
    #{name := <<"foo">>} = http_get(Config, "/cluster-name", "myuser", "myuser", ?OK),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

nodes_test(Config) ->
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/monitor", [{password, <<"monitor">>},
                                        {tags, <<"monitoring">>}], {group, '2xx'}),
    DiscNode = #{type => <<"disc">>, running => true},
    assert_list([DiscNode], http_get(Config, "/nodes")),
    assert_list([DiscNode], http_get(Config, "/nodes", "monitor", "monitor", ?OK)),
    http_get(Config, "/nodes", "user", "user", ?NOT_AUTHORISED),
    http_get(Config, "/nodes/does-not-exist", ?NOT_FOUND),
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(maps:get(name, Node)),
    assert_item(DiscNode, http_get(Config, Path, ?OK)),
    assert_item(DiscNode, http_get(Config, Path, "monitor", "monitor", ?OK)),
    http_get(Config, Path, "user", "user", ?NOT_AUTHORISED),
    http_delete(Config, "/users/user", {group, '2xx'}),
    http_delete(Config, "/users/monitor", {group, '2xx'}),
    passed.

memory_test(Config) ->
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(maps:get(name, Node)) ++ "/memory",
    Result = http_get(Config, Path, ?OK),
    assert_keys([memory], Result),
    Keys = [total, connection_readers, connection_writers, connection_channels,
            connection_other, queue_procs, plugins,
            other_proc, mnesia, mgmt_db, msg_index, other_ets, binary, code,
            atom, other_system, allocated_unused, reserved_unallocated],
    assert_keys(Keys, maps:get(memory, Result)),
    http_get(Config, "/nodes/nonode/memory", ?NOT_FOUND),
    %% Relative memory as a percentage of the total
    Result1 = http_get(Config, Path ++ "/relative", ?OK),
    assert_keys([memory], Result1),
    Breakdown = maps:get(memory, Result1),
    assert_keys(Keys, Breakdown),

    assert_item(#{total => 100}, Breakdown),
    %% allocated_unused and reserved_unallocated
    %% make this test pretty unpredictable
    assert_percentage(Breakdown, 20),
    http_get(Config, "/nodes/nonode/memory/relative", ?NOT_FOUND),
    passed.

ets_tables_memory_test(Config) ->
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(maps:get(name, Node)) ++ "/memory/ets",
    Result = http_get(Config, Path, ?OK),
    assert_keys([ets_tables_memory], Result),
    NonMgmtKeys = [tracked_connection, tracked_channel],
    Keys = [queue_stats, vhost_stats_coarse_conn_stats,
        connection_created_stats, channel_process_stats, consumer_stats,
        queue_msg_rates],
    assert_keys(Keys ++ NonMgmtKeys, maps:get(ets_tables_memory, Result)),
    http_get(Config, "/nodes/nonode/memory/ets", ?NOT_FOUND),
    %% Relative memory as a percentage of the total
    ResultRelative = http_get(Config, Path ++ "/relative", ?OK),
    assert_keys([ets_tables_memory], ResultRelative),
    Breakdown = maps:get(ets_tables_memory, ResultRelative),
    assert_keys(Keys, Breakdown),
    assert_item(#{total => 100}, Breakdown),
    assert_percentage(Breakdown),
    http_get(Config, "/nodes/nonode/memory/ets/relative", ?NOT_FOUND),

    ResultMgmt = http_get(Config, Path ++ "/management", ?OK),
    assert_keys([ets_tables_memory], ResultMgmt),
    assert_keys(Keys, maps:get(ets_tables_memory, ResultMgmt)),
    assert_no_keys(NonMgmtKeys, maps:get(ets_tables_memory, ResultMgmt)),

    ResultMgmtRelative = http_get(Config, Path ++ "/management/relative", ?OK),
    assert_keys([ets_tables_memory], ResultMgmtRelative),
    assert_keys(Keys, maps:get(ets_tables_memory, ResultMgmtRelative)),
    assert_no_keys(NonMgmtKeys, maps:get(ets_tables_memory, ResultMgmtRelative)),
    assert_item(#{total => 100}, maps:get(ets_tables_memory, ResultMgmtRelative)),
    assert_percentage(maps:get(ets_tables_memory, ResultMgmtRelative)),

    ResultUnknownFilter = http_get(Config, Path ++ "/blahblah", ?OK),
    #{ets_tables_memory := <<"no_tables">>} = ResultUnknownFilter,
    passed.

assert_percentage(Breakdown0) ->
    assert_percentage(Breakdown0, 0).

assert_percentage(Breakdown0, ExtraMargin) ->
    Breakdown = maps:to_list(Breakdown0),
    Total = lists:sum([P || {K, P} <- Breakdown, K =/= total]),
    AcceptableMargin = (length(Breakdown) - 1) + ExtraMargin,
    %% Rounding up and down can lose some digits. Never more than the number
    %% of items in the breakdown.
    case ((Total =< 100 + AcceptableMargin) andalso (Total >= 100 - AcceptableMargin)) of
        false ->
            throw({bad_percentage, Total, Breakdown});
        true ->
            ok
    end.

println(What) -> io:format("~tp~n", [What]).

auth_test(Config) ->
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"">>}], {group, '2xx'}),
    EmptyAuthResponseHeaders = test_auth(Config, ?NOT_AUTHORISED, []),
    ?assertEqual(true, lists:keymember("www-authenticate", 1,  EmptyAuthResponseHeaders)),
    %% NOTE: this one won't have www-authenticate in the response,
    %% because user/password are ok, tags are not
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("user", "user")]),
    %?assertEqual(true, lists:keymember("www-authenticate", 1,  WrongAuthResponseHeaders)),
    test_auth(Config, ?OK, [auth_header("guest", "guest")]),
    http_delete(Config, "/users/user", {group, '2xx'}),
    passed.

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
vhosts_test(Config) ->
    assert_list([#{name => <<"/">>}], http_get(Config, "/vhosts")),
    %% Create a new one
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    %% PUT should be idempotent
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    %% Check it's there
    assert_list([#{name => <<"/">>}, #{name => <<"myvhost">>}],
                http_get(Config, "/vhosts")),
    %% Check individually
    assert_item(#{name => <<"/">>}, http_get(Config, "/vhosts/%2F", ?OK)),
    assert_item(#{name => <<"myvhost">>},http_get(Config, "/vhosts/myvhost")),

    %% Crash it
    rabbit_ct_broker_helpers:force_vhost_failure(Config, <<"myvhost">>),
    [NodeData] = http_get(Config, "/nodes"),
    Node = binary_to_atom(maps:get(name, NodeData), utf8),
    assert_item(#{name => <<"myvhost">>, cluster_state => #{Node => <<"stopped">>}},
                http_get(Config, "/vhosts/myvhost")),

    %% Restart it
    http_post(Config, "/vhosts/myvhost/start/" ++ atom_to_list(Node), [], {group, '2xx'}),
    assert_item(#{name => <<"myvhost">>, cluster_state => #{Node => <<"running">>}},
                http_get(Config, "/vhosts/myvhost")),

    %% Delete it
    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),
    %% It's not there
    http_get(Config, "/vhosts/myvhost", ?NOT_FOUND),
    http_delete(Config, "/vhosts/myvhost", ?NOT_FOUND),

    passed.

vhosts_description_test(Config) ->
    http_put(Config, "/vhosts/myvhost", [{description, <<"vhost description">>},
                                         {tags, <<"tag1,tag2">>}], {group, '2xx'}),
    Expected = #{name => <<"myvhost">>,
                 metadata => #{
                               description => <<"vhost description">>,
                               %% this is an injected DQT
                               default_queue_type => <<"classic">>,
                               tags => [<<"tag1">>, <<"tag2">>]
                              }},
    assert_item(Expected, http_get(Config, "/vhosts/myvhost")),

    %% Delete it
    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),

    passed.

vhosts_trace_test(Config) ->
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    Disabled = #{name => <<"myvhost">>, tracing => false},
    Enabled  = #{name => <<"myvhost">>, tracing => true},
    assert_item(Disabled, http_get(Config, "/vhosts/myvhost")),
    http_put(Config, "/vhosts/myvhost", [{tracing, true}], {group, '2xx'}),
    assert_item(Enabled, http_get(Config, "/vhosts/myvhost")),
    http_put(Config, "/vhosts/myvhost", [{tracing, true}], {group, '2xx'}),
    assert_item(Enabled, http_get(Config, "/vhosts/myvhost")),
    http_put(Config, "/vhosts/myvhost", [{tracing, false}], {group, '2xx'}),
    assert_item(Disabled, http_get(Config, "/vhosts/myvhost")),
    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),

    passed.

users_test(Config) ->
    assert_item(#{name => <<"guest">>, tags => [<<"administrator">>]},
                http_get(Config, "/whoami")),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                    [rabbitmq_management, login_session_timeout, 100]),
    assert_item(#{name => <<"guest">>,
                    tags => [<<"administrator">>],
                    login_session_timeout => 100},
                http_get(Config, "/whoami")),
    http_delete(Config, "/users/users_test", [?NO_CONTENT, ?NOT_FOUND]),
    http_get(Config, "/users/users_test", [?NO_CONTENT, ?NOT_FOUND]),
    http_put_raw(Config, "/users/users_test", "Something not JSON", ?BAD_REQUEST),
    http_put(Config, "/users/users_test", [{flim, <<"flam">>}], ?BAD_REQUEST),
    http_put(Config, "/users/users_test", [{tags,     [<<"management">>]},
                                        {password, <<"users_test">>}],
                {group, '2xx'}),
    http_put(Config, "/users/users_test", [{password_hash, <<"not_hash">>}], ?BAD_REQUEST),
    http_put(Config, "/users/users_test", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                        {tags, <<"management">>}], {group, '2xx'}),
    assert_item(#{name => <<"users_test">>, tags => [<<"management">>],
                    password_hash => <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>,
                    hashing_algorithm => <<"rabbit_password_hashing_sha256">>},
                http_get(Config, "/users/users_test")),

    http_put(Config, "/users/users_test", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                        {hashing_algorithm, <<"rabbit_password_hashing_md5">>},
                                        {tags, [<<"management">>]}], {group, '2xx'}),
    assert_item(#{name => <<"users_test">>, tags => [<<"management">>],
                    password_hash => <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>,
                    hashing_algorithm => <<"rabbit_password_hashing_md5">>},
                http_get(Config, "/users/users_test")),
    http_put(Config, "/users/users_test", [{password, <<"password">>},
                                        {tags, [<<"administrator">>, <<"foo">>]}], {group, '2xx'}),
    assert_item(#{name => <<"users_test">>, tags => [<<"administrator">>, <<"foo">>]},
                http_get(Config, "/users/users_test")),
    Listed = lists:sort(http_get(Config, "/users")),
    ct:pal("Listed users: ~tp", [Listed]),
    User1 = #{name => <<"users_test">>, tags => [<<"administrator">>, <<"foo">>]},
    User2 = #{name => <<"guest">>, tags => [<<"administrator">>]},
    ?assert(lists:any(fun(U) ->
                maps:get(name, U) =:= maps:get(name, User1) andalso
                maps:get(tags, U) =:= maps:get(tags, User1)
            end, Listed)),
    ?assert(lists:any(fun(U) ->
                maps:get(name, U) =:= maps:get(name, User2) andalso
                maps:get(tags, U) =:= maps:get(tags, User2)
            end, Listed)),
    test_auth(Config, ?OK, [auth_header("users_test", "password")]),
    http_put(Config, "/users/users_test", [{password, <<"password">>},
                        {tags, []}], {group, '2xx'}),
    assert_item(#{name => <<"users_test">>, tags => []},
                http_get(Config, "/users/users_test")),
    http_delete(Config, "/users/users_test", {group, '2xx'}),
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("users_test", "password")]),
    http_get(Config, "/users/users_test", ?NOT_FOUND),
    passed.

without_permissions_users_test(Config) ->
    assert_item(#{name => <<"guest">>, tags => [<<"administrator">>]},
                http_get(Config, "/whoami")),
    http_put(Config, "/users/myuser", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    Perms = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/%2F/myuser", Perms, {group, '2xx'}),
    http_put(Config, "/users/myuserwithoutpermissions", [{password_hash,
                                                          <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                                         {tags, <<"management">>}], {group, '2xx'}),
    assert_list([#{name => <<"myuserwithoutpermissions">>, tags => [<<"management">>],
                   hashing_algorithm => <<"rabbit_password_hashing_sha256">>,
                   password_hash => <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>}],
                http_get(Config, "/users/without-permissions")),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    http_delete(Config, "/users/myuserwithoutpermissions", {group, '2xx'}),
    passed.

users_bulk_delete_test(Config) ->
    assert_item(#{name => <<"guest">>, tags => [<<"administrator">>]},
                http_get(Config, "/whoami")),
    http_put(Config, "/users/myuser1", [{tags, <<"management">>}, {password, <<"myuser">>}],
             {group, '2xx'}),
    http_put(Config, "/users/myuser2", [{tags, <<"management">>}, {password, <<"myuser">>}],
             {group, '2xx'}),
    http_put(Config, "/users/myuser3", [{tags, <<"management">>}, {password, <<"myuser">>}],
             {group, '2xx'}),
    http_get(Config, "/users/myuser1", {group, '2xx'}),
    http_get(Config, "/users/myuser2", {group, '2xx'}),
    http_get(Config, "/users/myuser3", {group, '2xx'}),

    http_post_json(Config, "/users/bulk-delete",
                   "{\"users\": [\"myuser1\", \"myuser2\"]}", {group, '2xx'}),
    http_get(Config, "/users/myuser1", ?NOT_FOUND),
    http_get(Config, "/users/myuser2", ?NOT_FOUND),
    http_get(Config, "/users/myuser3", {group, '2xx'}),
    http_post_json(Config, "/users/bulk-delete", "{\"users\": [\"myuser3\"]}",
                    {group, '2xx'}),
    http_get(Config, "/users/myuser3", ?NOT_FOUND),
    passed.

users_legacy_administrator_test(Config) ->
    http_put(Config, "/users/myuser1", [{administrator, <<"true">>},
                                        {password,      <<"myuser1">>}],
             {group, '2xx'}),
    http_put(Config, "/users/myuser2", [{administrator, <<"false">>},
                                        {password,      <<"myuser2">>}],
             {group, '2xx'}),
    assert_item(#{name => <<"myuser1">>, tags => [<<"administrator">>]},
                http_get(Config, "/users/myuser1")),
    assert_item(#{name => <<"myuser2">>, tags => []},
                http_get(Config, "/users/myuser2")),
    http_delete(Config, "/users/myuser1", {group, '2xx'}),
    http_delete(Config, "/users/myuser2", {group, '2xx'}),
    passed.

adding_a_user_with_password_test(Config) ->
    http_put(Config, "/users/user10", [{tags, <<"management">>},
                                       {password,      <<"password">>}],
             [?CREATED, ?NO_CONTENT]),
    http_delete(Config, "/users/user10", ?NO_CONTENT).
adding_a_user_with_password_hash_test(Config) ->
    http_put(Config, "/users/user11", [{tags, <<"management">>},
                                       %% SHA-256 of "secret"
                                       {password_hash, <<"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b">>}],
             [?CREATED, ?NO_CONTENT]),
    http_delete(Config, "/users/user11", ?NO_CONTENT).

adding_a_user_with_generated_password_hash_test(Config) ->
    #{ok := HashedPassword} = http_get(Config, "/auth/hash_password/some_password"),

    http_put(Config, "/users/user12", [{tags, <<"administrator">>},
                                       {password_hash, HashedPassword}],
             [?CREATED, ?NO_CONTENT]),
    % If the get succeeded, the hashed password generation is correct
    User = http_get(Config, "/users/user12", "user12", "some_password", ?OK),
    ?assertEqual(maps:get(password_hash, User), HashedPassword),
    http_delete(Config, "/users/user12", ?NO_CONTENT).

adding_a_user_with_permissions_in_single_operation_test(Config) ->
    QArgs = #{},
    PermArgs = #{configure => <<".*">>,
                 write     => <<".*">>,
                 read      => <<".*">>},
    http_delete(Config, "/vhosts/vhost42", [?NO_CONTENT, ?NOT_FOUND]),
    http_delete(Config, "/vhosts/vhost43", [?NO_CONTENT, ?NOT_FOUND]),
    http_delete(Config, "/users/user-preconfigured-perms",   [?NO_CONTENT, ?NOT_FOUND]),

    http_put(Config, "/vhosts/vhost42", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/vhosts/vhost43", none, [?CREATED, ?NO_CONTENT]),

    http_put(Config, "/users/user-preconfigured-perms", [{password, <<"user-preconfigured-perms">>},
                                       {tags, <<"management">>},
                                       {permissions, [
                                                      {<<"vhost42">>, PermArgs},
                                                      {<<"vhost43">>, PermArgs}
                                                     ]}],
             [?CREATED, ?NO_CONTENT]),
    assert_list([#{tracing => false, name => <<"vhost42">>},
                 #{tracing => false, name => <<"vhost43">>}],
                http_get(Config, "/vhosts", "user-preconfigured-perms", "user-preconfigured-perms", ?OK)),
    http_put(Config, "/queues/vhost42/myqueue", QArgs,
             "user-preconfigured-perms", "user-preconfigured-perms", [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/vhost43/myqueue", QArgs,
             "user-preconfigured-perms", "user-preconfigured-perms", [?CREATED, ?NO_CONTENT]),
    Test1 =
        fun(Path) ->
                http_get(Config, Path, "user-preconfigured-perms", "user-preconfigured-perms", ?OK)
        end,
    Test2 =
        fun(Path1, Path2) ->
                http_get(Config, Path1 ++ "/vhost42/" ++ Path2, "user-preconfigured-perms", "user-preconfigured-perms",
                         ?OK),
                http_get(Config, Path1 ++ "/vhost43/" ++ Path2, "user-preconfigured-perms", "user-preconfigured-perms",
                         ?OK)
        end,
    Test1("/exchanges"),
    Test2("/exchanges", ""),
    Test2("/exchanges", "amq.direct"),
    Test1("/queues"),
    Test2("/queues", ""),
    Test2("/queues", "myqueue"),
    Test1("/bindings"),
    Test2("/bindings", ""),
    Test2("/queues", "myqueue/bindings"),
    Test2("/exchanges", "amq.default/bindings/source"),
    Test2("/exchanges", "amq.default/bindings/destination"),
    Test2("/bindings", "e/amq.default/q/myqueue"),
    Test2("/bindings", "e/amq.default/q/myqueue/myqueue"),
    http_delete(Config, "/vhosts/vhost42", ?NO_CONTENT),
    http_delete(Config, "/vhosts/vhost43", ?NO_CONTENT),
    http_delete(Config, "/users/user-preconfigured-perms", ?NO_CONTENT),
    passed.

adding_a_user_without_tags_fails_test(Config) ->
    http_put(Config, "/users/no-tags", [{password, <<"password">>}], ?BAD_REQUEST).

%% creating a passwordless user makes sense when x509x certificates or another
%% "external" authentication mechanism or backend is used.
%% See rabbitmq/rabbitmq-management#383.
adding_a_user_without_password_or_hash_test(Config) ->
    http_put(Config, "/users/no-pwd", [{tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/no-pwd", [{tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_delete(Config, "/users/no-pwd", ?NO_CONTENT).

adding_a_user_with_both_password_and_hash_fails_test(Config) ->
    http_put(Config, "/users/myuser", [{password,      <<"password">>},
                                       {password_hash, <<"password_hash">>}],
             ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{tags, <<"management">>},
                                       {password,      <<"password">>},
                                       {password_hash, <<"password_hash">>}], ?BAD_REQUEST).

updating_a_user_without_password_or_hash_clears_password_test(Config) ->
    http_put(Config, "/users/myuser", [{tags,     <<"management">>},
                                       {password, <<"myuser">>}], [?CREATED, ?NO_CONTENT]),
    %% in this case providing no password or password_hash will
    %% clear users' credentials
    http_put(Config, "/users/myuser", [{tags,     <<"management">>}], [?CREATED, ?NO_CONTENT]),
    assert_item(#{name => <<"myuser">>,
                  tags => [<<"management">>],
                  password_hash => <<>>,
                  hashing_algorithm => <<"rabbit_password_hashing_sha256">>},
                http_get(Config, "/users/myuser")),
    http_delete(Config, "/users/myuser", ?NO_CONTENT).

-define(NON_GUEST_USERNAME, <<"abc">>).

user_credential_validation_accept_everything_succeeds_test(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?NON_GUEST_USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything),
    http_put(Config, "/users/abc", [{password, <<"password">>},
                                    {tags, <<"management">>}], {group, '2xx'}),
    rabbit_ct_broker_helpers:delete_user(Config, ?NON_GUEST_USERNAME).

user_credential_validation_min_length_succeeds_test(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?NON_GUEST_USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, min_length, 5),
    http_put(Config, "/users/abc", [{password, <<"password">>},
                                    {tags, <<"management">>}], {group, '2xx'}),
    rabbit_ct_broker_helpers:delete_user(Config, ?NON_GUEST_USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything).

user_credential_validation_min_length_fails_test(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?NON_GUEST_USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, min_length, 5),
    http_put(Config, "/users/abc", [{password, <<"_">>},
                                    {tags, <<"management">>}], ?BAD_REQUEST),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything).

updating_tags_of_a_passwordless_user_test(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?NON_GUEST_USERNAME),
    http_put(Config, "/users/abc", [{tags,     <<"management">>},
                                    {password, <<"myuser">>}], [?CREATED, ?NO_CONTENT]),

    %% clear user's password
    http_put(Config, "/users/abc", [{tags,     <<"management">>}], [?CREATED, ?NO_CONTENT]),
    assert_item(#{name => ?NON_GUEST_USERNAME,
                  tags => [<<"management">>],
                  password_hash => <<>>,
                  hashing_algorithm => <<"rabbit_password_hashing_sha256">>},
                http_get(Config, "/users/abc")),

    http_put(Config, "/users/abc", [{tags,     <<"impersonator">>}], [?CREATED, ?NO_CONTENT]),
    assert_item(#{name => ?NON_GUEST_USERNAME,
                  tags => [<<"impersonator">>],
                  password_hash => <<>>,
                  hashing_algorithm => <<"rabbit_password_hashing_sha256">>},
                http_get(Config, "/users/abc")),

    http_put(Config, "/users/abc", [{tags,     <<"">>}], [?CREATED, ?NO_CONTENT]),
    assert_item(#{name => ?NON_GUEST_USERNAME,
                  tags => [],
                  password_hash => <<>>,
                  hashing_algorithm => <<"rabbit_password_hashing_sha256">>},
                http_get(Config, "/users/abc")),

    http_delete(Config, "/users/abc", ?NO_CONTENT).

permissions_validation_test(Config) ->
    Good = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/wrong/guest", Good, ?BAD_REQUEST),
    http_put(Config, "/permissions/%2F/wrong", Good, ?BAD_REQUEST),
    http_put(Config, "/permissions/%2F/guest",
             [{configure, <<"[">>}, {write, <<".*">>}, {read, <<".*">>}],
             ?BAD_REQUEST),
    http_put(Config, "/permissions/%2F/guest", Good, {group, '2xx'}),
    passed.

permissions_list_test(Config) ->
    AllPerms            = http_get(Config, "/permissions"),
    GuestInDefaultVHost = #{user      => <<"guest">>,
                            vhost     => <<"/">>,
                            configure => <<".*">>,
                            write     => <<".*">>,
                            read      => <<".*">>},

    ?assert(lists:member(GuestInDefaultVHost, AllPerms)),

    http_put(Config, "/users/myuser1", [{password, <<"">>}, {tags, <<"administrator">>}],
             {group, '2xx'}),
    http_put(Config, "/users/myuser2", [{password, <<"">>}, {tags, <<"administrator">>}],
             {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost1", none, {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost2", none, {group, '2xx'}),

    Perms = [{configure, <<"foo">>}, {write, <<"foo">>}, {read, <<"foo">>}],
    http_put(Config, "/permissions/myvhost1/myuser1", Perms, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost2/myuser1", Perms, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost1/myuser2", Perms, {group, '2xx'}),

    %% The user that creates the vhosts gets permission automatically
    %% See https://github.com/rabbitmq/rabbitmq-management/issues/444
    ?assertEqual(6, length(http_get(Config, "/permissions"))),
    ?assertEqual(2, length(http_get(Config, "/users/myuser1/permissions"))),
    ?assertEqual(1, length(http_get(Config, "/users/myuser2/permissions"))),

    http_get(Config, "/users/notmyuser/permissions", ?NOT_FOUND),
    http_get(Config, "/vhosts/notmyvhost/permissions", ?NOT_FOUND),

    http_delete(Config, "/users/myuser1",   {group, '2xx'}),
    http_delete(Config, "/users/myuser2",   {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost1", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost2", {group, '2xx'}),
    passed.

permissions_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"myuser">>}, {tags, <<"administrator">>}],
             {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),

    http_put(Config, "/permissions/myvhost/myuser",
             [{configure, <<"foo">>}, {write, <<"foo">>}, {read, <<"foo">>}],
             {group, '2xx'}),

    Permission = #{user => <<"myuser">>,
                   vhost => <<"myvhost">>,
                   configure => <<"foo">>,
                   write => <<"foo">>,
                   read => <<"foo">>},
    %% The user that creates the vhosts gets permission automatically
    %% See https://github.com/rabbitmq/rabbitmq-management/issues/444
    PermissionOwner = #{user => <<"guest">>,
                        vhost => <<"/">>,
                        configure => <<".*">>,
                        write => <<".*">>,
                        read => <<".*">>},
    Default = #{user => <<"guest">>,
                vhost => <<"/">>,
                configure => <<".*">>,
                write => <<".*">>,
                read => <<".*">>},
    Permission = http_get(Config, "/permissions/myvhost/myuser"),
    assert_list(lists:sort([Permission, PermissionOwner, Default]),
                lists:sort(http_get(Config, "/permissions"))),
    assert_list([Permission], http_get(Config, "/users/myuser/permissions")),
    http_delete(Config, "/permissions/myvhost/myuser", {group, '2xx'}),
    http_get(Config, "/permissions/myvhost/myuser", ?NOT_FOUND),

    http_delete(Config, "/users/myuser", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),
    passed.

topic_permissions_list_test(Config) ->
    http_put(Config, "/users/myuser1", [{password, <<"">>}, {tags, <<"administrator">>}],
        {group, '2xx'}),
    http_put(Config, "/users/myuser2", [{password, <<"">>}, {tags, <<"administrator">>}],
        {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost1", none, {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost2", none, {group, '2xx'}),

    TopicPerms = [{exchange, <<"amq.topic">>}, {write, <<"^a">>}, {read, <<"^b">>}],
    http_put(Config, "/topic-permissions/myvhost1/myuser1", TopicPerms, {group, '2xx'}),
    http_put(Config, "/topic-permissions/myvhost2/myuser1", TopicPerms, {group, '2xx'}),
    http_put(Config, "/topic-permissions/myvhost1/myuser2", TopicPerms, {group, '2xx'}),

    TopicPerms2 = [{exchange, <<"amq.direct">>}, {write, <<"^a">>}, {read, <<"^b">>}],
    http_put(Config, "/topic-permissions/myvhost1/myuser1", TopicPerms2, {group, '2xx'}),

    4 = length(http_get(Config, "/topic-permissions")),
    3 = length(http_get(Config, "/users/myuser1/topic-permissions")),
    1 = length(http_get(Config, "/users/myuser2/topic-permissions")),
    3 = length(http_get(Config, "/vhosts/myvhost1/topic-permissions")),
    1 = length(http_get(Config, "/vhosts/myvhost2/topic-permissions")),

    http_get(Config, "/users/notmyuser/topic-permissions", ?NOT_FOUND),
    http_get(Config, "/vhosts/notmyvhost/topic-permissions", ?NOT_FOUND),

    %% Delete permissions for a single vhost-user-exchange combination
    http_delete(Config, "/topic-permissions/myvhost1/myuser1/amq.direct", {group, '2xx'}),
    3 = length(http_get(Config, "/topic-permissions")),

    http_delete(Config, "/users/myuser1", {group, '2xx'}),
    http_delete(Config, "/users/myuser2", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost1", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost2", {group, '2xx'}),
    passed.

topic_permissions_test(Config) ->
    http_put(Config, "/users/myuser1", [{password, <<"">>}, {tags, <<"administrator">>}],
        {group, '2xx'}),
    http_put(Config, "/users/myuser2", [{password, <<"">>}, {tags, <<"administrator">>}],
        {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost1", none, {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost2", none, {group, '2xx'}),

    TopicPerms = [{exchange, <<"amq.topic">>}, {write, <<"^a">>}, {read, <<"^b">>}],
    http_put(Config, "/topic-permissions/myvhost1/myuser1", TopicPerms, {group, '2xx'}),
    http_put(Config, "/topic-permissions/myvhost2/myuser1", TopicPerms, {group, '2xx'}),
    http_put(Config, "/topic-permissions/myvhost1/myuser2", TopicPerms, {group, '2xx'}),

    3 = length(http_get(Config, "/topic-permissions")),
    1 = length(http_get(Config, "/topic-permissions/myvhost1/myuser1")),
    1 = length(http_get(Config, "/topic-permissions/myvhost2/myuser1")),
    1 = length(http_get(Config, "/topic-permissions/myvhost1/myuser2")),

    http_get(Config, "/topic-permissions/myvhost1/notmyuser", ?NOT_FOUND),
    http_get(Config, "/topic-permissions/notmyvhost/myuser2", ?NOT_FOUND),

    http_delete(Config, "/users/myuser1", {group, '2xx'}),
    http_delete(Config, "/users/myuser2", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost1", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost2", {group, '2xx'}),
    passed.

connections_amqpl(Config) ->
    {Conn, _Ch} = open_connection_and_channel(Config),
    LocalPort = local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w",
               [LocalPort, amqp_port(Config)])),
    await_condition(
      fun () ->
              Connection = http_get(Config, Path, ?OK),
              ?assert(maps:is_key(recv_oct, Connection)),
              ?assert(maps:is_key(garbage_collection, Connection)),
              ?assert(maps:is_key(send_oct_details, Connection)),
              ?assert(maps:is_key(reductions, Connection)),
              true
      end),
    http_delete(Config, Path, {group, '2xx'}),
    %% TODO rabbit_reader:shutdown/2 returns before the connection is
    %% closed. It may not be worth fixing.
    Fun = fun() ->
                  try
                      http_get(Config, Path, ?NOT_FOUND),
                      true
                  catch
                      _:_ ->
                          false
                  end
          end,
    await_condition(Fun),
    close_connection(Conn),
    passed.

%% Test that AMQP 1.0 connection can be listed and closed via the rabbitmq_management plugin.
connections_amqp(Config) ->
    Node = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    User = <<"guest">>,
    OpnConf = #{address => ?config(rmq_hostname, Config),
                port => Port,
                container_id => <<"my container">>,
                sasl => {plain, User, <<"guest">>}},
    {ok, C1} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C1, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,
    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),
    ?assertEqual(1, length(rpc(Config, rabbit_amqp1_0, list_local, []))),
    [Connection1] = http_get(Config, "/connections"),
    ?assertMatch(#{node := Node,
                   vhost := <<"/">>,
                   user := User,
                   auth_mechanism := <<"PLAIN">>,
                   protocol := <<"AMQP 1-0">>,
                   client_properties := #{version := _,
                                          product := <<"AMQP 1.0 client">>,
                                          platform := _}},
                 Connection1),
    ConnectionName = maps:get(name, Connection1),
    http_delete(Config,
                "/connections/" ++ binary_to_list(uri_string:quote(ConnectionName)),
                ?NO_CONTENT),
    receive {amqp10_event,
             {connection, C1,
              {closed,
               {internal_error,
                <<"Connection forced: \"Closed via management plugin\"">>}}}} -> ok
    after 5000 -> ct:fail(closed_timeout)
    end,
    eventually(?_assertNot(is_process_alive(C1))),
    eventually(?_assertEqual([], http_get(Config, "/connections")), 10, 5),

    {ok, C2} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,
    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),
    http_delete(Config,
                "/connections/username/guest",
                ?NO_CONTENT),
    receive {amqp10_event,
             {connection, C2,
              {closed,
               {internal_error,
                <<"Connection forced: \"Closed via management plugin\"">>}}}} -> ok
    after 5000 -> ct:fail(closed_timeout)
    end,
    eventually(?_assertNot(is_process_alive(C2))),
    eventually(?_assertEqual([], http_get(Config, "/connections")), 10, 5),
    ?assertEqual(0, length(rpc(Config, rabbit_amqp1_0, list_local, []))).

%% Test that AMQP 1.0 sessions and links can be listed via the rabbitmq_management plugin.
amqp_sessions(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    User = <<"guest">>,
    OpnConf = #{address => ?config(rmq_hostname, Config),
                port => Port,
                container_id => <<"my container">>,
                sasl => {plain, User, <<"guest">>}},
    {ok, C} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,

    {ok, Session1} = amqp10_client:begin_session_sync(C),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(
                       Session1, <<"my link pair">>),
    QName = <<"my stream">>,
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}},
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(
                     Session1, <<"my sender">>,
                     rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"my key">>)),

    Filter = #{<<"ts filter">> => #filter{descriptor = <<"rabbitmq:stream-offset-spec">>,
                                          value = {timestamp, 1751023462000}},
               <<"bloom filter">> => #filter{descriptor = <<"rabbitmq:stream-filter">>,
                                             value = {list, [{utf8, <<"complaint">>},
                                                             {utf8, <<"user1">>}]}},
               <<"match filter">> => #filter{descriptor = <<"rabbitmq:stream-match-unfiltered">>,
                                             value = {boolean, true}},
               <<"prop filter">> => #filter{descriptor = ?DESCRIPTOR_CODE_PROPERTIES_FILTER,
                                            value = {map, [{{symbol, <<"subject">>},
                                                            {utf8, <<"complaint">>}},
                                                           {{symbol, <<"user-id">>},
                                                            {binary, <<"user1">>}}
                                                          ]}},
               <<"app prop filter">> => #filter{descriptor = ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER,
                                                value = {map, [{{utf8, <<"k1">>}, {int, -4}},
                                                               {{utf8, <<"☀️"/utf8>>}, {utf8, <<"🙂"/utf8>>}}
                                                              ]}}},
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session1, <<"my receiver">>,
                       rabbitmq_amqp_address:queue(QName),
                       settled, none, Filter),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver, 5000, never),

    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),
    [Connection] = http_get(Config, "/connections"),
    ConnectionName = maps:get(name, Connection),
    Path = "/connections/" ++ binary_to_list(uri_string:quote(ConnectionName)) ++ "/sessions",
    [Session] = http_get(Config, Path),
    ?assertMatch(
       #{channel_number := 0,
         handle_max := HandleMax,
         next_incoming_id := NextIncomingId,
         incoming_window := IncomingWindow,
         next_outgoing_id := NextOutgoingId,
         remote_incoming_window := RemoteIncomingWindow,
         remote_outgoing_window := RemoteOutgoingWindow,
         outgoing_unsettled_deliveries := 0
        } when is_integer(HandleMax) andalso
               is_integer(NextIncomingId) andalso
               is_integer(IncomingWindow) andalso
               is_integer(NextOutgoingId) andalso
               is_integer(RemoteIncomingWindow) andalso
               is_integer(RemoteOutgoingWindow),
               Session),

    {ok, IncomingLinks} = maps:find(incoming_links, Session),
    {ok, OutgoingLinks} = maps:find(outgoing_links, Session),
    ?assertEqual(2, length(IncomingLinks)),
    ?assertEqual(2, length(OutgoingLinks)),

    ?assertMatch([#{handle := 0,
                    link_name := <<"my link pair">>,
                    target_address := <<"/management">>,
                    delivery_count := DeliveryCount1,
                    credit := Credit1,
                    snd_settle_mode := <<"settled">>,
                    max_message_size := IncomingMaxMsgSize,
                    unconfirmed_messages := 0},
                  #{handle := 2,
                    link_name := <<"my sender">>,
                    target_address := <<"/exchanges/amq.direct/my%20key">>,
                    delivery_count := DeliveryCount2,
                    credit := Credit2,
                    snd_settle_mode := <<"mixed">>,
                    max_message_size := IncomingMaxMsgSize,
                    unconfirmed_messages := 0}]
                   when is_integer(Credit1) andalso
                        is_integer(Credit2) andalso
                        is_integer(IncomingMaxMsgSize) andalso
                        is_integer(DeliveryCount1) andalso
                        is_integer(DeliveryCount2),
                        IncomingLinks),

    [OutLink1, OutLink2] = OutgoingLinks,
    ?assertMatch(#{handle := 1,
                   link_name := <<"my link pair">>,
                   source_address := <<"/management">>,
                   queue_name := <<>>,
                   delivery_count := DeliveryCount3,
                   credit := 0,
                   max_message_size := <<"unlimited">>,
                   send_settled := true}
                   when is_integer(DeliveryCount3),
                        OutLink1),
    #{handle := 3,
      link_name := <<"my receiver">>,
      source_address := <<"/queues/my%20stream">>,
      queue_name := <<"my stream">>,
      delivery_count := DeliveryCount4,
      credit := 5000,
      max_message_size := <<"unlimited">>,
      send_settled := true,
      filter := ActualFilter} = OutLink2,
    ?assert(is_integer(DeliveryCount4)),
    ExpectedFilter = [#{name => <<"ts filter">>,
                        descriptor => <<"rabbitmq:stream-offset-spec">>,
                        value => 1751023462000},
                      #{name => <<"bloom filter">>,
                        descriptor => <<"rabbitmq:stream-filter">>,
                        value => [<<"complaint">>, <<"user1">>]},
                      #{name => <<"match filter">>,
                        descriptor => <<"rabbitmq:stream-match-unfiltered">>,
                        value => true},
                      #{name => <<"prop filter">>,
                        descriptor => ?DESCRIPTOR_CODE_PROPERTIES_FILTER,
                        value => [#{key => <<"subject">>, value => <<"complaint">>},
                                  #{key => <<"user-id">>, value => <<"user1">>}]},
                      #{name => <<"app prop filter">>,
                        descriptor => ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER,
                        value => [#{key => <<"k1">>, value => -4},
                                  #{key => <<"☀️"/utf8>>, value => <<"🙂"/utf8>>}]}],
    ?assertEqual(lists:sort(ExpectedFilter),
                 lists:sort(ActualFilter)),

    {ok, _Session2} = amqp10_client:begin_session_sync(C),
    Sessions = http_get(Config, Path),
    ?assertEqual(2, length(Sessions)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:close_connection(C).

%% Test that GET /connections/:name/sessions returns
%% 400 Bad Request for non-AMQP 1.0 connections.
amqpl_sessions(Config) ->
    {Conn, _Ch} = open_connection_and_channel(Config),
    LocalPort = local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w/sessions",
               [LocalPort, amqp_port(Config)])),
    ok = await_condition(
           fun() ->
                   http_get(Config, Path, 400),
                   true
           end).

%% Test that AMQP 1.0 connection can be listed if the rabbitmq_management plugin gets enabled
%% after the connection was established.
enable_plugin_amqp(Config) ->
    ?assertEqual(0, length(http_get(Config, "/connections"))),

    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management_agent),

    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => ?config(rmq_hostname, Config),
                port => Port,
                container_id => <<"my container">>,
                sasl => anon},
    {ok, Conn} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Conn, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,

    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management_agent),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management),
    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),

    ok = amqp10_client:close_connection(Conn),
    receive {amqp10_event, {connection, Conn, {closed, normal}}} -> ok
    after 5000 -> ct:fail({connection_close_timeout, Conn})
    end.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~ts flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

multiple_invalid_connections_test(Config) ->
    Count = 100,
    spawn_invalid(Config, Count),
    Page0 = http_get(Config, "/connections?page=1&page_size=100", ?OK),
    wait_for_answers(Count),
    Page1 = http_get(Config, "/connections?page=1&page_size=100", ?OK),
    ?assertEqual(0, maps:get(total_count, Page0)),
    ?assertEqual(0, maps:get(total_count, Page1)),
    passed.

test_auth(Config, Code, Headers) ->
    {ok, {{_, Code, _}, RespHeaders, _}} = req(Config, get, "/overview", Headers),
    RespHeaders.

exchanges_test(Config) ->
    %% Can list exchanges
    http_get(Config, "/exchanges", {group, '2xx'}),
    %% Can pass booleans or strings
    Good = [{type, <<"direct">>}, {durable, <<"true">>}],
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    http_get(Config, "/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put(Config, "/exchanges/myvhost/foo", Good, {group, '2xx'}),
    http_put(Config, "/exchanges/myvhost/foo", Good, {group, '2xx'}),
    http_get(Config, "/exchanges/%2F/foo", ?NOT_FOUND),
    assert_item(#{name => <<"foo">>,
                  vhost => <<"myvhost">>,
                  type => <<"direct">>,
                  durable => true,
                  auto_delete => false,
                  internal => false,
                  arguments => #{}},
                http_get(Config, "/exchanges/myvhost/foo")),
    http_put(Config, "/exchanges/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/exchanges/myvhost/bar", [{type, <<"bad_exchange_type">>}],
             ?BAD_REQUEST),
    http_put(Config, "/exchanges/myvhost/bar", [{type, <<"direct">>},
                                                {durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/exchanges/myvhost/foo", [{type, <<"direct">>}],
             ?BAD_REQUEST),

    http_delete(Config, "/exchanges/myvhost/foo", {group, '2xx'}),
    http_delete(Config, "/exchanges/myvhost/foo", ?NOT_FOUND),

    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),
    http_get(Config, "/exchanges/badvhost", ?NOT_FOUND),
    passed.

queues_test(Config) ->
    Good = [{durable, true}],
    http_get(Config, "/queues/%2F/foo", ?NOT_FOUND),
    http_put(Config, "/queues/%2F/foo", Good, {group, '2xx'}),
    http_put(Config, "/queues/%2F/foo", Good, {group, '2xx'}),
    http_get(Config, "/queues/%2F/foo", ?OK),

    rabbit_ct_broker_helpers:add_vhost(Config, <<"downvhost">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"downvhost">>),
    http_put(Config, "/queues/downvhost/foo", Good, {group, '2xx'}),
    http_put(Config, "/queues/downvhost/bar", Good, {group, '2xx'}),

    rabbit_ct_broker_helpers:force_vhost_failure(Config, <<"downvhost">>),
    %% The vhost is down
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    DownVHost = #{name => <<"downvhost">>, tracing => false, cluster_state => #{Node => <<"stopped">>}},
    assert_item(DownVHost, http_get(Config, "/vhosts/downvhost")),

    DownQueues = http_get(Config, "/queues/downvhost"),
    DownQueue  = http_get(Config, "/queues/downvhost/foo"),

    assert_list([#{name        => <<"bar">>,
                   vhost       => <<"downvhost">>,
                   state       => <<"stopped">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{'x-queue-type' => <<"classic">>}
                 },
                 #{name        => <<"foo">>,
                   vhost       => <<"downvhost">>,
                   state       => <<"stopped">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{'x-queue-type' => <<"classic">>}
                 }], DownQueues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"downvhost">>,
                  state       => <<"stopped">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{'x-queue-type' => <<"classic">>}
                }, DownQueue),

    http_put(Config, "/queues/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/queues/%2F/bar",
             [{durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/queues/%2F/foo",
             [{durable, false}],
             ?BAD_REQUEST),

    http_put(Config, "/queues/%2F/baz", Good, {group, '2xx'}),
    %% Wait until metrics are emitted and stats collected
    ?awaitMatch(true, maps:is_key(storage_version,
                                  http_get(Config, "/queues/%2F/baz")),
                30000),
    Queues = http_get(Config, "/queues/%2F"),
    Queue = http_get(Config, "/queues/%2F/foo"),
    assert_list([#{name        => <<"baz">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{'x-queue-type' => <<"classic">>},
                   storage_version => 2},
                 #{name        => <<"foo">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{'x-queue-type' => <<"classic">>},
                   storage_version => 2}], Queues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"/">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{'x-queue-type' => <<"classic">>},
                  storage_version => 2}, Queue),

    http_delete(Config, "/queues/%2F/foo", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/baz", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/foo", ?NOT_FOUND),
    http_get(Config, "/queues/badvhost", ?NOT_FOUND),

    http_delete(Config, "/queues/downvhost/foo", {group, '2xx'}),
    http_delete(Config, "/queues/downvhost/bar", {group, '2xx'}),
    passed.

quorum_queues_test(Config) ->
    %% Test in a loop that no metrics are left behing after deleting a queue
    quorum_queues_test_loop(Config, 2).

quorum_queues_test_loop(_Config, 0) ->
    passed;
quorum_queues_test_loop(Config, N) ->
    Good = [{durable, true}, {arguments, [{'x-queue-type', 'quorum'}]}],
    http_get(Config, "/queues/%2f/qq", ?NOT_FOUND),
    http_put(Config, "/queues/%2f/qq", Good, {group, '2xx'}),

    {Conn, Ch} = open_connection_and_channel(Config),
    Publish = fun() ->
                      amqp_channel:call(
                        Ch, #'basic.publish'{exchange = <<"">>,
                                             routing_key = <<"qq">>},
                        #amqp_msg{payload = <<"message">>})
              end,
    Publish(),
    Publish(),
    rabbit_ct_helpers:await_condition(
      fun() ->
              Num = maps:get(messages, http_get(Config, "/queues/%2f/qq?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5"), undefined),
              2 == Num
      end, ?COLLECT_INTERVAL * 100),

    http_delete(Config, "/queues/%2f/qq", {group, '2xx'}),
    http_put(Config, "/queues/%2f/qq", Good, {group, '2xx'}),

    rabbit_ct_helpers:await_condition(
      fun() ->
              0 == maps:get(messages, http_get(Config, "/queues/%2f/qq?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5"), undefined)
      end, ?COLLECT_INTERVAL * 100),

    http_delete(Config, "/queues/%2f/qq", {group, '2xx'}),
    close_connection(Conn),
    quorum_queues_test_loop(Config, N-1).

stream_queues_have_consumers_field(Config) ->
    Good = [{durable, true}, {arguments, [{'x-queue-type', 'stream'}]}],
    http_get(Config, "/queues/%2f/sq", ?NOT_FOUND),
    http_put(Config, "/queues/%2f/sq", Good, {group, '2xx'}),

    rabbit_ct_helpers:await_condition(
      fun() ->
              Qs = http_get(Config, "/queues/%2F"),
              length(Qs) == 1 andalso maps:is_key(consumers, lists:nth(1, Qs))
      end, ?COLLECT_INTERVAL * 100),

    Queues = http_get(Config, "/queues/%2F"),
    assert_list([#{name        => <<"sq">>,
                   arguments => #{'x-queue-type' => <<"stream">>},
                   consumers   => 0}],
                Queues),

    http_delete(Config, "/queues/%2f/sq", {group, '2xx'}),
    ok.

bindings_test(Config) ->
    XArgs = [{type, <<"direct">>}],
    QArgs = #{},
    http_put(Config, "/exchanges/%2F/myexchange", XArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/myqueue", QArgs, {group, '2xx'}),
    BArgs = [{routing_key, <<"routing">>}, {arguments, []}],
    http_post(Config, "/bindings/%2F/e/myexchange/q/myqueue", BArgs, {group, '2xx'}),
    http_get(Config, "/bindings/%2F/e/myexchange/q/myqueue/routing", ?OK),
    http_get(Config, "/bindings/%2F/e/myexchange/q/myqueue/rooting", ?NOT_FOUND),
    Binding =
        #{source => <<"myexchange">>,
          vhost => <<"/">>,
          destination => <<"myqueue">>,
          destination_type => <<"queue">>,
          routing_key => <<"routing">>,
          arguments => #{},
          properties_key => <<"routing">>},
    DBinding =
        #{source => <<"">>,
          vhost => <<"/">>,
          destination => <<"myqueue">>,
          destination_type => <<"queue">>,
          routing_key => <<"myqueue">>,
          arguments => #{},
          properties_key => <<"myqueue">>},
    Binding = http_get(Config, "/bindings/%2F/e/myexchange/q/myqueue/routing"),
    assert_list([Binding],
                http_get(Config, "/bindings/%2F/e/myexchange/q/myqueue")),
    assert_list([DBinding, Binding],
                http_get(Config, "/queues/%2F/myqueue/bindings")),
    assert_list([Binding],
                http_get(Config, "/exchanges/%2F/myexchange/bindings/source")),
    http_delete(Config, "/bindings/%2F/e/myexchange/q/myqueue/routing", {group, '2xx'}),
    http_delete(Config, "/bindings/%2F/e/myexchange/q/myqueue/routing", ?NOT_FOUND),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/myqueue", {group, '2xx'}),
    http_get(Config, "/bindings/badvhost", ?NOT_FOUND),
    http_get(Config, "/bindings/badvhost/myqueue/myexchange/routing", ?NOT_FOUND),
    http_get(Config, "/bindings/%2F/e/myexchange/q/myqueue/routing", ?NOT_FOUND),
    passed.

bindings_post_test(Config) ->
    XArgs = [{type, <<"direct">>}],
    QArgs = #{},
    BArgs = [{routing_key, <<"routing">>}, {arguments, [{foo, <<"bar">>}]}],
    http_put(Config, "/exchanges/%2F/myexchange", XArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/myqueue", QArgs, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/myexchange/q/badqueue", BArgs, ?NOT_FOUND),
    http_post(Config, "/bindings/%2F/e/badexchange/q/myqueue", BArgs, ?NOT_FOUND),

    Headers1 = http_post(Config, "/bindings/%2F/e/myexchange/q/myqueue", #{}, {group, '2xx'}),
    Want0 = "myqueue/\~",
    ?assertEqual(Want0, pget("location", Headers1)),

    Headers2 = http_post(Config, "/bindings/%2F/e/myexchange/q/myqueue", BArgs, {group, '2xx'}),
    %% Args hash is calculated from a table, generated from args.
    Hash = table_hash([{<<"foo">>,longstr,<<"bar">>}]),
    PropertiesKey = "routing\~" ++ Hash,

    Want1 = "myqueue/" ++ PropertiesKey,
    ?assertEqual(Want1, pget("location", Headers2)),

    PropertiesKeyBin = list_to_binary(PropertiesKey),
    Want2 = #{source => <<"myexchange">>,
              vhost => <<"/">>,
              destination => <<"myqueue">>,
              destination_type => <<"queue">>,
              routing_key => <<"routing">>,
              arguments => #{foo => <<"bar">>},
              properties_key => PropertiesKeyBin},
    URI = "/bindings/%2F/e/myexchange/q/myqueue/" ++ PropertiesKey,
    ?assertEqual(Want2, http_get(Config, URI, ?OK)),

    http_get(Config, URI ++ "x", ?NOT_FOUND),
    http_delete(Config, URI, {group, '2xx'}),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/myqueue", {group, '2xx'}),
    passed.

bindings_null_routing_key_test(Config) ->
    http_delete(Config, "/exchanges/%2F/myexchange", {one_of, [201, 404]}),
    XArgs = [{type, <<"direct">>}],
    QArgs = #{},
    BArgs = [{routing_key, null}, {arguments, #{}}],
    http_put(Config, "/exchanges/%2F/myexchange", XArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/myqueue", QArgs, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/myexchange/q/badqueue", BArgs, ?NOT_FOUND),
    http_post(Config, "/bindings/%2F/e/badexchange/q/myqueue", BArgs, ?NOT_FOUND),

    Headers1 = http_post(Config, "/bindings/%2F/e/myexchange/q/myqueue", #{}, {group, '2xx'}),
    Want0 = "myqueue/\~",
    ?assertEqual(Want0, pget("location", Headers1)),

    Headers2 = http_post(Config, "/bindings/%2F/e/myexchange/q/myqueue", BArgs, {group, '2xx'}),
    %% Args hash is calculated from a table, generated from args.
    Hash = table_hash([]),
    PropertiesKey = "null\~" ++ Hash,

    ?assertEqual("myqueue/null", pget("location", Headers2)),
    Want1 = #{arguments => #{},
              destination => <<"myqueue">>,
              destination_type => <<"queue">>,
              properties_key => <<"null">>,
              routing_key => null,
              source => <<"myexchange">>,
              vhost => <<"/">>},
    URI = "/bindings/%2F/e/myexchange/q/myqueue/" ++ PropertiesKey,
    ?assertEqual(Want1, http_get(Config, URI, ?OK)),

    http_get(Config, URI ++ "x", ?NOT_FOUND),
    http_delete(Config, URI, {group, '2xx'}),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/myqueue", {group, '2xx'}),
    passed.

bindings_e2e_test(Config) ->
    BArgs = [{routing_key, <<"routing">>}, {arguments, []}],
    http_post(Config, "/bindings/%2F/e/amq.direct/e/badexchange", BArgs, ?NOT_FOUND),
    http_post(Config, "/bindings/%2F/e/badexchange/e/amq.fanout", BArgs, ?NOT_FOUND),
    Headers = http_post(Config, "/bindings/%2F/e/amq.direct/e/amq.fanout", BArgs, {group, '2xx'}),
    "amq.fanout/routing" = pget("location", Headers),
    #{source := <<"amq.direct">>,
      vhost := <<"/">>,
      destination := <<"amq.fanout">>,
      destination_type := <<"exchange">>,
      routing_key := <<"routing">>,
      arguments := #{},
      properties_key := <<"routing">>} =
        http_get(Config, "/bindings/%2F/e/amq.direct/e/amq.fanout/routing", ?OK),
    http_delete(Config, "/bindings/%2F/e/amq.direct/e/amq.fanout/routing", {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/amq.direct/e/amq.headers", BArgs, {group, '2xx'}),
    Binding =
        #{source => <<"amq.direct">>,
          vhost => <<"/">>,
          destination => <<"amq.headers">>,
          destination_type => <<"exchange">>,
          routing_key => <<"routing">>,
          arguments => #{},
          properties_key => <<"routing">>},
    Binding = http_get(Config, "/bindings/%2F/e/amq.direct/e/amq.headers/routing"),
    assert_list([Binding],
                http_get(Config, "/bindings/%2F/e/amq.direct/e/amq.headers")),
    assert_list([Binding],
                http_get(Config, "/exchanges/%2F/amq.direct/bindings/source")),
    assert_list([Binding],
                http_get(Config, "/exchanges/%2F/amq.headers/bindings/destination")),
    http_delete(Config, "/bindings/%2F/e/amq.direct/e/amq.headers/routing", {group, '2xx'}),
    http_get(Config, "/bindings/%2F/e/amq.direct/e/amq.headers/rooting", ?NOT_FOUND),
    passed.

permissions_administrator_test(Config) ->
    http_put(Config, "/users/isadmin", [{password, <<"isadmin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/notadmin", [{password, <<"notadmin">>},
                                         {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/notadmin", [{password, <<"notadmin">>},
                                         {tags, <<"management">>}], {group, '2xx'}),
    Test =
        fun(Path) ->
                http_get(Config, Path, "notadmin", "notadmin", ?NOT_AUTHORISED),
                http_get(Config, Path, "isadmin", "isadmin", ?OK),
                http_get(Config, Path, "guest", "guest", ?OK)
        end,
    Test("/vhosts/%2F"),
    Test("/vhosts/%2F/permissions"),
    Test("/users"),
    Test("/users/guest"),
    Test("/users/guest/permissions"),
    Test("/permissions"),
    Test("/permissions/%2F/guest"),
    http_delete(Config, "/users/notadmin", {group, '2xx'}),
    http_delete(Config, "/users/isadmin", {group, '2xx'}),
    passed.

permissions_vhost_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/myadmin", [{password, <<"myadmin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost1", none, {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost2", none, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost1/myuser", PermArgs, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost1/guest", PermArgs, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost2/guest", PermArgs, {group, '2xx'}),
    assert_list([#{name => <<"/">>},
                 #{name => <<"myvhost1">>},
                 #{name => <<"myvhost2">>}], http_get(Config, "/vhosts", ?OK)),
    assert_list([#{name => <<"myvhost1">>}],
                http_get(Config, "/vhosts", "myuser", "myuser", ?OK)),
    http_put(Config, "/queues/myvhost1/myqueue", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/myvhost2/myqueue", QArgs, {group, '2xx'}),
    Test1 =
        fun(Path) ->
                Results = http_get(Config, Path, "myuser", "myuser", ?OK),
                [case maps:get(vhost, Result) of
                     <<"myvhost2">> ->
                         throw({got_result_from_vhost2_in, Path, Result});
                     _ ->
                         ok
                 end || Result <- Results]
        end,
    Test2 =
        fun(Path1, Path2) ->
                http_get(Config, Path1 ++ "/myvhost1/" ++ Path2, "myuser", "myuser",
                         ?OK),
                http_get(Config, Path1 ++ "/myvhost2/" ++ Path2, "myuser", "myuser",
                         ?NOT_AUTHORISED)
        end,
    Test3 =
        fun(Path1) ->
                http_get(Config, Path1 ++ "/myvhost1/", "myadmin", "myadmin",
                         ?OK)
        end,
    Test1("/exchanges"),
    Test2("/exchanges", ""),
    Test2("/exchanges", "amq.direct"),
    Test3("/exchanges"),
    Test1("/queues"),
    Test2("/queues", ""),
    Test3("/queues"),
    Test2("/queues", "myqueue"),
    Test1("/bindings"),
    Test2("/bindings", ""),
    Test3("/bindings"),
    Test2("/queues", "myqueue/bindings"),
    Test2("/exchanges", "amq.default/bindings/source"),
    Test2("/exchanges", "amq.default/bindings/destination"),
    Test2("/bindings", "e/amq.default/q/myqueue"),
    Test2("/bindings", "e/amq.default/q/myqueue/myqueue"),
    http_delete(Config, "/vhosts/myvhost1", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost2", {group, '2xx'}),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    http_delete(Config, "/users/myadmin", {group, '2xx'}),
    passed.

permissions_amqp_test(Config) ->
    %% Just test that it works at all, not that it works in all possible cases.
    QArgs = #{},
    PermArgs = [{configure, <<"foo.*">>}, {write, <<"foo.*">>},
                {read,      <<"foo.*">>}],
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/permissions/%2F/myuser", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/bar-queue", QArgs, "myuser", "myuser",
             ?NOT_AUTHORISED),
    http_put(Config, "/queues/%2F/bar-queue", QArgs, "nonexistent", "nonexistent",
             ?NOT_AUTHORISED),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

permissions_queue_delete_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<"foo.*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/permissions/%2F/myuser", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/bar-queue", QArgs, {group, '2xx'}),
    http_delete(Config, "/queues/%2F/bar-queue", "myuser", "myuser", ?NOT_AUTHORISED),
    http_delete(Config, "/queues/%2F/bar-queue", {group, '2xx'}),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

%% Opens a new connection and a channel on it.
%% The channel is not managed by rabbit_ct_client_helpers and
%% should be explicitly closed by the caller.
open_connection_and_channel(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),
    {ok, Ch}   = amqp_connection:open_channel(Conn),
    {Conn, Ch}.

get_conn(Config, Username, Password) ->
    Port       = amqp_port(Config),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{
                                          port     = Port,
                      username = list_to_binary(Username),
                      password = list_to_binary(Password)}),
    LocalPort = local_port(Conn),
    ConnPath = rabbit_misc:format(
                 "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w",
                 [LocalPort, Port]),
    ChPath = rabbit_misc:format(
               "/channels/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w%20(1)",
               [LocalPort, Port]),
    ConnChPath = rabbit_misc:format(
                   "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w/channels",
                   [LocalPort, Port]),
    {Conn, ConnPath, ChPath, ConnChPath}.

permissions_connection_channel_consumer_test(Config) ->
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/permissions/%2F/user", PermArgs, {group, '2xx'}),
    http_put(Config, "/users/monitor", [{password, <<"monitor">>},
                                        {tags, <<"monitoring">>}], {group, '2xx'}),
    http_put(Config, "/permissions/%2F/monitor", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test", #{}, {group, '2xx'}),

    {Conn1, UserConn, UserCh, UserConnCh} = get_conn(Config, "user", "user"),
    {Conn2, MonConn, MonCh, MonConnCh} = get_conn(Config, "monitor", "monitor"),
    {Conn3, AdmConn, AdmCh, AdmConnCh} = get_conn(Config, "guest", "guest"),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    {ok, Ch3} = amqp_connection:open_channel(Conn3),
    [amqp_channel:subscribe(
       Ch, #'basic.consume'{queue = <<"test">>}, self()) ||
        Ch <- [Ch1, Ch2, Ch3]],
    AssertLength = fun (Path, User, Len) ->
                           Res = http_get(Config, Path, User, User, ?OK),
                           ?assertEqual(Len, length(Res))
                   end,
    await_condition(
      fun () ->
              [begin
                   AssertLength(P, "user", 1),
                   AssertLength(P, "monitor", 3),
                   AssertLength(P, "guest", 3)
               end || P <- ["/connections", "/channels", "/consumers", "/consumers/%2F"]],
              true
      end),

    AssertRead = fun(Path, UserStatus) ->
                         http_get(Config, Path, "user", "user", UserStatus),
                         http_get(Config, Path, "monitor", "monitor", ?OK),
                         http_get(Config, Path, ?OK)
                 end,
    AssertRead(UserConn, ?OK),
    AssertRead(MonConn, ?NOT_AUTHORISED),
    AssertRead(AdmConn, ?NOT_AUTHORISED),
    AssertRead(UserCh, ?OK),
    AssertRead(MonCh, ?NOT_AUTHORISED),
    AssertRead(AdmCh, ?NOT_AUTHORISED),
    AssertRead(UserConnCh, ?OK),
    AssertRead(MonConnCh, ?NOT_AUTHORISED),
    AssertRead(AdmConnCh, ?NOT_AUTHORISED),

    AssertClose = fun(Path, User, Status) ->
                          http_delete(Config, Path, User, User, Status)
                  end,
    AssertClose(UserConn, "monitor", ?NOT_AUTHORISED),
    AssertClose(MonConn, "user", ?NOT_AUTHORISED),
    AssertClose(AdmConn, "guest", {group, '2xx'}),
    AssertClose(MonConn, "guest", {group, '2xx'}),
    AssertClose(UserConn, "user", {group, '2xx'}),

    http_delete(Config, "/users/user", {group, '2xx'}),
    http_delete(Config, "/users/monitor", {group, '2xx'}),
    http_get(Config, "/connections/foo", ?NOT_FOUND),
    http_get(Config, "/channels/foo", ?NOT_FOUND),
    http_delete(Config, "/queues/%2F/test", {group, '2xx'}),
    passed.

consumers_cq_test(Config) ->
    consumers_test(Config, [{'x-queue-type', <<"classic">>}]).

consumers_qq_test(Config) ->
    consumers_test(Config, [{'x-queue-type', <<"quorum">>}]).

consumers_test(Config, Args) ->
    QArgs = [{auto_delete, false}, {durable, true},
             {arguments, Args}],
    http_put(Config, "/queues/%2F/test", QArgs, {group, '2xx'}),
    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(
      Ch, #'basic.consume'{queue        = <<"test">>,
                           no_ack       = false,
                           consumer_tag = <<"my-ctag">> }, self()),
    await_condition(
      fun () ->
              assert_list([#{exclusive       => false,
                             ack_required    => true,
                             active          => true,
                             activity_status => <<"up">>,
                             consumer_tag    => <<"my-ctag">>}], http_get(Config, "/consumers")),
              true
      end),
    amqp_connection:close(Conn),
    http_delete(Config, "/queues/%2F/test", {group, '2xx'}),
    passed.

single_active_consumer_cq_test(Config) ->
    single_active_consumer(Config,
                           "/queues/%2F/single-active-consumer-cq",
                           <<"single-active-consumer-cq">>,
                           [{'x-queue-type', <<"classic">>}]).

single_active_consumer_qq_test(Config) ->
    single_active_consumer(Config,
                           "/queues/%2F/single-active-consumer-qq",
                           <<"single-active-consumer-qq">>,
                           [{'x-queue-type', <<"quorum">>}]).

single_active_consumer(Config, Url, QName, Args) ->
    QArgs = [{auto_delete, false}, {durable, true},
             {arguments, [{'x-single-active-consumer', true}] ++ Args}],
    http_put(Config, Url, QArgs, {group, '2xx'}),
    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(
        Ch, #'basic.consume'{queue        = QName,
            no_ack       = true,
            consumer_tag = <<"1">> }, self()),
    {ok, Ch2} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(
        Ch2, #'basic.consume'{queue        = QName,
            no_ack       = true,
            consumer_tag = <<"2">> }, self()),
    await_condition(
      fun () ->
              assert_list([#{exclusive       => false,
                             ack_required    => false,
                             active          => true,
                             activity_status => <<"single_active">>,
                             consumer_tag    => <<"1">>},
                           #{exclusive       => false,
                             ack_required    => false,
                             active          => false,
                             activity_status => <<"waiting">>,
                             consumer_tag    => <<"2">>}], http_get(Config, "/consumers")),
              true
      end),
    amqp_channel:close(Ch),
    await_condition(
      fun () ->
              assert_list([#{exclusive       => false,
                             ack_required    => false,
                             active          => true,
                             activity_status => <<"single_active">>,
                             consumer_tag    => <<"2">>}], http_get(Config, "/consumers")),
              true
      end),
    amqp_connection:close(Conn),
    http_delete(Config, Url, {group, '2xx'}),
    passed.

defs(Config, Key, URI, CreateMethod, Args) ->
    defs(Config, Key, URI, CreateMethod, Args,
         fun(URI2) -> http_delete(Config, URI2, {group, '2xx'}) end).

defs_v(Config, Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,
    ReplaceVHostInArgs = fun(M, V2) -> maps:map(fun(vhost, _) -> V2;
                                             (_, V1)    -> V1 end, M) end,

    %% Test against default vhost
    defs(Config, Key, Rep1(URI, "%2F"), CreateMethod, ReplaceVHostInArgs(Args, <<"/">>)),

    %% Test against new vhost
    http_put(Config, "/vhosts/test", none, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/test/guest", PermArgs, {group, '2xx'}),
    DeleteFun0 = fun(URI2) ->
                     http_delete(Config, URI2, {group, '2xx'})
                 end,
    DeleteFun1 = fun(_) ->
                    http_delete(Config, "/vhosts/test", {group, '2xx'})
                 end,
    defs(Config, Key, Rep1(URI, "test"),
         CreateMethod, ReplaceVHostInArgs(Args, <<"test">>),
         DeleteFun0, DeleteFun1).

create(Config, CreateMethod, URI, Args) ->
    case CreateMethod of
        put        -> http_put(Config, URI, Args, {group, '2xx'}),
                      URI;
        put_update -> http_put(Config, URI, Args, {group, '2xx'}),
                      URI;
        post       -> Headers = http_post(Config, URI, Args, {group, '2xx'}),
                      rabbit_web_dispatch_util:unrelativise(
                        URI, pget("location", Headers))
    end.

defs(Config, Key, URI, CreateMethod, Args, DeleteFun) ->
    defs(Config, Key, URI, CreateMethod, Args, DeleteFun, DeleteFun).

defs(Config, Key, URI, CreateMethod, Args, DeleteFun0, DeleteFun1) ->
    %% Create the item
    URI2 = create(Config, CreateMethod, URI, Args),
    %% Make sure it ends up in definitions
    Definitions = http_get(Config, "/definitions", ?OK),
    true = lists:any(fun(I) -> test_item(Args, I) end, maps:get(Key, Definitions)),

    %% Delete it
    DeleteFun0(URI2),

    %% Post the definitions back, it should get recreated in correct form
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),
    assert_item(Args, http_get(Config, URI2, ?OK)),

    %% And delete it again
    DeleteFun1(URI2),

    passed.

register_parameters_and_policy_validator(Config) ->
    rpc(Config, rabbit_mgmt_runtime_parameters_util, register, []),
    rpc(Config, rabbit_mgmt_runtime_parameters_util, register_policy_validator, []).

unregister_parameters_and_policy_validator(Config) ->
    rpc(Config, rabbit_mgmt_runtime_parameters_util, unregister_policy_validator, []),
    rpc(Config, rabbit_mgmt_runtime_parameters_util, unregister, []).

definitions_test(Config) ->
    register_parameters_and_policy_validator(Config),

    defs_v(Config, queues, "/queues/<vhost>/my-queue", put,
           #{name    => <<"my-queue">>,
             durable => true}),
    defs_v(Config, exchanges, "/exchanges/<vhost>/my-exchange", put,
           #{name => <<"my-exchange">>,
             type => <<"direct">>}),
    defs_v(Config, bindings, "/bindings/<vhost>/e/amq.direct/e/amq.fanout", post,
           #{routing_key => <<"routing">>, arguments => #{}}),
    defs_v(Config, policies, "/policies/<vhost>/my-policy", put,
           #{vhost      => vhost,
             name       => <<"my-policy">>,
             pattern    => <<".*">>,
             definition => #{testpos => [1, 2, 3]},
             priority   => 1}),
    defs_v(Config, parameters, "/parameters/test/<vhost>/good", put,
           #{vhost     => vhost,
             component => <<"test">>,
             name      => <<"good">>,
             value     => <<"ignore">>}),
    defs(Config, global_parameters, "/global-parameters/good", put,
         #{name  =>    <<"good">>,
           value =>    #{a => <<"b">>}}),
    defs(Config, users, "/users/myuser", put,
         #{name              => <<"myuser">>,
           password_hash     => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
           hashing_algorithm => <<"rabbit_password_hashing_sha256">>,
           tags              => [<<"management">>]}),
    defs(Config, vhosts, "/vhosts/myvhost", put,
         #{name => <<"myvhost">>}),
    defs(Config, permissions, "/permissions/%2F/guest", put,
         #{user      => <<"guest">>,
           vhost     => <<"/">>,
           configure => <<"c">>,
           write     => <<"w">>,
           read      => <<"r">>}),
    defs(Config, topic_permissions, "/topic-permissions/%2F/guest", put,
         #{user     => <<"guest">>,
           vhost    => <<"/">>,
           exchange => <<"amq.topic">>,
           write    => <<"^a">>,
           read     => <<"^b">>}),

    %% We just messed with guest's permissions
    http_put(Config, "/permissions/%2F/guest",
             #{configure => <<".*">>,
               write     => <<".*">>,
               read      => <<".*">>}, {group, '2xx'}),
    BrokenConfig =
        #{users       => [],
          vhosts      => [],
          permissions => [],
          queues      => [],
          exchanges   => [#{name      =>  <<"not.direct">>,
                          vhost       => <<"/">>,
                          type        => <<"definitely not direct">>,
                          durable     => true,
                          auto_delete => false,
                          arguments   => []}
                         ],
          bindings    => []},
    http_post(Config, "/definitions", BrokenConfig, ?BAD_REQUEST),

    unregister_parameters_and_policy_validator(Config),
    passed.

long_definitions_test(Config) ->
    %% Vhosts take time to start. Generate a bunch of them
    Vhosts = long_definitions_vhosts(long_definitions_test),
    LongDefs =
        #{users       => [],
          vhosts      => Vhosts,
          permissions => [],
          queues      => [],
          exchanges   => [],
          bindings    => []},
    http_post(Config, "/definitions", LongDefs, {group, '2xx'}),
    passed.

long_definitions_multipart_test(Config) ->
    %% Vhosts take time to start. Generate a bunch of them
    Vhosts = long_definitions_vhosts(long_definitions_multipart_test),
    LongDefs =
        #{users       => [],
          vhosts      => Vhosts,
          permissions => [],
          queues      => [],
          exchanges   => [],
          bindings    => []},
    Data = binary_to_list(format_for_upload(LongDefs)),
    CodeExp = {group, '2xx'},
    Boundary = "------------long_definitions_multipart_test",
    Body = format_multipart_filedata(Boundary, [{file, "file", Data}]),
    ContentType = lists:concat(["multipart/form-data; boundary=", Boundary]),
    MoreHeaders = [{"content-type", ContentType}, {"content-length", integer_to_list(length(Body))}],
    http_upload_raw(Config, post, "/definitions", Body, "guest", "guest", CodeExp, MoreHeaders),
    passed.

long_definitions_vhosts(long_definitions_test) ->
    [#{name => <<"long_definitions_test-", (integer_to_binary(N))/binary>>} ||
     N <- lists:seq(1, 120)];
long_definitions_vhosts(long_definitions_multipart_test) ->
    Bin = list_to_binary(lists:flatten(lists:duplicate(524288, "X"))),
    [#{name => <<"long_definitions_test-", Bin/binary, (integer_to_binary(N))/binary>>} ||
     N <- lists:seq(1, 16)].

    defs_default_queue_type_vhost(Config, QueueType) ->
    register_parameters_and_policy_validator(Config),

    %% Create a test vhost
    http_put(Config, "/vhosts/definitions-dqt-vhost-test-vhost", #{default_queue_type => QueueType}, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/definitions-dqt-vhost-test-vhost/guest", PermArgs, {group, '2xx'}),

    %% Import queue definition without an explicit queue type
    http_post(Config, "/definitions/definitions-dqt-vhost-test-vhost",
                #{queues => [#{name => <<"test-queue">>, durable => true}]},
                {group, '2xx'}),

    %% And check whether it was indeed created with the default type
    Q = http_get(Config, "/queues/definitions-dqt-vhost-test-vhost/test-queue", ?OK),
    ?assertEqual(QueueType, maps:get(type, Q)),

    %% Remove the test vhost
    http_delete(Config, "/vhosts/definitions-dqt-vhost-test-vhost", {group, '2xx'}),
    ok.

definitions_vhost_metadata_test(Config) ->
    register_parameters_and_policy_validator(Config),

    VHostName = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    Desc = <<"Created by definitions_vhost_metadata_test">>,
    DQT = <<"quorum">>,
    Tags = [<<"one">>, <<"tag-two">>],
    Metadata = #{
        description => Desc,
        default_queue_type => DQT,
        tags => Tags
    },

    %% Create a test vhost
    http_put(Config, io_lib:format("/vhosts/~ts", [VHostName]), Metadata, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, io_lib:format("/permissions/~ts/guest", [VHostName]), PermArgs, {group, '2xx'}),

    %% Get the definitions
    Definitions = http_get(Config, "/definitions", ?OK),
    ct:pal("Exported definitions:~n~tp~tn", [Definitions]),

    %% Check if vhost definition is correct
    VHosts = maps:get(vhosts, Definitions),
    {value, VH} = lists:search(fun(VH) ->
                                    maps:get(name, VH) =:= VHostName
                                end, VHosts),
    ?assertEqual(#{
        name => VHostName,
        description => Desc,
        tags => Tags,
        metadata => Metadata
    }, VH),

    %% Post the definitions back
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),

    %% Remove the test vhost
    http_delete(Config, io_lib:format("/vhosts/~ts", [VHostName]), {group, '2xx'}),
    ok.

definitions_file_metadata_test(Config) ->
    register_parameters_and_policy_validator(Config),

    VHostName = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    Desc = <<"Created by definitions_vhost_metadata_test">>,
    DQT = <<"quorum">>,
    Tags = [<<"tag-one">>, <<"tag-two">>],
    Metadata = #{
        description => Desc,
        default_queue_type => DQT,
        tags => Tags
    },

    http_put(Config, io_lib:format("/vhosts/~ts", [VHostName]), Metadata, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, io_lib:format("/permissions/~ts/guest", [VHostName]), PermArgs, {group, '2xx'}),

    AllDefinitions = http_get(Config, "/definitions", ?OK),
    %% verify definitions file metadata
    ?assertEqual(<<"cluster">>, maps:get(rabbitmq_definition_format, AllDefinitions)),
    ?assert(is_binary(maps:get(original_cluster_name, AllDefinitions))),

    %% Post the definitions back
    http_post(Config, "/definitions", AllDefinitions, {group, '2xx'}),

    VHDefinitions = http_get(Config, io_lib:format("/definitions/~ts", [VHostName]), ?OK),
    %% verify definitions file metadata
    ?assertEqual(<<"single_virtual_host">>, maps:get(rabbitmq_definition_format, VHDefinitions)),
    ?assertEqual(VHostName, (maps:get(original_vhost_name, VHDefinitions))),
    ?assert(is_map_key(default_queue_type, maps:get(metadata, VHDefinitions))),

    %% Remove the test vhost
    http_delete(Config, io_lib:format("/vhosts/~ts", [VHostName]), {group, '2xx'}),
    ok.

definitions_default_queue_type_test(Config) ->
    defs_default_queue_type_vhost(Config, <<"classic">>),
    defs_default_queue_type_vhost(Config, <<"quorum">>).

defs_vhost(Config, Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,

    %% Create a vhost host
    http_put(Config, "/vhosts/defs-vhost-1298379187", none, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/defs-vhost-1298379187/guest", PermArgs, {group, '2xx'}),

    %% Test against the default vhost
    defs_vhost(Config, Key, URI, Rep1, "%2F", "defs-vhost-1298379187", CreateMethod, Args,
        fun(URI2) -> http_delete(Config, URI2, {group, '2xx'}) end),

    %% Test against the newly created vhost
    defs_vhost(Config, Key, URI, Rep1, "defs-vhost-1298379187", "%2F", CreateMethod, Args,
        fun(URI2) -> http_delete(Config, URI2, {group, '2xx'}) end),

    %% Remove the newly created vhost
    http_delete(Config, "/vhosts/defs-vhost-1298379187", {group, '2xx'}).

defs_vhost(Config, Key, URI0, Rep1, VHost1, VHost2, CreateMethod, Args,
           DeleteFun) ->
    %% Create the item
    URI2 = create(Config, CreateMethod, Rep1(URI0, VHost1), Args),

    %% Make sure it ends up in definitions
    Definitions = http_get(Config, "/definitions/" ++ VHost1, ?OK),
    true = lists:any(fun(I) -> test_item(Args, I) end, maps:get(Key, Definitions)),

    %% `vhost` is implied when importing/exporting for a single
    %% virtual host, let's make sure that it doesn't accidentally
    %% appear in the exported definitions. This can (and did) cause a
    %% confusion about which part of the request to use as the source
    %% for the vhost name.
    case [ I || #{vhost := _} = I <- maps:get(Key, Definitions)] of
        [] -> ok;
        WithVHost -> error({vhost_included_in, Key, WithVHost})
    end,

    %% Make sure it is not in the other vhost
    Definitions0 = http_get(Config, "/definitions/" ++ VHost2, ?OK),
    false = lists:any(fun(I) -> test_item(Args, I) end, maps:get(Key, Definitions0)),

    %% Post the definitions back
    http_post(Config, "/definitions/" ++ VHost2, Definitions, {group, '2xx'}),

    %% Make sure it is now in the other vhost
    Definitions1 = http_get(Config, "/definitions/" ++ VHost2, ?OK),
    true = lists:any(fun(I) -> test_item(Args, I) end, maps:get(Key, Definitions1)),

    %% Delete it
    DeleteFun(URI2),
    URI3 = create(Config, CreateMethod, Rep1(URI0, VHost2), Args),
    DeleteFun(URI3),
    passed.

definitions_vhost_test(Config) ->
    %% Ensures that definitions can be exported/imported from a single virtual
    %% host to another

    register_parameters_and_policy_validator(Config),

    defs_vhost(Config, queues, "/queues/<vhost>/definitions-vhost-test-imported-q", put,
        #{name    => <<"definitions-vhost-test-imported-q">>,
          durable => true}),
    defs_vhost(Config, exchanges, "/exchanges/<vhost>/definitions-vhost-test-imported-dx", put,
        #{name => <<"definitions-vhost-test-imported-dx">>,
          type => <<"direct">>}),
    defs_vhost(Config, bindings, "/bindings/<vhost>/e/amq.direct/e/amq.fanout", post,
        #{routing_key => <<"routing">>, arguments => #{}}),
    defs_vhost(Config, policies, "/policies/<vhost>/definitions-vhost-test-policy", put,
        #{name       => <<"definitions-vhost-test-policy">>,
          pattern    => <<".*">>,
          definition => #{testpos => [1, 2, 3]},
          priority   => 1}),

    defs_vhost(Config, parameters, "/parameters/vhost-limits/<vhost>/limits", put,
        #{name       => <<"limits">>,
          component  => <<"vhost-limits">>,
          value      => #{ 'max-connections' => 100 }}),
    Upload =
        #{queues     => [],
          exchanges  => [],
          policies   => [],
          parameters => [],
          bindings   => []},
    http_post(Config, "/definitions/othervhost", Upload, ?NOT_FOUND),

    unregister_parameters_and_policy_validator(Config),
    passed.

definitions_password_test(Config) ->
    % Import definitions from 3.5.x
    Config35 = #{rabbit_version => <<"3.5.4">>,
                 users => [#{name          => <<"myuser">>,
                           password_hash => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
                           tags          => <<"management">>}
                        ]},
    Expected35 = #{name              => <<"myuser">>,
                   password_hash     => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
                   hashing_algorithm => <<"rabbit_password_hashing_md5">>,
                   tags              => [<<"management">>]},
    http_post(Config, "/definitions", Config35, {group, '2xx'}),
    Definitions35 = http_get(Config, "/definitions", ?OK),
    ct:pal("Definitions35: ~tp", [Definitions35]),
    Users35 = maps:get(users, Definitions35),
    true = lists:any(fun(I) -> test_item(Expected35, I) end, Users35),

    %% Import definitions from from 3.6.0
    Config36 = #{rabbit_version => <<"3.6.0">>,
                 users => [#{name          => <<"myuser">>,
                           password_hash => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
                           tags          => <<"management">>}
                        ]},
    Expected36 = #{name              => <<"myuser">>,
                   password_hash     => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
                   hashing_algorithm => <<"rabbit_password_hashing_sha256">>,
                   tags              => [<<"management">>]},
    http_post(Config, "/definitions", Config36, {group, '2xx'}),

    Definitions36 = http_get(Config, "/definitions", ?OK),
    Users36 = maps:get(users, Definitions36),
    true = lists:any(fun(I) -> test_item(Expected36, I) end, Users36),

    %% No hashing_algorithm provided
    ConfigDefault = #{rabbit_version => <<"3.6.1">>,
                      users => [#{name        => <<"myuser">>,
                                password_hash => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
                                tags          => <<"management">>}
                             ]},
    rpc(Config, application, set_env, [rabbit, password_hashing_module, rabbit_password_hashing_sha512]),

    ExpectedDefault = #{name              => <<"myuser">>,
                        password_hash     => <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>,
                        hashing_algorithm => <<"rabbit_password_hashing_sha512">>,
                        tags              => [<<"management">>]},
    http_post(Config, "/definitions", ConfigDefault, {group, '2xx'}),

    DefinitionsDefault = http_get(Config, "/definitions", ?OK),
    UsersDefault = maps:get(users, DefinitionsDefault),

    true = lists:any(fun(I) -> test_item(ExpectedDefault, I) end, UsersDefault),
    passed.

definitions_remove_things_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"my-exclusive">>,
                                            exclusive = true }),
    http_get(Config, "/queues/%2F/my-exclusive", ?OK),
    Definitions = http_get(Config, "/definitions", ?OK),
    [] = maps:get(queues, Definitions),
    [] = maps:get(exchanges, Definitions),
    [] = maps:get(bindings, Definitions),
    amqp_channel:close(Ch),
    close_connection(Conn),
    passed.

definitions_server_named_queue_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    %% declares a durable server-named queue for the sake of exporting the definition
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{queue = <<"">>, durable = true}),
    Path = "/queues/%2F/" ++ rabbit_http_util:quote_plus(QName),
    http_get(Config, Path, ?OK),
    Definitions = http_get(Config, "/definitions", ?OK),
    close_channel(Ch),
    close_connection(Conn),
    http_delete(Config, Path, {group, '2xx'}),
    http_get(Config, Path, ?NOT_FOUND),
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),
    %% amq.* entities are not imported
    http_get(Config, Path, ?NOT_FOUND),
    passed.

definitions_with_charset_test(Config) ->
    Path = "/definitions",
    Body0 = http_get(Config, Path, ?OK),
    Headers = [auth_header("guest", "guest")],
    Url = uri_base_from(Config, 0) ++ Path,
    Body1 = format_for_upload(Body0),
    Request = {Url, Headers, "application/json; charset=utf-8", Body1},
    {ok, {{_, ?NO_CONTENT, _}, _, []}} = httpc:request(post, Request, ?HTTPC_OPTS, []),
    passed.

arguments_test(Config) ->
    XArgs = [{type, <<"headers">>},
                {arguments, [{'alternate-exchange', <<"amq.direct">>}]}],
    QArgs = [{arguments, [{'x-expires', 1800000}]}],
    BArgs = [{routing_key, <<"">>},
                {arguments, [{'x-match', <<"all">>},
                            {foo, <<"bar">>}]}],
    http_delete(Config, "/exchanges/%2F/arguments-test-x", {one_of, [201, 404]}),
    http_put(Config, "/exchanges/%2F/arguments-test-x", XArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/arguments-test", QArgs, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/arguments-test-x/q/arguments-test", BArgs, {group, '2xx'}),

    #{'alternate-exchange' := <<"amq.direct">>} =
        maps:get(arguments, http_get(Config, "/exchanges/%2F/arguments-test-x", ?OK)),
    #{'x-expires' := 1800000} =
        maps:get(arguments, http_get(Config, "/queues/%2F/arguments-test", ?OK)),

    ArgsTable = [{<<"foo">>,longstr,<<"bar">>}, {<<"x-match">>, longstr, <<"all">>}],
    Hash = table_hash(ArgsTable),
    PropertiesKey = [$~] ++ Hash,

    assert_item(
        #{'x-match' => <<"all">>, foo => <<"bar">>},
        maps:get(arguments,
            http_get(Config, "/bindings/%2F/e/arguments-test-x/q/arguments-test/" ++
            PropertiesKey, ?OK))
    ),
    http_delete(Config, "/exchanges/%2F/arguments-test-x", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/arguments-test", {group, '2xx'}),
    passed.

table_hash(Table) ->
    binary_to_list(rabbit_mgmt_format:args_hash(Table)).

arguments_table_test(Config) ->
    Args = #{'upstreams' => [<<"amqp://localhost/%2F/upstream1">>,
                                <<"amqp://localhost/%2F/upstream2">>]},
    XArgs = #{type      => <<"headers">>,
                arguments => Args},
    http_delete(Config, "/exchanges/%2F/arguments-table-test-x", {one_of, [201, 404]}),
    http_put(Config, "/exchanges/%2F/arguments-table-test-x", XArgs, {group, '2xx'}),
    Args = maps:get(arguments, http_get(Config, "/exchanges/%2F/arguments-table-test-x", ?OK)),
    http_delete(Config, "/exchanges/%2F/arguments-table-test-x", {group, '2xx'}),
    passed.

queue_purge_test(Config) ->
    QArgs = #{},
    http_put(Config, "/queues/%2F/myqueue", QArgs, {group, '2xx'}),
    {Conn, Ch} = open_connection_and_channel(Config),
    Publish = fun() ->
                      amqp_channel:call(
                        Ch, #'basic.publish'{exchange = <<"">>,
                                             routing_key = <<"myqueue">>},
                        #amqp_msg{payload = <<"message">>})
              end,
    Publish(),
    Publish(),
    amqp_channel:call(
      Ch, #'queue.declare'{queue = <<"exclusive">>, exclusive = true}),
    {#'basic.get_ok'{}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue = <<"myqueue">>}),
    http_delete(Config, "/queues/%2F/myqueue/contents", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/badqueue/contents", ?NOT_FOUND),
    http_delete(Config, "/queues/%2F/exclusive/contents", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/exclusive", ?BAD_REQUEST),
    #'basic.get_empty'{} =
        amqp_channel:call(Ch, #'basic.get'{queue = <<"myqueue">>}),
    close_channel(Ch),
    close_connection(Conn),
    http_delete(Config, "/queues/%2F/myqueue", {group, '2xx'}),
    passed.

queue_actions_test(Config) ->
    http_put(Config, "/queues/%2F/q", #{}, {group, '2xx'}),
    http_post(Config, "/queues/%2F/q/actions", [{action, change_colour}], ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/q", {group, '2xx'}),
    passed.

exclusive_consumer_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue     = QName,
                                                exclusive = true}, self()),
    await_condition(
      fun () ->
              http_get(Config, "/queues/%2F/"), %% Just check we don't blow up
              true
      end),
    close_channel(Ch),
    close_connection(Conn),
    passed.


exclusive_queue_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
    amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    Path = "/queues/%2F/" ++ rabbit_http_util:quote_plus(QName),
    await_condition(
      fun () ->
              Queue = http_get(Config, Path),
              assert_item(#{name        => QName,
                            vhost       => <<"/">>,
                            durable     => false,
                            auto_delete => false,
                            exclusive   => true,
                            arguments   => #{'x-queue-type' => <<"classic">>}
                           }, Queue),
              true
      end),
    amqp_channel:close(Ch),
    close_connection(Conn),
    passed.

connections_channels_pagination_test(Config) ->
    %% this test uses "unmanaged" (by Common Test helpers) connections to avoid
    %% connection caching
    Conn      = open_unmanaged_connection(Config),
    {ok, Ch}  = amqp_connection:open_channel(Conn),
    Conn1     = open_unmanaged_connection(Config),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    Conn2     = open_unmanaged_connection(Config),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    await_condition(
      fun () ->
              PageOfTwo = http_get(Config, "/connections?page=1&page_size=2", ?OK),
              ?assertEqual(3, maps:get(total_count, PageOfTwo)),
              ?assertEqual(3, maps:get(filtered_count, PageOfTwo)),
              ?assertEqual(2, maps:get(item_count, PageOfTwo)),
              ?assertEqual(1, maps:get(page, PageOfTwo)),
              ?assertEqual(2, maps:get(page_size, PageOfTwo)),
              ?assertEqual(2, maps:get(page_count, PageOfTwo)),


              TwoOfTwo = http_get(Config, "/channels?page=2&page_size=2", ?OK),
              ?assertEqual(3, maps:get(total_count, TwoOfTwo)),
              ?assertEqual(3, maps:get(filtered_count, TwoOfTwo)),
              ?assertEqual(1, maps:get(item_count, TwoOfTwo)),
              ?assertEqual(2, maps:get(page, TwoOfTwo)),
              ?assertEqual(2, maps:get(page_size, TwoOfTwo)),
              ?assertEqual(2, maps:get(page_count, TwoOfTwo)),
              true
      end),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    amqp_channel:close(Ch1),
    amqp_connection:close(Conn1),
    amqp_channel:close(Ch2),
    amqp_connection:close(Conn2),

    passed.

exchanges_pagination_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/guest", PermArgs, {group, '2xx'}),
    http_get(Config, "/exchanges/vh1?page=1&page_size=2", ?OK),
    http_put(Config, "/exchanges/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/exchanges/vh1/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/exchanges/%2F/test2_reg", QArgs, {group, '2xx'}),
    http_put(Config, "/exchanges/vh1/reg_test3", QArgs, {group, '2xx'}),

    Total = length(rpc(Config, rabbit_exchange, list_names, [])),
    await_condition(
      fun () ->
              PageOfTwo = http_get(Config, "/exchanges?page=1&page_size=2", ?OK),
              ?assertEqual(Total, maps:get(total_count, PageOfTwo)),
              ?assertEqual(Total, maps:get(filtered_count, PageOfTwo)),
              ?assertEqual(2, maps:get(item_count, PageOfTwo)),
              ?assertEqual(1, maps:get(page, PageOfTwo)),
              ?assertEqual(2, maps:get(page_size, PageOfTwo)),
              ?assertEqual(round(Total / 2), maps:get(page_count, PageOfTwo)),
              assert_list([#{name => <<"">>, vhost => <<"/">>},
                           #{name => <<"amq.direct">>, vhost => <<"/">>}
                          ], maps:get(items, PageOfTwo)),

              ByName = http_get(Config, "/exchanges?page=1&page_size=2&name=reg", ?OK),
              ?assertEqual(Total, maps:get(total_count, ByName)),
              ?assertEqual(2, maps:get(filtered_count, ByName)),
              ?assertEqual(2, maps:get(item_count, ByName)),
              ?assertEqual(1, maps:get(page, ByName)),
              ?assertEqual(2, maps:get(page_size, ByName)),
              ?assertEqual(1, maps:get(page_count, ByName)),
              assert_list([#{name => <<"test2_reg">>, vhost => <<"/">>},
                           #{name => <<"reg_test3">>, vhost => <<"vh1">>}
                          ], maps:get(items, ByName)),


              RegExByName = http_get(Config,
                                     "/exchanges?page=1&page_size=2&name=%5E(?=%5Ereg)&use_regex=true",
                                     ?OK),
              ?assertEqual(Total, maps:get(total_count, RegExByName)),
              ?assertEqual(1, maps:get(filtered_count, RegExByName)),
              ?assertEqual(1, maps:get(item_count, RegExByName)),
              ?assertEqual(1, maps:get(page, RegExByName)),
              ?assertEqual(2, maps:get(page_size, RegExByName)),
              ?assertEqual(1, maps:get(page_count, RegExByName)),
              assert_list([#{name => <<"reg_test3">>, vhost => <<"vh1">>}
                          ], maps:get(items, RegExByName)),
              true
      end),


    http_get(Config, "/exchanges?page=1000", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=-1", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=1&page_size=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/exchanges?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/exchanges/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/exchanges/%2F/test2_reg", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/reg_test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
    passed.

exchanges_pagination_permissions_test(Config) ->
    http_put(Config, "/users/admin",   [{password, <<"admin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/non-admin", [{password, <<"non-admin">>},
                                          {tags, <<"management">>}], {group, '2xx'}),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/non-admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/%2F/admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/admin",   Perms, {group, '2xx'}),
    QArgs = #{},
    http_put(Config, "/exchanges/%2F/test0", QArgs, "admin", "admin", {group, '2xx'}),
    http_put(Config, "/exchanges/vh1/test1", QArgs, "non-admin", "non-admin", {group, '2xx'}),

    await_condition(
      fun () ->
              FirstPage = http_get(Config, "/exchanges?page=1&name=test1", "non-admin", "non-admin", ?OK),

              ?assertEqual(8, maps:get(total_count, FirstPage)),
              ?assertEqual(1, maps:get(item_count, FirstPage)),
              ?assertEqual(1, maps:get(page, FirstPage)),
              ?assertEqual(100, maps:get(page_size, FirstPage)),
              ?assertEqual(1, maps:get(page_count, FirstPage)),
              assert_list([#{name => <<"test1">>, vhost => <<"vh1">>}
                          ], maps:get(items, FirstPage)),
              true
      end),
    http_delete(Config, "/exchanges/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/users/admin", {group, '2xx'}),
    http_delete(Config, "/users/non-admin", {group, '2xx'}),
    passed.



queue_pagination_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh.tests.queue_pagination_test", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh.tests.queue_pagination_test/guest", PermArgs, {group, '2xx'}),

    http_get(Config, "/queues/vh.tests.queue_pagination_test?page=1&page_size=2", ?OK),

    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh.tests.queue_pagination_test/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test2_reg", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh.tests.queue_pagination_test/reg_test3", QArgs, {group, '2xx'}),

    ?AWAIT(
       begin
           Total = length(rpc(Config, rabbit_amqqueue, list_names, [])),

           PageOfTwo = http_get(Config, "/queues?page=1&page_size=2", ?OK),
           ?assertEqual(Total, maps:get(total_count, PageOfTwo)),
           ?assertEqual(Total, maps:get(filtered_count, PageOfTwo)),
           ?assertEqual(2, maps:get(item_count, PageOfTwo)),
           ?assertEqual(1, maps:get(page, PageOfTwo)),
           ?assertEqual(2, maps:get(page_size, PageOfTwo)),
           ?assertEqual(2, maps:get(page_count, PageOfTwo)),
           assert_list([#{name => <<"test0">>, vhost => <<"/">>, storage_version => 2},
                        #{name => <<"test2_reg">>, vhost => <<"/">>, storage_version => 2}
                       ], maps:get(items, PageOfTwo)),

           SortedByName = http_get(Config, "/queues?sort=name&page=1&page_size=2", ?OK),
           ?assertEqual(Total, maps:get(total_count, SortedByName)),
           ?assertEqual(Total, maps:get(filtered_count, SortedByName)),
           ?assertEqual(2, maps:get(item_count, SortedByName)),
           ?assertEqual(1, maps:get(page, SortedByName)),
           ?assertEqual(2, maps:get(page_size, SortedByName)),
           ?assertEqual(2, maps:get(page_count, SortedByName)),
           assert_list([#{name => <<"reg_test3">>, vhost => <<"vh.tests.queue_pagination_test">>},
                        #{name => <<"test0">>, vhost => <<"/">>}
                       ], maps:get(items, SortedByName)),


           FirstPage = http_get(Config, "/queues?page=1", ?OK),
           ?assertEqual(Total, maps:get(total_count, FirstPage)),
           ?assertEqual(Total, maps:get(filtered_count, FirstPage)),
           ?assertEqual(4, maps:get(item_count, FirstPage)),
           ?assertEqual(1, maps:get(page, FirstPage)),
           ?assertEqual(100, maps:get(page_size, FirstPage)),
           ?assertEqual(1, maps:get(page_count, FirstPage)),
           assert_list([#{name => <<"test0">>, vhost => <<"/">>},
                        #{name => <<"test1">>, vhost => <<"vh.tests.queue_pagination_test">>},
                        #{name => <<"test2_reg">>, vhost => <<"/">>},
                        #{name => <<"reg_test3">>, vhost =><<"vh.tests.queue_pagination_test">>}
                       ], maps:get(items, FirstPage)),
           %% The reduced API version just has the most useful fields.
           %% garbage_collection is not one of them
           IsEnabled = rabbit_ct_broker_helpers:is_feature_flag_enabled(
                         Config, detailed_queues_endpoint),
           case IsEnabled of
               true  ->
                   [?assertNot(maps:is_key(garbage_collection, Item)) ||
                    Item <- maps:get(items, FirstPage)];
               false ->
                   [?assert(maps:is_key(garbage_collection, Item)) ||
                    Item <- maps:get(items, FirstPage)]
           end,
           ReverseSortedByName = http_get(Config,
                                          "/queues?page=2&page_size=2&sort=name&sort_reverse=true",
                                          ?OK),
           ?assertEqual(Total, maps:get(total_count, ReverseSortedByName)),
           ?assertEqual(Total, maps:get(filtered_count, ReverseSortedByName)),
           ?assertEqual(2, maps:get(item_count, ReverseSortedByName)),
           ?assertEqual(2, maps:get(page, ReverseSortedByName)),
           ?assertEqual(2, maps:get(page_size, ReverseSortedByName)),
           ?assertEqual(2, maps:get(page_count, ReverseSortedByName)),
           assert_list([#{name => <<"test0">>, vhost => <<"/">>},
                        #{name => <<"reg_test3">>, vhost => <<"vh.tests.queue_pagination_test">>}
                       ], maps:get(items, ReverseSortedByName)),


           ByName = http_get(Config, "/queues?page=1&page_size=2&name=reg", ?OK),
           ?assertEqual(Total, maps:get(total_count, ByName)),
           ?assertEqual(2, maps:get(filtered_count, ByName)),
           ?assertEqual(2, maps:get(item_count, ByName)),
           ?assertEqual(1, maps:get(page, ByName)),
           ?assertEqual(2, maps:get(page_size, ByName)),
           ?assertEqual(1, maps:get(page_count, ByName)),
           assert_list([#{name => <<"test2_reg">>, vhost => <<"/">>},
                        #{name => <<"reg_test3">>, vhost => <<"vh.tests.queue_pagination_test">>}
                       ], maps:get(items, ByName)),

           RegExByName = http_get(Config,
                                  "/queues?page=1&page_size=2&name=%5E(?=%5Ereg)&use_regex=true",
                                  ?OK),
           ?assertEqual(Total, maps:get(total_count, RegExByName)),
           ?assertEqual(1, maps:get(filtered_count, RegExByName)),
           ?assertEqual(1, maps:get(item_count, RegExByName)),
           ?assertEqual(1, maps:get(page, RegExByName)),
           ?assertEqual(2, maps:get(page_size, RegExByName)),
           ?assertEqual(1, maps:get(page_count, RegExByName)),
           assert_list([#{name => <<"reg_test3">>, vhost => <<"vh.tests.queue_pagination_test">>}
                       ], maps:get(items, RegExByName)),
           true
       end
      ),


    http_get(Config, "/queues?page=1000", ?BAD_REQUEST),
    http_get(Config, "/queues?page=-1", ?BAD_REQUEST),
    http_get(Config, "/queues?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/queues?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh.tests.queue_pagination_test/test1", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/test2_reg", {group, '2xx'}),
    http_delete(Config, "/queues/vh.tests.queue_pagination_test/reg_test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh.tests.queue_pagination_test", {group, '2xx'}),
    passed.

queue_pagination_columns_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh.tests.queue_pagination_columns_test", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh.tests.queue_pagination_columns_test/guest", PermArgs, [?CREATED, ?NO_CONTENT]),

    http_get(Config, "/queues/vh.tests.queue_pagination_columns_test?columns=name&page=1&page_size=2", ?OK),
    http_put(Config, "/queues/%2F/queue_a", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh.tests.queue_pagination_columns_test/queue_b", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/queue_c", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh.tests.queue_pagination_columns_test/queue_d", QArgs, {group, '2xx'}),
    PageOfTwo = http_get(Config, "/queues?columns=name&page=1&page_size=2", ?OK),
    ?assertEqual(4, maps:get(total_count, PageOfTwo)),
    ?assertEqual(4, maps:get(filtered_count, PageOfTwo)),
    ?assertEqual(2, maps:get(item_count, PageOfTwo)),
    ?assertEqual(1, maps:get(page, PageOfTwo)),
    ?assertEqual(2, maps:get(page_size, PageOfTwo)),
    ?assertEqual(2, maps:get(page_count, PageOfTwo)),
    assert_list([#{name => <<"queue_a">>},
                 #{name => <<"queue_c">>}
    ], maps:get(items, PageOfTwo)),

    ColumnNameVhost = http_get(Config, "/queues/vh.tests.queue_pagination_columns_test?columns=name&page=1&page_size=2", ?OK),
    ?assertEqual(2, maps:get(total_count, ColumnNameVhost)),
    ?assertEqual(2, maps:get(filtered_count, ColumnNameVhost)),
    ?assertEqual(2, maps:get(item_count, ColumnNameVhost)),
    ?assertEqual(1, maps:get(page, ColumnNameVhost)),
    ?assertEqual(2, maps:get(page_size, ColumnNameVhost)),
    ?assertEqual(1, maps:get(page_count, ColumnNameVhost)),
    assert_list([#{name => <<"queue_b">>},
                 #{name => <<"queue_d">>}
    ], maps:get(items, ColumnNameVhost)),

    ColumnsNameVhost = http_get(Config, "/queues?columns=name,vhost&page=2&page_size=2", ?OK),
    ?assertEqual(4, maps:get(total_count, ColumnsNameVhost)),
    ?assertEqual(4, maps:get(filtered_count, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(item_count, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(page, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(page_size, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(page_count, ColumnsNameVhost)),
    assert_list([
        #{name  => <<"queue_b">>,
          vhost => <<"vh.tests.queue_pagination_columns_test">>},
        #{name  => <<"queue_d">>,
          vhost => <<"vh.tests.queue_pagination_columns_test">>}
    ], maps:get(items, ColumnsNameVhost)),

    ?awaitMatch(
       true,
       begin
           ColumnsGarbageCollection = http_get(Config, "/queues?columns=name,garbage_collection&page=2&page_size=2", ?OK),
           %% The reduced API version just has the most useful fields,
           %% but we can still query any info item using `columns`
           lists:all(fun(Item) ->
                             maps:is_key(garbage_collection, Item)
                     end,
                     maps:get(items, ColumnsGarbageCollection))
       end, 30000),

    http_delete(Config, "/queues/%2F/queue_a", {group, '2xx'}),
    http_delete(Config, "/queues/vh.tests.queue_pagination_columns_test/queue_b", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/queue_c", {group, '2xx'}),
    http_delete(Config, "/queues/vh.tests.queue_pagination_columns_test/queue_d", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh.tests.queue_pagination_columns_test", {group, '2xx'}),
    passed.

queues_detailed_test(Config) ->
    QArgs = #{},
    http_put(Config, "/queues/%2F/queue_a", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/queue_c", QArgs, {group, '2xx'}),

    ?awaitMatch(
       true,
       begin
           Detailed = http_get(Config, "/queues/detailed", ?OK),
           lists:all(fun(Item) ->
                             maps:is_key(garbage_collection, Item)
                     end, Detailed)
       end, 30000),

    Detailed = http_get(Config, "/queues/detailed", ?OK),
    ?assertNot(lists:any(fun(Item) ->
                                 maps:is_key(backing_queue_status, Item)
                         end, Detailed)),
    %% It's null
    ?assert(lists:any(fun(Item) ->
                              maps:is_key(single_active_consumer_tag, Item)
                      end, Detailed)),

    Reduced = http_get(Config, "/queues", ?OK),
    ?assertNot(lists:any(fun(Item) ->
                                 maps:is_key(garbage_collection, Item)
                         end, Reduced)),
    ?assertNot(lists:any(fun(Item) ->
                                 maps:is_key(backing_queue_status, Item)
                         end, Reduced)),
    ?assertNot(lists:any(fun(Item) ->
                                 maps:is_key(single_active_consumer_tag, Item)
                         end, Reduced)),

    http_delete(Config, "/queues/%2F/queue_a", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/queue_c", {group, '2xx'}),
    passed.

queues_pagination_permissions_test(Config) ->
    http_put(Config, "/users/non-admin", [{password, <<"non-admin">>},
                                          {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/admin",   [{password, <<"admin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/non-admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/%2F/admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/admin",   Perms, {group, '2xx'}),
    QArgs = #{},
    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/test1", QArgs, "non-admin","non-admin", {group, '2xx'}),
    FirstPage = http_get(Config, "/queues?page=1", "non-admin", "non-admin", ?OK),
    ?assertEqual(1, maps:get(total_count, FirstPage)),
    ?assertEqual(1, maps:get(item_count, FirstPage)),
    ?assertEqual(1, maps:get(page, FirstPage)),
    ?assertEqual(100, maps:get(page_size, FirstPage)),
    ?assertEqual(1, maps:get(page_count, FirstPage)),
    assert_list([#{name => <<"test1">>, vhost => <<"vh1">>}
                ], maps:get(items, FirstPage)),

    FirstPageAdm = http_get(Config, "/queues?page=1", "admin", "admin", ?OK),
    ?assertEqual(2, maps:get(total_count, FirstPageAdm)),
    ?assertEqual(2, maps:get(item_count, FirstPageAdm)),
    ?assertEqual(1, maps:get(page, FirstPageAdm)),
    ?assertEqual(100, maps:get(page_size, FirstPageAdm)),
    ?assertEqual(1, maps:get(page_count, FirstPageAdm)),
    assert_list([#{name => <<"test1">>, vhost => <<"vh1">>},
                 #{name => <<"test0">>, vhost => <<"/">>}
                ], maps:get(items, FirstPageAdm)),

    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/test1","admin","admin", {group, '2xx'}),
    http_delete(Config, "/users/admin", {group, '2xx'}),
    http_delete(Config, "/users/non-admin", {group, '2xx'}),
    passed.

samples_range_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),

    %% Channels
    ?AWAIT(
       begin
           [ConnInfo | _] = http_get(Config, "/channels?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/channels?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

           ConnDetails = maps:get(connection_details, ConnInfo),
           ConnName0 = maps:get(name, ConnDetails),
           ConnName = uri_string:recompose(#{path => binary_to_list(ConnName0)}),
           ChanName = ConnName ++ uri_string:recompose(#{path => " (1)"}),

           http_get(Config, "/channels/" ++ ChanName ++ "?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/channels/" ++ ChanName ++ "?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

           http_get(Config, "/vhosts/%2F/channels?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/vhosts/%2F/channels?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

           %% Connections.

           http_get(Config, "/connections?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/connections?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

           http_get(Config, "/connections/" ++ ConnName ++ "?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/connections/" ++ ConnName ++ "?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

           http_get(Config, "/connections/" ++ ConnName ++ "/channels?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/connections/" ++ ConnName ++ "/channels?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

           http_get(Config, "/vhosts/%2F/connections?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/vhosts/%2F/connections?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
           true
       end),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),

    %% Exchanges

    http_get(Config, "/exchanges?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/exchanges?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_get(Config, "/exchanges/%2F/amq.direct?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/exchanges/%2F/amq.direct?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Nodes
    http_get(Config, "/nodes?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/nodes?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Overview
    http_get(Config, "/overview?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/overview?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Queues
    http_put(Config, "/queues/%2F/test-001", #{}, {group, '2xx'}),

    ?AWAIT(
       begin
           http_get(Config, "/queues/%2F?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/queues/%2F?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
           http_get(Config, "/queues/%2F/test-001?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/queues/%2F/test-001?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
           true
       end),

    http_delete(Config, "/queues/%2F/test-001", {group, '2xx'}),

    %% Vhosts
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),

    ?AWAIT(
       begin
           http_get(Config, "/vhosts?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/vhosts?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
           http_get(Config, "/vhosts/vh1?lengths_age=60&lengths_incr=1", ?OK),
           http_get(Config, "/vhosts/vh1?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
           true
       end),

    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),

    passed.

sorting_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh19", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh19/guest", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh19/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test2", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh19/test3", QArgs, {group, '2xx'}),
    ?AWAIT(
       begin
           assert_list([#{name => <<"test0">>},
                        #{name => <<"test2">>},
                        #{name => <<"test1">>},
                        #{name => <<"test3">>}], http_get(Config, "/queues", ?OK)),
           assert_list([#{name => <<"test0">>},
                        #{name => <<"test1">>},
                        #{name => <<"test2">>},
                        #{name => <<"test3">>}], http_get(Config, "/queues?sort=name", ?OK)),
           assert_list([#{name => <<"test0">>},
                        #{name => <<"test2">>},
                        #{name => <<"test1">>},
                        #{name => <<"test3">>}], http_get(Config, "/queues?sort=vhost", ?OK)),
           assert_list([#{name => <<"test3">>},
                        #{name => <<"test1">>},
                        #{name => <<"test2">>},
                        #{name => <<"test0">>}], http_get(Config, "/queues?sort_reverse=true", ?OK)),
           assert_list([#{name => <<"test3">>},
                        #{name => <<"test2">>},
                        #{name => <<"test1">>},
                        #{name => <<"test0">>}], http_get(Config, "/queues?sort=name&sort_reverse=true", ?OK)),
           assert_list([#{name => <<"test3">>},
                        #{name => <<"test1">>},
                        #{name => <<"test2">>},
                        #{name => <<"test0">>}], http_get(Config, "/queues?sort=vhost&sort_reverse=true", ?OK)),
           true
       end),
    %% Rather poor but at least test it doesn't blow up with dots
    http_get(Config, "/queues?sort=owner_pid_details.name", ?OK),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh19/test1", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/test2", {group, '2xx'}),
    http_delete(Config, "/queues/vh19/test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh19", {group, '2xx'}),
    passed.

format_output_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh129", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh129/guest", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),

    ?AWAIT(
       begin
           assert_list([#{name => <<"test0">>,
                          consumer_capacity => 0,
                          consumer_utilisation => 0,
                          exclusive_consumer_tag => null}], http_get(Config, "/queues", ?OK)),
           true
       end),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh129", {group, '2xx'}),
    passed.

columns_test(Config) ->
    Path = "/queues/%2F/columns.test",
    TTL = 30000,
    http_delete(Config, Path, [{group, '2xx'}, 404]),
    http_put(Config, Path, [{arguments, [{<<"x-message-ttl">>, TTL}]}],
             {group, '2xx'}),
    Item = #{arguments => #{'x-message-ttl' => TTL, 'x-queue-type' => <<"classic">>}, name => <<"columns.test">>},

    ?AWAIT(
       begin
           [Item] = http_get(Config, "/queues?columns=arguments.x-message-ttl,name", ?OK),
           Item = http_get(Config, "/queues/%2F/columns.test?columns=arguments.x-message-ttl,name", ?OK),
           true
       end),
    http_delete(Config, Path, {group, '2xx'}),
    passed.

get_test(Config) ->
    %% Real world example...
    Headers = [{<<"x-forwarding">>, array,
                [{table,
                  [{<<"uri">>, longstr,
                    <<"amqp://localhost/%2F/upstream">>}]}]}],
    http_put(Config, "/queues/%2F/myqueue", #{}, {group, '2xx'}),
    {Conn, Ch} = open_connection_and_channel(Config),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    Publish = fun (Payload) ->
                      amqp_channel:cast(
                        Ch, #'basic.publish'{exchange = <<>>,
                                             routing_key = <<"myqueue">>},
                        #amqp_msg{props = #'P_basic'{headers = Headers},
                                  payload = Payload}),
                      amqp_channel:wait_for_confirms_or_die(Ch, 5)
              end,
    Publish(<<"1aaa">>),
    Publish(<<"2aaa">>),
    Publish(<<"3aaa">>),
    [Msg] = http_post(Config, "/queues/%2F/myqueue/get", [{ackmode, ack_requeue_false},
                                                          {count,    1},
                                                          {encoding, auto},
                                                          {truncate, 1}], ?OK),
    false         = maps:get(redelivered, Msg),
    <<>>          = maps:get(exchange,    Msg),
    <<"myqueue">> = maps:get(routing_key, Msg),
    <<"1">>       = maps:get(payload,     Msg),
    #{'x-forwarding' :=
      [#{uri := <<"amqp://localhost/%2F/upstream">>}]} =
        maps:get(headers, maps:get(properties, Msg)),

    [M2, M3] = http_post(Config, "/queues/%2F/myqueue/get", [{ackmode,  ack_requeue_true},
                                                             {count,    5},
                                                             {encoding, auto}], ?OK),
    <<"2aaa">> = maps:get(payload, M2),
    <<"3aaa">> = maps:get(payload, M3),
    2 = length(http_post(Config, "/queues/%2F/myqueue/get", [{ackmode,  ack_requeue_false},
                                                             {count,    5},
                                                             {encoding, auto}], ?OK)),
    Publish(<<"4aaa">>),
    Publish(<<"5aaa">>),
    [M4, M5] = http_post(Config, "/queues/%2F/myqueue/get",
                         [{ackmode,  reject_requeue_true},
                          {count,    5},
                          {encoding, auto}], ?OK),

    <<"4aaa">> = maps:get(payload, M4),
    <<"5aaa">> = maps:get(payload, M5),
    2 = length(http_post(Config, "/queues/%2F/myqueue/get",
                         [{ackmode,  ack_requeue_false},
                          {count,    5},
                          {encoding, auto}], ?OK)),

    [] = http_post(Config, "/queues/%2F/myqueue/get", [{ackmode,  ack_requeue_false},
                                                       {count,    5},
                                                       {encoding, auto}], ?OK),
    http_delete(Config, "/queues/%2F/myqueue", {group, '2xx'}),
    amqp_channel:close(Ch),
    close_connection(Conn),

    passed.

get_encoding_test(Config) ->
    Utf8Text = <<"Loïc was here!"/utf8>>,
    Utf8Payload = base64:encode(Utf8Text),
    BinPayload = base64:encode(<<0:64, 16#ff, 16#fd, 0:64>>),
    Utf8Msg = msg(<<"get_encoding_test">>, #{}, Utf8Payload, <<"base64">>),
    BinMsg  = msg(<<"get_encoding_test">>, #{}, BinPayload, <<"base64">>),
    http_put(Config, "/queues/%2F/get_encoding_test", #{}, {group, '2xx'}),
    http_post(Config, "/exchanges/%2F/amq.default/publish", Utf8Msg, ?OK),
    http_post(Config, "/exchanges/%2F/amq.default/publish", BinMsg,  ?OK),

    [RecvUtf8Msg1, RecvBinMsg1] = http_post(Config, "/queues/%2F/get_encoding_test/get",
                                                          [{ackmode, ack_requeue_false},
                                                           {count,    2},
                                                           {encoding, auto}], ?OK),
    %% Utf-8 payload must be returned as a utf-8 string when auto encoding is used.
    ?assertEqual(<<"string">>, maps:get(payload_encoding, RecvUtf8Msg1)),
    ?assertEqual(Utf8Text, maps:get(payload, RecvUtf8Msg1)),
    %% Binary payload must be base64-encoded when auto is used.
    ?assertEqual(<<"base64">>, maps:get(payload_encoding, RecvBinMsg1)),
    ?assertEqual(BinPayload, maps:get(payload, RecvBinMsg1)),
    %% Good. Now try forcing the base64 encoding.
    http_post(Config, "/exchanges/%2F/amq.default/publish", Utf8Msg, ?OK),
    http_post(Config, "/exchanges/%2F/amq.default/publish", BinMsg,  ?OK),
    [RecvUtf8Msg2, RecvBinMsg2] = http_post(Config, "/queues/%2F/get_encoding_test/get",
                                                          [{ackmode, ack_requeue_false},
                                                           {count,    2},
                                                           {encoding, base64}], ?OK),
    %% All payloads must be base64-encoded when base64 encoding is used.
    ?assertEqual(<<"base64">>, maps:get(payload_encoding, RecvUtf8Msg2)),
    ?assertEqual(Utf8Payload, maps:get(payload, RecvUtf8Msg2)),
    ?assertEqual(<<"base64">>, maps:get(payload_encoding, RecvBinMsg2)),
    ?assertEqual(BinPayload, maps:get(payload, RecvBinMsg2)),
    http_delete(Config, "/queues/%2F/get_encoding_test", {group, '2xx'}),
    passed.

get_fail_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/queues/%2F/myqueue", #{}, {group, '2xx'}),
    http_post(Config, "/queues/%2F/myqueue/get",
              [{ackmode, ack_requeue_false},
               {count,    1},
               {encoding, auto}], "myuser", "password", ?NOT_AUTHORISED),
    http_delete(Config, "/queues/%2F/myqueue", {group, '2xx'}),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.


-define(LARGE_BODY_BYTES, 5000000).

publish_test(Config) ->
    Headers = #{'x-forwarding' => [#{uri => <<"amqp://localhost/%2F/upstream">>}]},
    Msg = msg(<<"publish_test">>, Headers, <<"Hello world">>),
    http_put(Config, "/queues/%2F/publish_test", #{}, {group, '2xx'}),
    ?assertEqual(#{routed => true},
                  http_post(Config, "/exchanges/%2F/amq.default/publish", Msg, ?OK)),
    [Msg2] = http_post(Config, "/queues/%2F/publish_test/get", [{ackmode, ack_requeue_false},
                                                                {count,    1},
                                                                {encoding, auto}], ?OK),
    assert_item(Msg, Msg2),
    http_post(Config, "/exchanges/%2F/amq.default/publish", Msg2, ?OK),
    [Msg3] = http_post(Config, "/queues/%2F/publish_test/get", [{ackmode, ack_requeue_false},
                                                               {count,    1},
                                                               {encoding, auto}], ?OK),
    assert_item(Msg, Msg3),
    http_delete(Config, "/queues/%2F/publish_test", {group, '2xx'}),
    passed.

publish_large_message_test(Config) ->
  Headers = #{'x-forwarding' => [#{uri => <<"amqp://localhost/%2F/upstream">>}]},
  Body = binary:copy(<<"a">>, ?LARGE_BODY_BYTES),
  Msg = msg(<<"publish_accept_json_test">>, Headers, Body),
  http_put(Config, "/queues/%2F/publish_accept_json_test", #{}, {group, '2xx'}),
  ?assertEqual(#{routed => true},
               http_post_accept_json(Config, "/exchanges/%2F/amq.default/publish",
                                     Msg, ?OK)),

  [Msg2] = http_post_accept_json(Config, "/queues/%2F/publish_accept_json_test/get",
                                 [{ackmode, ack_requeue_false},
                                  {count, 1},
                                  {encoding, auto}], ?OK),
  assert_item(Msg, Msg2),
  http_post_accept_json(Config, "/exchanges/%2F/amq.default/publish", Msg2, ?OK),
  [Msg3] = http_post_accept_json(Config, "/queues/%2F/publish_accept_json_test/get",
                                 [{ackmode, ack_requeue_false},
                                  {count, 1},
                                  {encoding, auto}], ?OK),
  assert_item(Msg, Msg3),
  http_delete(Config, "/queues/%2F/publish_accept_json_test", {group, '2xx'}),
  passed.

-define(EXCESSIVELY_LARGE_BODY_BYTES, 35000000).

publish_large_message_exceeding_http_request_body_size_test(Config) ->
  Headers = #{'x-forwarding' => [#{uri => <<"amqp://localhost/%2F/upstream">>}]},
  Body = binary:copy(<<"a">>, ?EXCESSIVELY_LARGE_BODY_BYTES),
  Msg = msg(<<"large_message_exceeding_http_request_body_size_test">>, Headers, Body),
  http_put(Config, "/queues/%2F/large_message_exceeding_http_request_body_size_test", #{}, {group, '2xx'}),
  %% exceeds the default HTTP API request body size limit
  http_post_accept_json(Config, "/exchanges/%2F/amq.default/publish",
                                     Msg, ?BAD_REQUEST),
  http_delete(Config, "/queues/%2F/large_message_exceeding_http_request_body_size_test", {group, '2xx'}),
  passed.

publish_accept_json_test(Config) ->
    Headers = #{'x-forwarding' => [#{uri => <<"amqp://localhost/%2F/upstream">>}]},
    Msg = msg(<<"publish_accept_json_test">>, Headers, <<"Hello world">>),
    http_put(Config, "/queues/%2F/publish_accept_json_test", #{}, {group, '2xx'}),
    ?assertEqual(#{routed => true},
                 http_post_accept_json(Config, "/exchanges/%2F/amq.default/publish",
                                       Msg, ?OK)),

    [Msg2] = http_post_accept_json(Config, "/queues/%2F/publish_accept_json_test/get",
                                   [{ackmode, ack_requeue_false},
                                    {count, 1},
                                    {encoding, auto}], ?OK),
    assert_item(Msg, Msg2),
    http_post_accept_json(Config, "/exchanges/%2F/amq.default/publish", Msg2, ?OK),
    [Msg3] = http_post_accept_json(Config, "/queues/%2F/publish_accept_json_test/get",
                                   [{ackmode, ack_requeue_false},
                                    {count, 1},
                                    {encoding, auto}], ?OK),
    assert_item(Msg, Msg3),
    http_delete(Config, "/queues/%2F/publish_accept_json_test", {group, '2xx'}),
    passed.

publish_fail_test(Config) ->
    Msg = msg(<<"publish_fail_test">>, [], <<"Hello world">>),
    http_put(Config, "/queues/%2F/publish_fail_test", #{}, {group, '2xx'}),
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    http_post(Config, "/exchanges/%2F/amq.default/publish", Msg, "myuser", "password",
              ?NOT_AUTHORISED),
    Msg2 = [{exchange,         <<"">>},
            {routing_key,      <<"publish_fail_test">>},
            {properties,       [{user_id, <<"foo">>}]},
            {payload,          <<"Hello world">>},
            {payload_encoding, <<"string">>}],
    http_post(Config, "/exchanges/%2F/amq.default/publish", Msg2, ?BAD_REQUEST),
    Msg3 = [{exchange,         <<"">>},
            {routing_key,      <<"publish_fail_test">>},
            {properties,       []},
            {payload,          [<<"not a string">>]},
            {payload_encoding, <<"string">>}],
    http_post(Config, "/exchanges/%2F/amq.default/publish", Msg3, ?BAD_REQUEST),
    MsgTemplate = [{exchange,         <<"">>},
                   {routing_key,      <<"publish_fail_test">>},
                   {payload,          <<"Hello world">>},
                   {payload_encoding, <<"string">>}],
    [http_post(Config, "/exchanges/%2F/amq.default/publish",
               [{properties, [BadProp]} | MsgTemplate], ?BAD_REQUEST)
     || BadProp <- [{priority,   <<"really high">>},
                    {timestamp,  <<"recently">>},
                    {expiration, 1234}]],
    http_delete(Config, "/queues/%2F/publish_fail_test", {group, '2xx'}),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

publish_base64_test(Config) ->
    %% "abcd"
    %% @todo Note that we used to accept [] instead of {struct, []} when we shouldn't have.
    %% This is a breaking change and probably needs to be documented.
    Msg     = msg(<<"publish_base64_test">>, #{}, <<"YWJjZA==">>, <<"base64">>),
    BadMsg1 = msg(<<"publish_base64_test">>, #{}, <<"flibble">>,  <<"base64">>),
    BadMsg2 = msg(<<"publish_base64_test">>, #{}, <<"YWJjZA==">>, <<"base99">>),
    http_put(Config, "/queues/%2F/publish_base64_test", #{}, {group, '2xx'}),
    http_post(Config, "/exchanges/%2F/amq.default/publish", Msg, ?OK),
    http_post(Config, "/exchanges/%2F/amq.default/publish", BadMsg1, ?BAD_REQUEST),
    http_post(Config, "/exchanges/%2F/amq.default/publish", BadMsg2, ?BAD_REQUEST),

    [Msg2] = http_post(Config, "/queues/%2F/publish_base64_test/get", [{ackmode, ack_requeue_false},
                                                           {count,    1},
                                                           {encoding, auto}], ?OK),
    ?assertEqual(<<"abcd">>, maps:get(payload, Msg2)),
    http_delete(Config, "/queues/%2F/publish_base64_test", {group, '2xx'}),
    passed.

publish_unrouted_test(Config) ->
    Msg = msg(<<"hmmm">>, #{}, <<"Hello world">>),
    ?assertEqual(#{routed => false},
                 http_post(Config, "/exchanges/%2F/amq.default/publish", Msg, ?OK)).

if_empty_unused_test(Config) ->
    http_put(Config, "/exchanges/%2F/test", #{}, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test", #{}, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/test/q/test", #{}, {group, '2xx'}),
    http_post(Config, "/exchanges/%2F/amq.default/publish",
              msg(<<"test">>, #{}, <<"Hello world">>), ?OK),
    http_delete(Config, "/queues/%2F/test?if-empty=true", ?BAD_REQUEST),
    http_delete(Config, "/exchanges/%2F/test?if-unused=true", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/test/contents", {group, '2xx'}),

    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = <<"test">> }, self()),
    http_delete(Config, "/queues/%2F/test?if-unused=true", ?BAD_REQUEST),
    amqp_connection:close(Conn),

    http_delete(Config, "/queues/%2F/test?if-empty=true", {group, '2xx'}),
    http_delete(Config, "/exchanges/%2F/test?if-unused=true", {group, '2xx'}),
    passed.

parameters_test(Config) ->
    register_parameters_and_policy_validator(Config),

    http_put(Config, "/parameters/test/%2F/good", [{value, <<"ignore">>}], {group, '2xx'}),
    http_put(Config, "/parameters/test/%2F/maybe", [{value, <<"good">>}], {group, '2xx'}),
    http_put(Config, "/parameters/test/%2F/maybe", [{value, <<"bad">>}], ?BAD_REQUEST),
    http_put(Config, "/parameters/test/%2F/bad", [{value, <<"good">>}], ?BAD_REQUEST),
    http_put(Config, "/parameters/test/um/good", [{value, <<"ignore">>}], ?NOT_FOUND),

    Good = #{vhost     => <<"/">>,
             component => <<"test">>,
             name      => <<"good">>,
             value     => <<"ignore">>},
    Maybe = #{vhost     => <<"/">>,
              component => <<"test">>,
              name      => <<"maybe">>,
              value     => <<"good">>},
    List = [Good, Maybe],

    assert_list(List, http_get(Config, "/parameters")),
    assert_list(List, http_get(Config, "/parameters/test")),
    assert_list(List, http_get(Config, "/parameters/test/%2F")),
    assert_list([],   http_get(Config, "/parameters/oops")),
    http_get(Config, "/parameters/test/oops", ?NOT_FOUND),

    assert_item(Good,  http_get(Config, "/parameters/test/%2F/good", ?OK)),
    assert_item(Maybe, http_get(Config, "/parameters/test/%2F/maybe", ?OK)),

    http_delete(Config, "/parameters/test/%2F/good", {group, '2xx'}),
    http_delete(Config, "/parameters/test/%2F/maybe", {group, '2xx'}),
    http_delete(Config, "/parameters/test/%2F/bad", ?NOT_FOUND),

    0 = length(http_get(Config, "/parameters")),
    0 = length(http_get(Config, "/parameters/test")),
    0 = length(http_get(Config, "/parameters/test/%2F")),
    unregister_parameters_and_policy_validator(Config),
    passed.

global_parameters_test(Config) ->
    InitialParameters = http_get(Config, "/global-parameters"),
    http_put(Config, "/global-parameters/good", [{value, [{a, <<"b">>}]}], {group, '2xx'}),
    http_put(Config, "/global-parameters/maybe", [{value,[{c, <<"d">>}]}], {group, '2xx'}),

    Good  = #{name  =>  <<"good">>,
              value =>  #{a => <<"b">>}},
    Maybe = #{name  =>  <<"maybe">>,
              value =>  #{c => <<"d">>}},
    List  = InitialParameters ++ [Good, Maybe],

    assert_list(List, http_get(Config, "/global-parameters")),
    http_get(Config, "/global-parameters/oops", ?NOT_FOUND),

    assert_item(Good,  http_get(Config, "/global-parameters/good", ?OK)),
    assert_item(Maybe, http_get(Config, "/global-parameters/maybe", ?OK)),

    http_delete(Config, "/global-parameters/good", {group, '2xx'}),
    http_delete(Config, "/global-parameters/maybe", {group, '2xx'}),
    http_delete(Config, "/global-parameters/bad", ?NOT_FOUND),

    InitialCount = length(InitialParameters),
    InitialCount = length(http_get(Config, "/global-parameters")),
    passed.

operator_policy_test(Config) ->
    register_parameters_and_policy_validator(Config),
    PolicyPos  = #{vhost      => <<"/">>,
                   name       => <<"policy_pos">>,
                   pattern    => <<".*">>,
                   definition => #{testpos => [1,2,3]},
                   priority   => 10},
    PolicyEven = #{vhost      => <<"/">>,
                   name       => <<"policy_even">>,
                   pattern    => <<".*">>,
                   definition => #{testeven => [1,2,3,4]},
                   priority   => 10},
    http_put(Config,
             "/operator-policies/%2F/policy_pos",
             PolicyPos,
             {group, '2xx'}),
    http_put(Config,
             "/operator-policies/%2F/policy_even",
             PolicyEven,
             {group, '2xx'}),
    assert_item(PolicyPos,  http_get(Config, "/operator-policies/%2F/policy_pos",  ?OK)),
    assert_item(PolicyEven, http_get(Config, "/operator-policies/%2F/policy_even", ?OK)),
    List = [PolicyPos, PolicyEven],
    assert_list(List, http_get(Config, "/operator-policies",     ?OK)),
    assert_list(List, http_get(Config, "/operator-policies/%2F", ?OK)),

    http_delete(Config, "/operator-policies/%2F/policy_pos", {group, '2xx'}),
    http_delete(Config, "/operator-policies/%2F/policy_even", {group, '2xx'}),
    0 = length(http_get(Config, "/operator-policies")),
    0 = length(http_get(Config, "/operator-policies/%2F")),
    unregister_parameters_and_policy_validator(Config),
    passed.

disabled_operator_policy_test(Config) ->
    register_parameters_and_policy_validator(Config),
    PolicyPos  = #{vhost      => <<"/">>,
                   name       => <<"policy_pos">>,
                   pattern    => <<".*">>,
                   definition => #{testpos => [1,2,3]},
                   priority   => 10},
    http_put(Config, "/operator-policies/%2F/policy_pos", PolicyPos, ?METHOD_NOT_ALLOWED),
    http_delete(Config, "/operator-policies/%2F/policy_pos", ?METHOD_NOT_ALLOWED),
    0 = length(http_get(Config, "/operator-policies",     ?OK)),
    0 = length(http_get(Config, "/operator-policies/%2F", ?OK)),
    unregister_parameters_and_policy_validator(Config),
    passed.

policy_test(Config) ->
    register_parameters_and_policy_validator(Config),
    PolicyPos  = #{vhost      => <<"/">>,
                   name       => <<"policy_pos">>,
                   pattern    => <<".*">>,
                   definition => #{testpos => [1,2,3]},
                   priority   => 10},
    PolicyEven = #{vhost      => <<"/">>,
                   name       => <<"policy_even">>,
                   pattern    => <<".*">>,
                   definition => #{testeven => [1,2,3,4]},
                   priority   => 10},
    http_put(Config,
             "/policies/%2F/policy_pos",
             maps:remove(key, PolicyPos),
             {group, '2xx'}),
    http_put(Config,
             "/policies/%2F/policy_even",
             maps:remove(key, PolicyEven),
             {group, '2xx'}),
    assert_item(PolicyPos,  http_get(Config, "/policies/%2F/policy_pos",  ?OK)),
    assert_item(PolicyEven, http_get(Config, "/policies/%2F/policy_even", ?OK)),
    List = [PolicyPos, PolicyEven],
    assert_list(List, http_get(Config, "/policies",     ?OK)),
    assert_list(List, http_get(Config, "/policies/%2F", ?OK)),

    http_delete(Config, "/policies/%2F/policy_pos", {group, '2xx'}),
    http_delete(Config, "/policies/%2F/policy_even", {group, '2xx'}),
    0 = length(http_get(Config, "/policies")),
    0 = length(http_get(Config, "/policies/%2F")),
    unregister_parameters_and_policy_validator(Config),
    passed.

policy_permissions_test(Config) ->
    register_parameters_and_policy_validator(Config),

    http_put(Config, "/users/admin",  [{password, <<"admin">>},
                                       {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/mon",    [{password, <<"mon">>},
                                       {tags, <<"monitoring">>}], {group, '2xx'}),
    http_put(Config, "/users/policy", [{password, <<"policy">>},
                                       {tags, <<"policymaker">>}], {group, '2xx'}),
    http_put(Config, "/users/mgmt",   [{password, <<"mgmt">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put(Config, "/vhosts/v", none, {group, '2xx'}),
    http_put(Config, "/permissions/v/admin",  Perms, {group, '2xx'}),
    http_put(Config, "/permissions/v/mon",    Perms, {group, '2xx'}),
    http_put(Config, "/permissions/v/policy", Perms, {group, '2xx'}),
    http_put(Config, "/permissions/v/mgmt",   Perms, {group, '2xx'}),

    Policy = [{pattern,    <<".*">>},
              {definition, [{<<"max-length-bytes">>, 3000000}]}],
    Param = [{value, <<"">>}],

    http_put(Config, "/policies/%2F/HA", Policy, {group, '2xx'}),
    http_put(Config, "/parameters/test/%2F/good", Param, {group, '2xx'}),

    Pos = fun (U) ->
                  http_put(Config, "/policies/v/HA", Policy, U, U, {group, '2xx'}),
                  http_put(Config, "/parameters/test/v/good", Param, U, U, {group, '2xx'}),
                  http_get(Config, "/policies",          U, U, {group, '2xx'}),
                  http_get(Config, "/parameters/test",   U, U, {group, '2xx'}),
                  http_get(Config, "/parameters",        U, U, {group, '2xx'}),
                  http_get(Config, "/policies/v",        U, U, {group, '2xx'}),
                  http_get(Config, "/parameters/test/v", U, U, {group, '2xx'}),
                  http_get(Config, "/policies/v/HA",          U, U, {group, '2xx'}),
                  http_get(Config, "/parameters/test/v/good", U, U, {group, '2xx'})
          end,
    Neg = fun (U) ->
                  http_put(Config, "/policies/v/HA",    Policy, U, U, ?NOT_AUTHORISED),
                  http_put(Config,
                           "/parameters/test/v/good",   Param, U, U, ?NOT_AUTHORISED),
                  http_put(Config,
                           "/parameters/test/v/admin",  Param, U, U, ?NOT_AUTHORISED),
                  %% Policies are read-only for management and monitoring.
                  http_get(Config, "/policies",                 U, U, ?OK),
                  http_get(Config, "/policies/v",               U, U, ?OK),
                  http_get(Config, "/parameters",               U, U, ?NOT_AUTHORISED),
                  http_get(Config, "/parameters/test",          U, U, ?NOT_AUTHORISED),
                  http_get(Config, "/parameters/test/v",        U, U, ?NOT_AUTHORISED),
                  http_get(Config, "/policies/v/HA",            U, U, ?NOT_AUTHORISED),
                  http_get(Config, "/parameters/test/v/good",   U, U, ?NOT_AUTHORISED)
          end,
    AlwaysNeg =
        fun (U) ->
                http_put(Config, "/policies/%2F/HA",  Policy, U, U, ?NOT_AUTHORISED),
                http_put(Config, "/parameters/test/%2F/good", Param, U, U, ?NOT_AUTHORISED),
                http_get(Config, "/policies/%2F/HA",          U, U, ?NOT_AUTHORISED),
                http_get(Config, "/parameters/test/%2F/good", U, U, ?NOT_AUTHORISED)
        end,
    AdminPos =
        fun (U) ->
                http_put(Config, "/policies/%2F/HA",  Policy, U, U, {group, '2xx'}),
                http_put(Config, "/parameters/test/%2F/good", Param, U, U, {group, '2xx'}),
                http_get(Config, "/policies/%2F/HA",          U, U, {group, '2xx'}),
                http_get(Config, "/parameters/test/%2F/good", U, U, {group, '2xx'})
        end,

    [Neg(U) || U <- ["mon", "mgmt"]],
    [Pos(U) || U <- ["admin", "policy"]],
    [AlwaysNeg(U) || U <- ["mon", "mgmt", "policy"]],
    [AdminPos(U) || U <- ["admin"]],

    %% This one is deliberately different between admin and policymaker.
    http_put(Config, "/parameters/test/v/admin", Param, "admin", "admin", {group, '2xx'}),
    http_put(Config, "/parameters/test/v/admin", Param, "policy", "policy",
             ?BAD_REQUEST),

    http_delete(Config, "/vhosts/v", {group, '2xx'}),
    http_delete(Config, "/users/admin", {group, '2xx'}),
    http_delete(Config, "/users/mon", {group, '2xx'}),
    http_delete(Config, "/users/policy", {group, '2xx'}),
    http_delete(Config, "/users/mgmt", {group, '2xx'}),
    http_delete(Config, "/policies/%2F/HA", {group, '2xx'}),

    unregister_parameters_and_policy_validator(Config),
    passed.

issue67_test(Config)->
    {ok, {{_, 401, _}, Headers, _}} = req(Config, get, "/queues",
                                          [auth_header("user_no_access", "password_no_access")]),
    ?assertEqual("application/json",
                 proplists:get_value("content-type",Headers)),
    passed.

extensions_test(Config) ->
    [#{javascript := <<"dispatcher.js">>}] = http_get(Config, "/extensions", ?OK),
    passed.


cors_test(Config) ->
    %% With CORS disabled. No header should be received.
    R = req(Config, get, "/overview", [auth_header("guest", "guest")]),
    io:format("CORS test R: ~tp~n", [R]),
    {ok, {_, HdNoCORS, _}} = R,
    io:format("CORS test HdNoCORS: ~tp~n", [HdNoCORS]),
    false = lists:keymember("access-control-allow-origin", 1, HdNoCORS),
    %% The Vary header should include "Origin" regardless of CORS configuration.
    {_, "accept, accept-encoding, origin"} = lists:keyfind("vary", 1, HdNoCORS),
    %% Enable CORS.
    rpc(Config, application, set_env, [rabbitmq_management, cors_allow_origins, ["https://rabbitmq.com"]]),
    %% We should only receive allow-origin and allow-credentials from GET.
    {ok, {_, HdGetCORS, _}} = req(Config, get, "/overview",
                                  [{"origin", "https://rabbitmq.com"}, auth_header("guest", "guest")]),
    true = lists:keymember("access-control-allow-origin", 1, HdGetCORS),
    true = lists:keymember("access-control-allow-credentials", 1, HdGetCORS),
    false = lists:keymember("access-control-expose-headers", 1, HdGetCORS),
    false = lists:keymember("access-control-max-age", 1, HdGetCORS),
    false = lists:keymember("access-control-allow-methods", 1, HdGetCORS),
    false = lists:keymember("access-control-allow-headers", 1, HdGetCORS),
    %% We should receive allow-origin, allow-credentials and allow-methods from OPTIONS.
    {ok, {{_, 200, _}, HdOptionsCORS, _}} = req(Config, options, "/overview",
                                                [{"origin", "https://rabbitmq.com"}]),
    true = lists:keymember("access-control-allow-origin", 1, HdOptionsCORS),
    true = lists:keymember("access-control-allow-credentials", 1, HdOptionsCORS),
    false = lists:keymember("access-control-expose-headers", 1, HdOptionsCORS),
    true = lists:keymember("access-control-max-age", 1, HdOptionsCORS),
    true = lists:keymember("access-control-allow-methods", 1, HdOptionsCORS),
    false = lists:keymember("access-control-allow-headers", 1, HdOptionsCORS),
    %% We should receive allow-headers when request-headers is sent.
    {ok, {_, HdAllowHeadersCORS, _}} = req(Config, options, "/overview",
                                           [{"origin", "https://rabbitmq.com"},
                                            auth_header("guest", "guest"),
                                            {"access-control-request-headers", "x-piggy-bank"}]),
    {_, "x-piggy-bank"} = lists:keyfind("access-control-allow-headers", 1, HdAllowHeadersCORS),
    %% Disable preflight request caching.
    rpc(Config, application, set_env, [rabbitmq_management, cors_max_age, undefined]),
    %% We shouldn't receive max-age anymore.
    {ok, {_, HdNoMaxAgeCORS, _}} = req(Config, options, "/overview",
                                       [{"origin", "https://rabbitmq.com"}, auth_header("guest", "guest")]),
    false = lists:keymember("access-control-max-age", 1, HdNoMaxAgeCORS),

    %% Check OPTIONS method in all paths
    check_cors_all_endpoints(Config),
    %% Disable CORS again.
    rpc(Config, application, set_env, [rabbitmq_management, cors_allow_origins, []]),
    passed.

check_cors_all_endpoints(Config) ->
    Endpoints = get_all_http_endpoints(),

    [begin
        ct:pal("Verifying CORS for module ~tp using an OPTIONS request~n", [EP]),
        {ok, {{_, 200, _}, _, _}} = req(Config, options, EP, [{"origin", "https://rabbitmq.com"}])
    end
     || EP <- Endpoints].

get_all_http_endpoints() ->
    [ Path || {Path, _, _} <- rabbit_mgmt_dispatcher:dispatcher() ].

vhost_limits_list_test(Config) ->
    [] = http_get(Config, "/vhost-limits", ?OK),

    http_get(Config, "/vhost-limits/limit_test_vhost_1", ?NOT_FOUND),
    rabbit_ct_broker_helpers:add_vhost(Config, <<"limit_test_vhost_1">>),

    [] = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),
    http_get(Config, "/vhost-limits/limit_test_vhost_2", ?NOT_FOUND),

    rabbit_ct_broker_helpers:add_vhost(Config, <<"limit_test_vhost_2">>),

    [] = http_get(Config, "/vhost-limits/limit_test_vhost_2", ?OK),

    Limits1 = [#{vhost => <<"limit_test_vhost_1">>,
                 value => #{'max-connections' => 100, 'max-queues' => 100}}],
    Limits2 = [#{vhost => <<"limit_test_vhost_2">>,
                 value => #{'max-connections' => 200}}],

    Expected = Limits1 ++ Limits2,

    lists:map(
        fun(#{vhost := VHost, value := Val}) ->
            Param = [ {atom_to_binary(K, utf8),V} || {K,V} <- maps:to_list(Val) ],
            ct:pal("Setting limits of virtual host '~ts' to ~tp", [VHost, Param]),
            ok = rabbit_ct_broker_helpers:set_parameter(Config, 0, VHost, <<"vhost-limits">>, <<"limits">>, Param)
        end,
        Expected),

    ?assertEqual(lists:usort(Expected), lists:usort(http_get(Config, "/vhost-limits", ?OK))),
    ?assertEqual(Limits1, http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK)),
    ?assertEqual(Limits2, http_get(Config, "/vhost-limits/limit_test_vhost_2", ?OK)),

    NoVhostUser = <<"no_vhost_user">>,
    rabbit_ct_broker_helpers:add_user(Config, NoVhostUser),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, NoVhostUser, [management]),
    [] = http_get(Config, "/vhost-limits", NoVhostUser, NoVhostUser, ?OK),
    http_get(Config, "/vhost-limits/limit_test_vhost_1", NoVhostUser, NoVhostUser, ?NOT_AUTHORISED),
    http_get(Config, "/vhost-limits/limit_test_vhost_2", NoVhostUser, NoVhostUser, ?NOT_AUTHORISED),

    Vhost1User = <<"limit_test_vhost_1_user">>,
    rabbit_ct_broker_helpers:add_user(Config, Vhost1User),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, Vhost1User, [management]),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Vhost1User, <<"limit_test_vhost_1">>),
    Limits1 = http_get(Config, "/vhost-limits", Vhost1User, Vhost1User, ?OK),
    Limits1 = http_get(Config, "/vhost-limits/limit_test_vhost_1", Vhost1User, Vhost1User, ?OK),
    http_get(Config, "/vhost-limits/limit_test_vhost_2", Vhost1User, Vhost1User, ?NOT_AUTHORISED),

    Vhost2User = <<"limit_test_vhost_2_user">>,
    rabbit_ct_broker_helpers:add_user(Config, Vhost2User),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, Vhost2User, [management]),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Vhost2User, <<"limit_test_vhost_2">>),
    Limits2 = http_get(Config, "/vhost-limits", Vhost2User, Vhost2User, ?OK),
    http_get(Config, "/vhost-limits/limit_test_vhost_1", Vhost2User, Vhost2User, ?NOT_AUTHORISED),
    Limits2 = http_get(Config, "/vhost-limits/limit_test_vhost_2", Vhost2User, Vhost2User, ?OK).

vhost_limit_set_test(Config) ->
    [] = http_get(Config, "/vhost-limits", ?OK),
    rabbit_ct_broker_helpers:add_vhost(Config, <<"limit_test_vhost_1">>),
    [] = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),

    %% Set a limit
    http_put(Config, "/vhost-limits/limit_test_vhost_1/max-queues", [{value, 100}], ?NO_CONTENT),


    Limits_Queues =  [#{vhost => <<"limit_test_vhost_1">>,
        value => #{'max-queues' => 100}}],

    Limits_Queues = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),

    %% Set another limit
    http_put(Config, "/vhost-limits/limit_test_vhost_1/max-connections", [{value, 200}], ?NO_CONTENT),

    Limits_Queues_Connections = [#{vhost => <<"limit_test_vhost_1">>,
        value => #{'max-connections' => 200, 'max-queues' => 100}}],

    Limits_Queues_Connections = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),

    Limits1 = [#{vhost => <<"limit_test_vhost_1">>,
       value => #{'max-connections' => 200, 'max-queues' => 100}}],
    Limits1 = http_get(Config, "/vhost-limits", ?OK),

    %% Update a limit
    http_put(Config, "/vhost-limits/limit_test_vhost_1/max-connections", [{value, 1000}], ?NO_CONTENT),
    Limits2 = [#{vhost => <<"limit_test_vhost_1">>,
       value => #{'max-connections' => 1000, 'max-queues' => 100}}],
    Limits2 = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),


    Vhost1User = <<"limit_test_vhost_1_user">>,
    rabbit_ct_broker_helpers:add_user(Config, Vhost1User),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, Vhost1User, [management]),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Vhost1User, <<"limit_test_vhost_1">>),

    Limits3 = [#{vhost => <<"limit_test_vhost_1">>,
        value => #{'max-connections' => 1000,
      'max-queues' => 100}}],
    Limits3 = http_get(Config, "/vhost-limits/limit_test_vhost_1", Vhost1User, Vhost1User, ?OK),

    %% Only admin can update limits
    http_put(Config, "/vhost-limits/limit_test_vhost_1/max-connections", [{value, 300}], ?NO_CONTENT),

    %% Clear a limit
    http_delete(Config, "/vhost-limits/limit_test_vhost_1/max-connections", ?NO_CONTENT),
    Limits4 = [#{vhost => <<"limit_test_vhost_1">>,
        value => #{'max-queues' => 100}}],
    Limits4 = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),

    %% Only admin can clear limits
    http_delete(Config, "/vhost-limits/limit_test_vhost_1/max-queues", Vhost1User, Vhost1User, ?NOT_AUTHORISED),

    %% Unknown limit error
    http_put(Config, "/vhost-limits/limit_test_vhost_1/max-channels", [{value, 200}], ?BAD_REQUEST).

user_limits_list_test(Config) ->
    ?assertEqual([], http_get(Config, "/user-limits", ?OK)),

    Vhost1 = <<"limit_test_vhost_1">>,
    rabbit_ct_broker_helpers:add_vhost(Config, <<"limit_test_vhost_1">>),

    http_get(Config, "/user-limits/limit_test_user_1", ?NOT_FOUND),

    User1 = <<"limit_test_user_1">>,
    rabbit_ct_broker_helpers:add_user(Config, User1),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, User1, [management]),
    rabbit_ct_broker_helpers:set_full_permissions(Config, User1, Vhost1),

    ?assertEqual([], http_get(Config, "/user-limits/limit_test_user_1", ?OK)),
    http_get(Config, "/user-limits/limit_test_user_2", ?NOT_FOUND),

    User2 = <<"limit_test_user_2">>,
    rabbit_ct_broker_helpers:add_user(Config, User2),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, User2, [management]),
    rabbit_ct_broker_helpers:set_full_permissions(Config, User2, Vhost1),

    ?assertEqual([], http_get(Config, "/user-limits/limit_test_user_2", ?OK)),

    Limits1 = [
        #{
            user => User1,
            value => #{
                'max-connections' => 100,
                'max-channels' => 100
            }
        }],
    Limits2 = [
        #{
            user => User2,
            value => #{
                'max-connections' => 200
            }
        }],

    Expected = Limits1 ++ Limits2,

    lists:map(
        fun(#{user := Username, value := Val}) ->
            rabbit_ct_broker_helpers:set_user_limits(Config, 0, Username, Val)
        end,
        Expected),

    rabbit_ct_helpers:await_condition(
        fun() ->
            Expected =:= http_get(Config, "/user-limits", ?OK)
        end),
    Limits1 = http_get(Config, "/user-limits/limit_test_user_1", ?OK),
    Limits2 = http_get(Config, "/user-limits/limit_test_user_2", ?OK),

    %% Clear limits and assert
    rabbit_ct_broker_helpers:clear_user_limits(Config, 0, User1,
        <<"max-connections">>),

    Limits3 = [#{user => User1, value => #{'max-channels' => 100}}],
    ?assertEqual(Limits3, http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    rabbit_ct_broker_helpers:clear_user_limits(Config, 0, User1,
        <<"max-channels">>),
    ?assertEqual([], http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    rabbit_ct_broker_helpers:clear_user_limits(Config, 0, <<"limit_test_user_2">>,
        <<"all">>),
    ?assertEqual([], http_get(Config, "/user-limits/limit_test_user_2", ?OK)),

    %% Limit user with no vhost
    NoVhostUser = <<"no_vhost_user">>,
    rabbit_ct_broker_helpers:add_user(Config, NoVhostUser),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, NoVhostUser, [management]),

    Limits4 = #{
        user  => NoVhostUser,
        value => #{
            'max-connections' => 150,
            'max-channels' => 150
        }
    },
    rabbit_ct_broker_helpers:set_user_limits(Config, 0, NoVhostUser, maps:get(value, Limits4)),

    ?assertEqual([Limits4], http_get(Config, "/user-limits/no_vhost_user", ?OK)).

user_limit_set_test(Config) ->
    ?assertEqual([], http_get(Config, "/user-limits", ?OK)),

    User1 = <<"limit_test_user_1">>,
    rabbit_ct_broker_helpers:add_user(Config, User1),

    ?assertEqual([], http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    %% Set a user limit
    http_put(Config, "/user-limits/limit_test_user_1/max-channels", [{value, 100}], ?NO_CONTENT),

    MaxChannelsLimit = [#{user => User1, value => #{'max-channels' => 100}}],
    ?assertEqual(MaxChannelsLimit, http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    %% Set another user limit
    http_put(Config, "/user-limits/limit_test_user_1/max-connections", [{value, 200}], ?NO_CONTENT),

    MaxConnectionsAndChannelsLimit = [
        #{
            user => User1,
            value => #{
                'max-connections' => 200,
                'max-channels'    => 100
            }
        }
    ],
    ?assertEqual(MaxConnectionsAndChannelsLimit, http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    Limits1 = [
        #{
            user => User1,
            value => #{
                'max-connections' => 200,
                'max-channels'    => 100
            }
        }],
    ?assertEqual(Limits1, http_get(Config, "/user-limits", ?OK)),

    %% Update a user limit
    http_put(Config, "/user-limits/limit_test_user_1/max-connections", [{value, 1000}], ?NO_CONTENT),
    Limits2 = [
        #{
            user => User1,
            value => #{
              'max-connections' => 1000,
              'max-channels' => 100
            }
        }],
    ?assertEqual(Limits2, http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    Vhost1 = <<"limit_test_vhost_1">>,
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost1),

    Vhost1User = <<"limit_test_vhost_1_user">>,
    rabbit_ct_broker_helpers:add_user(Config, Vhost1User),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, Vhost1User, [management]),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Vhost1User, Vhost1),

    Limits3 = [
        #{
            user => User1,
            value => #{
                'max-connections' => 1000,
                'max-channels'    => 100
            }
        }],
    ?assertEqual(Limits3, http_get(Config, "/user-limits/limit_test_user_1", Vhost1User, Vhost1User, ?OK)),

    %% Clear a limit
    http_delete(Config, "/user-limits/limit_test_user_1/max-connections", ?NO_CONTENT),
    Limits4 = [#{user => User1, value => #{'max-channels' => 100}}],
    ?assertEqual(Limits4, http_get(Config, "/user-limits/limit_test_user_1", ?OK)),

    %% Only admin can clear limits
    http_delete(Config, "/user-limits/limit_test_user_1/max-channels", Vhost1User, Vhost1User, ?NOT_AUTHORISED),

    %% Unknown limit error
    http_put(Config, "/user-limits/limit_test_user_1/max-unknown", [{value, 200}], ?BAD_REQUEST).

rates_test(Config) ->
    http_put(Config, "/queues/%2F/myqueue", none, {group, '2xx'}),
    {Conn, Ch} = open_connection_and_channel(Config),
    Pid = spawn_link(fun() -> publish(Ch) end),

    Condition = fun() ->
        Overview = http_get(Config, "/overview"),
        MsgStats = maps:get(message_stats, Overview),
        QueueTotals = maps:get(queue_totals, Overview),

        maps:get(messages_ready, QueueTotals) > 0 andalso
        maps:get(messages, QueueTotals) > 0 andalso
        maps:get(publish, MsgStats) > 0 andalso
        maps:get(rate, maps:get(publish_details, MsgStats)) > 0 andalso
        maps:get(rate, maps:get(messages_ready_details, QueueTotals)) > 0 andalso
        maps:get(rate, maps:get(messages_details, QueueTotals)) > 0
    end,
    rabbit_ct_helpers:await_condition(Condition, 60000),
    Pid ! stop_publish,
    close_channel(Ch),
    close_connection(Conn),
    http_delete(Config, "/queues/%2F/myqueue", ?NO_CONTENT),
    passed.

cli_redirect_test(Config) ->
    assert_permanent_redirect(Config, "cli", "/cli/index.html"),
    passed.

api_redirect_test(Config) ->
    assert_permanent_redirect(Config, "api", "/api/index.html"),
    passed.

stats_redirect_test(Config) ->
    assert_permanent_redirect(Config, "doc/stats.html", "/api/index.html"),
    passed.

oauth_test(Config) ->
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, "rabbitmq_auth_backend_oauth2"),

    Map1 = http_get(Config, "/auth", ?OK),
    %% Defaults
    ?assertEqual(false, maps:get(oauth_enabled, Map1)),

    %% Misconfiguration
    rpc(Config, application, set_env, [rabbitmq_management, oauth_enabled, true]),
    Map2 = http_get(Config, "/auth", ?OK),
    ?assertEqual(false, maps:get(oauth_enabled, Map2)),
    ?assertEqual(<<>>, maps:get(oauth_client_id, Map2)),
    ?assertEqual(<<>>, maps:get(oauth_provider_url, Map2)),
    %% Valid config requires non empty OAuthClientId, OAuthClientSecret, OAuthResourceId, OAuthProviderUrl
    rpc(Config, application, set_env, [rabbitmq_management, oauth_client_id, "rabbit_user"]),
    rpc(Config, application, set_env, [rabbitmq_management, oauth_client_secret, "rabbit_secret"]),
    rpc(Config, application, set_env, [rabbitmq_management, oauth_provider_url, "http://localhost:8080/uaa"]),
    rpc(Config, application, set_env, [rabbitmq_auth_backend_oauth2, resource_server_id, "rabbitmq"]),
    Map3 = http_get(Config, "/auth", ?OK),
    println(Map3),
    ?assertEqual(true, maps:get(oauth_enabled, Map3)),
    ?assertEqual(<<"rabbit_user">>, maps:get(oauth_client_id, Map3)),
    ?assertEqual(<<"rabbit_secret">>, maps:get(oauth_client_secret, Map3)),
    ?assertEqual(<<"rabbitmq">>, maps:get(resource_server_id, Map3)),
    ?assertEqual(<<"http://localhost:8080/uaa">>, maps:get(oauth_provider_url, Map3)),
    %% cleanup
    rpc(Config, application, unset_env, [rabbitmq_management, oauth_enabled]).

version_test(Config) ->
    ActualVersion = http_get(Config, "/version"),
    ct:log("ActualVersion : ~p", [ActualVersion]),
    ExpectedVersion = rpc(Config, rabbit, base_product_version, []),
    ct:log("ExpectedVersion : ~p", [ExpectedVersion]),
    ?assertEqual(ExpectedVersion, binary_to_list(ActualVersion)).

login_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags,     <<"management">>}], {group, '2xx'}),
    %% Let's do a post without any other form of authorization
    {ok, {{_, CodeAct, _}, Headers, _}} =
        req(Config, 0, post, "/login",
            [{"content-type", "application/x-www-form-urlencoded"}],
            <<"username=myuser&password=myuser">>),
    ?assertEqual(200, CodeAct),

    %% Extract the authorization header
    Cookie = list_to_binary(proplists:get_value("set-cookie", Headers)),
    [_, Auth] = binary:split(Cookie, <<"=">>, []),

    %% Request the overview with the auth obtained
    {ok, {{_, CodeAct1, _}, _, _}} =
        req(Config, get, "/overview", [{"Authorization", "Basic " ++ binary_to_list(Auth)}]),
    ?assertEqual(200, CodeAct1),

    %% Let's request a login with an unknown user
    {ok, {{_, CodeAct2, _}, Headers2, _}} =
        req(Config, 0, post, "/login",
            [{"content-type", "application/x-www-form-urlencoded"}],
            <<"username=misteryusernumber1&password=myuser">>),
    ?assertEqual(401, CodeAct2),
    ?assert(not proplists:is_defined("set-cookie", Headers2)),

    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

csp_headers_test(Config) ->
    AuthHeader = auth_header("guest", "guest"),
    Headers = [{"origin", "https://rabbitmq.com"}, AuthHeader],
    {ok, {_, HdGetCsp0, _}} = req(Config, get, "/whoami", Headers),
    ?assert(lists:keymember("content-security-policy", 1, HdGetCsp0)),
    {ok, {_, HdGetCsp1, _}} = req(Config, get_static, "/index.html", Headers),
    ?assert(lists:keymember("content-security-policy", 1, HdGetCsp1)).

disable_basic_auth_test(Config) ->
    rpc(Config, application, set_env, [rabbitmq_management, disable_basic_auth, true]),
    http_get(Config, "/overview", ?NOT_AUTHORISED),

    %% Ensure that a request without auth header does not return a basic auth prompt
    OverviewResponseHeaders = http_get_no_auth(Config, "/overview", ?NOT_AUTHORISED),
    ?assertEqual(false, lists:keymember("www-authenticate", 1,  OverviewResponseHeaders)),

    http_get(Config, "/nodes", ?NOT_AUTHORISED),
    http_get(Config, "/vhosts", ?NOT_AUTHORISED),
    http_get(Config, "/vhost-limits", ?NOT_AUTHORISED),
    http_put(Config, "/queues/%2F/myqueue", none, ?NOT_AUTHORISED),
    Policy = [{pattern,    <<".*">>},
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
    http_put(Config, "/policies/%2F/HA",  Policy, ?NOT_AUTHORISED),
    http_delete(Config, "/queues/%2F/myqueue", ?NOT_AUTHORISED),
    http_get(Config, "/definitions", ?NOT_AUTHORISED),
    http_post(Config, "/definitions", [], ?NOT_AUTHORISED),
    rpc(Config, application, set_env, [rabbitmq_management, disable_basic_auth, 50]),
    %% Defaults to 'false' when config is invalid
    http_get(Config, "/overview", ?OK).

auth_attempts_test(Config) ->
    rpc(Config, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    {Conn, _Ch} = open_connection_and_channel(Config),
    close_connection(Conn),
    [NodeData] = http_get(Config, "/nodes"),
    Node = binary_to_atom(maps:get(name, NodeData), utf8),
    Map = http_get(Config, "/auth/attempts/" ++ atom_to_list(Node), ?OK),
    Http = get_auth_attempts(<<"http">>, Map),
    Amqp091 = get_auth_attempts(<<"amqp091">>, Map),
    ?assertEqual(false, maps:is_key(remote_address, Amqp091)),
    ?assertEqual(false, maps:is_key(username, Amqp091)),
    ?assertEqual(1, maps:get(auth_attempts, Amqp091)),
    ?assertEqual(1, maps:get(auth_attempts_succeeded, Amqp091)),
    ?assertEqual(0, maps:get(auth_attempts_failed, Amqp091)),
    ?assertEqual(false, maps:is_key(remote_address, Http)),
    ?assertEqual(false, maps:is_key(username, Http)),
    ?assertEqual(2, maps:get(auth_attempts, Http)),
    ?assertEqual(2, maps:get(auth_attempts_succeeded, Http)),
    ?assertEqual(0, maps:get(auth_attempts_failed, Http)),

    rpc(Config, application, set_env, [rabbit, track_auth_attempt_source, true]),
    {Conn2, _Ch2} = open_connection_and_channel(Config),
    close_connection(Conn2),
    Map2 = http_get(Config, "/auth/attempts/" ++ atom_to_list(Node) ++ "/source", ?OK),
    Map3 = http_get(Config, "/auth/attempts/" ++ atom_to_list(Node), ?OK),
    Http2 = get_auth_attempts(<<"http">>, Map2),
    Http3 = get_auth_attempts(<<"http">>, Map3),
    Amqp091_2 = get_auth_attempts(<<"amqp091">>, Map2),
    Amqp091_3 = get_auth_attempts(<<"amqp091">>, Map3),
    ?assertEqual(<<"127.0.0.1">>, maps:get(remote_address, Http2)),
    ?assertEqual(<<"guest">>, maps:get(username, Http2)),
    ?assertEqual(1, maps:get(auth_attempts, Http2)),
    ?assertEqual(1, maps:get(auth_attempts_succeeded, Http2)),
    ?assertEqual(0, maps:get(auth_attempts_failed, Http2)),

    ?assertEqual(false, maps:is_key(remote_address, Http3)),
    ?assertEqual(false, maps:is_key(username, Http3)),
    ?assertEqual(4, maps:get(auth_attempts, Http3)),
    ?assertEqual(4, maps:get(auth_attempts_succeeded, Http3)),
    ?assertEqual(0, maps:get(auth_attempts_failed, Http3)),

    ?assertEqual(true, <<>> =/= maps:get(remote_address, Amqp091_2)),
    ?assertEqual(<<"guest">>, maps:get(username, Amqp091_2)),
    ?assertEqual(1, maps:get(auth_attempts, Amqp091_2)),
    ?assertEqual(1, maps:get(auth_attempts_succeeded, Amqp091_2)),
    ?assertEqual(0, maps:get(auth_attempts_failed, Amqp091_2)),

    ?assertEqual(false, maps:is_key(remote_address, Amqp091_3)),
    ?assertEqual(false, maps:is_key(username, Amqp091_3)),
    ?assertEqual(2, maps:get(auth_attempts, Amqp091_3)),
    ?assertEqual(2, maps:get(auth_attempts_succeeded, Amqp091_3)),
    ?assertEqual(0, maps:get(auth_attempts_failed, Amqp091_3)),

    passed.


config_environment_test(Config) ->
    rpc(Config, application, set_env, [rabbitmq_management,
                                       config_environment_test_env,
                                       config_environment_test_value]),
    ResultString = http_get_no_decode(Config, "/config/effective",
                                      "guest", "guest", ?OK),
    CleanString = re:replace(ResultString, "\\s+", "", [global,{return,list}]),
    {ok, Tokens, _} = erl_scan:string(CleanString++"."),
    {ok, AbsForm} = erl_parse:parse_exprs(Tokens),
    {value, EnvList, _} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    V = proplists:get_value(config_environment_test_env,
                            proplists:get_value(rabbitmq_management, EnvList)),
    ?assertEqual(config_environment_test_value, V).


disabled_qq_replica_opers_test(Config) ->
    Nodename = rabbit_data_coercion:to_list(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    Body = [{node, Nodename}],
    http_post(Config, "/queues/quorum/%2F/qq.whatever/replicas/add", Body, ?METHOD_NOT_ALLOWED),
    http_delete(Config, "/queues/quorum/%2F/qq.whatever/replicas/delete", ?METHOD_NOT_ALLOWED, Body),
    http_post(Config, "/queues/quorum/replicas/on/" ++ Nodename ++ "/grow", Body, ?METHOD_NOT_ALLOWED),
    http_delete(Config, "/queues/quorum/replicas/on/" ++ Nodename ++ "/shrink", ?METHOD_NOT_ALLOWED),
    passed.

qq_status_test(Config) ->
    QQArgs = [{durable, true}, {arguments, [{'x-queue-type', 'quorum'}]}],
    http_get(Config, "/queues/%2f/qq_status", ?NOT_FOUND),
    http_put(Config, "/queues/%2f/qq_status", QQArgs, {group, '2xx'}),
    [MapRes] = http_get(Config, "/queues/quorum/%2f/qq_status/status", ?OK),
    Keys = ['Commit Index','Last Applied','Last Log Index',
            'Last Written','Machine Version','Membership','Node Name',
            'Raft State','Snapshot Index','Term'],
    ?assertEqual(lists:sort(Keys), lists:sort(maps:keys(MapRes))),
    http_delete(Config, "/queues/%2f/qq_status", {group, '2xx'}),


    CQArgs = [{durable, true}],
    http_get(Config, "/queues/%2F/cq_status", ?NOT_FOUND),
    http_put(Config, "/queues/%2F/cq_status", CQArgs, {group, '2xx'}),
    ResBody = http_get_no_decode(Config, "/queues/quorum/%2f/cq_status/status", "guest", "guest", 503),
    ?assertEqual(#{reason => <<"classic_queue_not_supported">>,
                   status => <<"failed">>}, decode_body(ResBody)),
    http_delete(Config, "/queues/%2f/cq_status", {group, '2xx'}),
    passed.


list_deprecated_features_test(Config) ->
    Desc = "This is a deprecated feature",
    DocUrl = "https://rabbitmq.com/",
    FeatureFlags = #{?FUNCTION_NAME =>
                         #{provided_by => ?MODULE,
                           deprecation_phase => permitted_by_default,
                           desc => Desc,
                           doc_url => DocUrl}},
    ok = rpc(Config, rabbit_feature_flags, inject_test_feature_flags, [FeatureFlags]),
    Result = http_get(Config, "/deprecated-features", ?OK),
    Features = lists:filter(fun(Map) ->
                                    maps:get(name, Map) == atom_to_binary(?FUNCTION_NAME)
                            end, Result),
    ?assertMatch([_], Features),
    [Feature] = Features,
    ?assertEqual(<<"permitted_by_default">>, maps:get(deprecation_phase, Feature)),
    ?assertEqual(atom_to_binary(?MODULE), maps:get(provided_by, Feature)),
    ?assertEqual(list_to_binary(Desc), maps:get(desc, Feature)),
    ?assertEqual(list_to_binary(DocUrl), maps:get(doc_url, Feature)).

list_used_deprecated_features_test(Config) ->
    Desc = "This is a deprecated feature in use",
    DocUrl = "https://rabbitmq.com/",
    FeatureFlags = #{?FUNCTION_NAME =>
                         #{provided_by => ?MODULE,
                           deprecation_phase => removed,
                           desc => Desc,
                           doc_url => DocUrl,
                           callbacks => #{is_feature_used => {rabbit_mgmt_wm_deprecated_features, feature_is_used}}}},
    ok = rpc(Config, rabbit_feature_flags, inject_test_feature_flags, [FeatureFlags]),
    Result = http_get(Config, "/deprecated-features/used", ?OK),
    Features = lists:filter(fun(Map) ->
                                    maps:get(name, Map) == atom_to_binary(?FUNCTION_NAME)
                            end, Result),
    ?assertMatch([_], Features),
    [Feature] = Features,
    ?assertEqual(<<"removed">>, maps:get(deprecation_phase, Feature)),
    ?assertEqual(atom_to_binary(?MODULE), maps:get(provided_by, Feature)),
    ?assertEqual(list_to_binary(Desc), maps:get(desc, Feature)),
    ?assertEqual(list_to_binary(DocUrl), maps:get(doc_url, Feature)).

cluster_and_node_tags_test(Config) ->
    Overview = http_get(Config, "/overview"),
    ClusterTags = maps:get(cluster_tags, Overview),
    NodeTags = maps:get(node_tags, Overview),
    ExpectedTags = #{az => <<"us-east-3">>,environment => <<"production">>,
                     region => <<"us-east">>},
    ?assertEqual(ExpectedTags, ClusterTags),
    ?assertEqual(ExpectedTags, NodeTags),
    passed.

default_queue_type_fallback_in_overview_test(Config) ->
    Overview = http_get(Config, "/overview"),
    DQT = maps:get(default_queue_type, Overview),
    ?assertEqual(<<"classic">>, DQT).

default_queue_type_with_value_configured_in_overview_test(Config) ->
    Overview = with_default_queue_type(Config, rabbit_quorum_queue,
        fun(Cfg) ->
            http_get(Cfg, "/overview")
        end),

    DQT = maps:get(default_queue_type, Overview),
    ?assertEqual(<<"quorum">>, DQT).

default_queue_types_in_vhost_list_test(Config) ->
    TestName = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    VHost1 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 1])),
    VHost2 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 2])),
    VHost3 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 3])),
    VHost4 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 4])),

    VHosts = #{
        VHost1 => undefined,
        VHost2 => <<"classic">>,
        VHost3 => <<"quorum">>,
        VHost4 => <<"stream">>
    },

    try
        begin
            lists:foreach(
                fun({V, QT}) ->
                    rabbit_ct_broker_helpers:add_vhost(Config, V),
                    rabbit_ct_broker_helpers:set_full_permissions(Config, V),
                    rabbit_ct_broker_helpers:update_vhost_metadata(Config, V, #{
                        default_queue_type => QT
                    })
                end, maps:to_list(VHosts)),

            List = http_get(Config, "/vhosts"),
            ct:pal("GET /api/vhosts returned: ~tp", [List]),
            VH1 = find_map_by_name(VHost1, List),
            ?assertEqual(<<"classic">>, maps:get(default_queue_type, VH1)),

            VH2 = find_map_by_name(VHost2, List),
            ?assertEqual(<<"classic">>, maps:get(default_queue_type, VH2)),

            VH3 = find_map_by_name(VHost3, List),
            ?assertEqual(<<"quorum">>, maps:get(default_queue_type, VH3)),

            VH4 = find_map_by_name(VHost4, List),
            ?assertEqual(<<"stream">>, maps:get(default_queue_type, VH4))
        end
    after
        lists:foreach(
            fun(V) ->
                rabbit_ct_broker_helpers:delete_vhost(Config, V)
            end, maps:keys(VHosts))
    end.

default_queue_type_of_one_vhost_test(Config) ->
    TestName = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    VHost1 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 1])),
    VHost2 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 2])),
    VHost3 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 3])),
    VHost4 = rabbit_data_coercion:to_binary(io_lib:format("~ts-~b", [TestName, 4])),

    VHosts = #{
        VHost1 => undefined,
        VHost2 => <<"classic">>,
        VHost3 => <<"quorum">>,
        VHost4 => <<"stream">>
    },

    try
        begin
            lists:foreach(
                fun({V, QT}) ->
                    rabbit_ct_broker_helpers:add_vhost(Config, V),
                    rabbit_ct_broker_helpers:set_full_permissions(Config, V),
                    rabbit_ct_broker_helpers:update_vhost_metadata(Config, V, #{
                        default_queue_type => QT
                    })
                end, maps:to_list(VHosts)),

            VH1 = http_get(Config, io_lib:format("/vhosts/~ts", [VHost1])),
            ?assertEqual(<<"classic">>, maps:get(default_queue_type, VH1)),

            VH2 = http_get(Config, io_lib:format("/vhosts/~ts", [VHost2])),
            ?assertEqual(<<"classic">>, maps:get(default_queue_type, VH2)),

            VH3 = http_get(Config, io_lib:format("/vhosts/~ts", [VHost3])),
            ?assertEqual(<<"quorum">>, maps:get(default_queue_type, VH3)),

            VH4 = http_get(Config, io_lib:format("/vhosts/~ts", [VHost4])),
            ?assertEqual(<<"stream">>, maps:get(default_queue_type, VH4))
        end
    after
        lists:foreach(
            fun(V) ->
                rabbit_ct_broker_helpers:delete_vhost(Config, V)
            end, maps:keys(VHosts))
    end.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

%% Finds a map by its <<"name">> key in an HTTP API response.
-spec find_map_by_name(binary(), [#{binary() => any()}]) -> #{binary() => any()} | undefined.
find_map_by_name(NameToFind, List) ->
    case lists:filter(fun(#{name := Name}) ->
                        Name =:= NameToFind
                      end, List) of
        []    -> undefined;
        [Val] -> Val
    end.

msg(Key, Headers, Body) ->
    msg(Key, Headers, Body, <<"string">>).

msg(Key, Headers, Body, Enc) ->
    #{exchange         => <<"">>,
      routing_key      => Key,
      properties       => #{delivery_mode => 2,
                           headers        =>  Headers},
      payload          => Body,
      payload_encoding => Enc}.

local_port(Conn) ->
    [{sock, Sock}] = amqp_connection:info(Conn, [sock]),
    {ok, Port} = inet:port(Sock),
    Port.

spawn_invalid(_Config, 0) ->
    ok;
spawn_invalid(Config, N) ->
    Self = self(),
    spawn(fun() ->
                  timer:sleep(rand:uniform(250)),
                  {ok, Sock} = gen_tcp:connect("localhost", amqp_port(Config), [list]),
                  ok = gen_tcp:send(Sock, "Some Data"),
                  receive_msg(Self)
          end),
    spawn_invalid(Config, N-1).

receive_msg(Self) ->
    receive
        {tcp, _, [$A, $M, $Q, $P | _]} ->
            Self ! done
    after
        60000 ->
            Self ! no_reply
    end.

wait_for_answers(0) ->
    ok;
wait_for_answers(N) ->
    receive
        done ->
            wait_for_answers(N-1);
        no_reply ->
            throw(no_reply)
    end.

publish(Ch) ->
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = <<"myqueue">>},
                      #amqp_msg{payload = <<"message">>}),
    receive
        stop_publish ->
            ok
    after 20 ->
        publish(Ch)
    end.

%% @doc encode fields and file for HTTP post multipart/form-data.
%% @reference Inspired by <a href="http://code.activestate.com/recipes/146306/">Python implementation</a>.
format_multipart_filedata(Boundary, Files) ->
    FileParts = lists:map(fun({FieldName, FileName, FileContent}) ->
                                  [lists:concat(["--", Boundary]),
                                   lists:concat(["content-disposition: form-data; name=\"", atom_to_list(FieldName), "\"; filename=\"", FileName, "\""]),
                                   lists:concat(["content-type: ", "application/octet-stream"]),
                                   "",
                                   FileContent]
                          end, Files),
    FileParts2 = lists:append(FileParts),
    EndingParts = [lists:concat(["--", Boundary, "--"]), ""],
    Parts = lists:append([FileParts2, EndingParts]),
    string:join(Parts, "\r\n").

get_auth_attempts(Protocol, Map) ->
    [A] = lists:filter(fun(#{protocol := P}) ->
                               P == Protocol
                       end, Map),
    A.

await_condition(Fun) ->
    rabbit_ct_helpers:await_condition(
      fun () ->
              try
                  Fun()
              catch _:_ ->
                        false
              end
      end, ?COLLECT_INTERVAL * 100).


clear_default_queue_type(Config) ->
    rpc(Config, application, unset_env, [rabbit, default_queue_type]).

with_default_queue_type(Config, DQT, Fun) ->
    rpc(Config, application, set_env, [rabbit, default_queue_type, DQT]),
    ToReturn = Fun(Config),
    rpc(Config, application, unset_env, [rabbit, default_queue_type]),
    ToReturn.
