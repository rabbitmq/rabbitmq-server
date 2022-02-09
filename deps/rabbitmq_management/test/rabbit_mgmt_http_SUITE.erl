%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_http_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/1]).
-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2,
                                assert_keys/2, assert_no_keys/2,
                                http_get/2, http_get/3, http_get/5,
                                http_get_no_auth/3,
                                http_get_as_proplist/2,
                                http_put/4, http_put/6,
                                http_post/4, http_post/6,
                                http_upload_raw/8,
                                http_delete/3, http_delete/5,
                                http_put_raw/4, http_post_accept_json/4,
                                req/4, auth_header/2,
                                assert_permanent_redirect/3,
                                uri_base_from/2, format_for_upload/1,
                                amqp_port/1, req/6]).

-import(rabbit_misc, [pget/2]).

-define(COLLECT_INTERVAL, 1000).
-define(PATH_PREFIX, "/custom-prefix").

-compile(export_all).

all() ->
    [
     {group, all_tests_with_prefix},
     {group, all_tests_without_prefix},
     {group, user_limits_ff}
    ].

groups() ->
    [
     {all_tests_with_prefix, [], all_tests()},
     {all_tests_without_prefix, [], all_tests()},
     {user_limits_ff, [], [
        user_limits_list_test,
        user_limit_set_test
     ]}
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
    users_test,
    users_legacy_administrator_test,
    adding_a_user_with_password_test,
    adding_a_user_with_password_hash_test,
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
    connections_test,
    multiple_invalid_connections_test,
    exchanges_test,
    queues_test,
    quorum_queues_test,
    queues_well_formed_json_test,
    bindings_test,
    bindings_post_test,
    bindings_null_routing_key_test,
    bindings_e2e_test,
    permissions_administrator_test,
    permissions_vhost_test,
    permissions_amqp_test,
    permissions_connection_channel_consumer_test,
    consumers_cq_test,
    consumers_qq_test,
    definitions_test,
    definitions_vhost_test,
    definitions_password_test,
    definitions_remove_things_test,
    definitions_server_named_queue_test,
    definitions_with_charset_test,
    long_definitions_test,
    long_definitions_multipart_test,
    aliveness_test,
    arguments_test,
    arguments_table_test,
    queue_purge_test,
    queue_actions_test,
    exclusive_consumer_test,
    exclusive_queue_test,
    connections_channels_pagination_test,
    exchanges_pagination_test,
    exchanges_pagination_permissions_test,
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
    publish_accept_json_test,
    publish_fail_test,
    publish_base64_test,
    publish_unrouted_test,
    if_empty_unused_test,
    parameters_test,
    global_parameters_test,
    policy_test,
    policy_permissions_test,
    issue67_test,
    extensions_test,
    cors_test,
    vhost_limits_list_test,
    vhost_limit_set_test,
    rates_test,
    single_active_consumer_cq_test,
    single_active_consumer_qq_test,
    oauth_test,
    disable_basic_auth_test,
    login_test,
    csp_headers_test,
    auth_attempts_test
].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
merge_app_env(Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics_interval, ?COLLECT_INTERVAL}
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
    rabbit_ct_helpers:run_setup_steps(Config, Steps).

finish_init(Group, Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    NodeConf = [{rmq_nodename_suffix, Group}],
    Config1 = rabbit_ct_helpers:set_config(Config, NodeConf),
    merge_app_env(Config1).

enable_feature_flag_or_skip(FFName, Group, Config0) ->
    Config1 = finish_init(Group, Config0),
    Config2 = start_broker(Config1),
    Nodes = rabbit_ct_broker_helpers:get_node_configs(
              Config2, nodename),
    Ret = rabbit_ct_broker_helpers:rpc(
            Config2, 0,
            rabbit_feature_flags,
            is_supported_remotely,
            [Nodes, [FFName], 60000]),
    case Ret of
        true ->
            ok = rabbit_ct_broker_helpers:rpc(
                    Config2, 0, rabbit_feature_flags, enable, [FFName]),
            Config2;
        false ->
            end_per_group(Group, Config2),
            {skip, rabbit_misc:format("Feature flag '~s' is not supported", [FFName])}
    end.

init_per_group(all_tests_with_prefix=Group, Config0) ->
    PathConfig = {rabbitmq_management, [{path_prefix, ?PATH_PREFIX}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    Config2 = finish_init(Group, Config1),
    Config3 = start_broker(Config2),
    Nodes = rabbit_ct_broker_helpers:get_node_configs(
              Config3, nodename),
    Ret = rabbit_ct_broker_helpers:rpc(
            Config3, 0,
            rabbit_feature_flags,
            is_supported_remotely,
            [Nodes, [quorum_queue], 60000]),
    case Ret of
        true ->
            ok = rabbit_ct_broker_helpers:rpc(
                    Config3, 0, rabbit_feature_flags, enable, [quorum_queue]),
            Config3;
        false ->
            end_per_group(Group, Config3),
            {skip, "Quorum queues are unsupported"}
    end;
init_per_group(user_limits_ff = Group, Config0) ->
    enable_feature_flag_or_skip(user_limits, Group, Config0);
init_per_group(Group, Config0) ->
    enable_feature_flag_or_skip(quorum_queue, Group, Config0).

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

init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"rabbit_mgmt_SUITE:init_per_testcase">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"rabbit_mgmt_SUITE:end_per_testcase">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, disable_basic_auth, false]),
    Config1 = end_per_testcase0(Testcase, Config),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

end_per_testcase0(T, Config)
  when T =:= long_definitions_test; T =:= long_definitions_multipart_test ->
    Vhosts = long_definitions_vhosts(T),
    [rabbit_ct_broker_helpers:delete_vhost(Config, Name)
     || #{name := Name} <- Vhosts],
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
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_user_1_user">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"limit_test_vhost_1_user">>),
    Config;
end_per_testcase0(permissions_vhost_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost1">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost2">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"myuser1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"myuser2">>),
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
            connection_other, queue_procs, queue_slave_procs, plugins,
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
    NonMgmtKeys = [rabbit_vhost,rabbit_user_permission],
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

auth_test(Config) ->
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"">>}], {group, '2xx'}),
    EmptyAuthResponseHeaders = test_auth(Config, ?NOT_AUTHORISED, []),
    ?assertEqual(true, lists:keymember("www-authenticate", 1,  EmptyAuthResponseHeaders)),
    %% NOTE: this one won't have www-authenticate in the response,
    %% because user/password are ok, tags are not
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("user", "user")]),
    WrongAuthResponseHeaders = test_auth(Config, ?NOT_AUTHORISED, [auth_header("guest", "gust")]),
    ?assertEqual(true, lists:keymember("www-authenticate", 1,  WrongAuthResponseHeaders)),
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
    Ret = rabbit_ct_broker_helpers:enable_feature_flag(
            Config, virtual_host_metadata),

    http_put(Config, "/vhosts/myvhost", [{description, <<"vhost description">>},
                                         {tags, <<"tag1,tag2">>}], {group, '2xx'}),
    Expected = case Ret of
                   {skip, _} ->
                       #{name => <<"myvhost">>};
                   _ ->
                       #{name => <<"myvhost">>,
                         metadata => #{
                           description => <<"vhost description">>,
                           tags => [<<"tag1">>, <<"tag2">>]
                          }}
               end,
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
    http_get(Config, "/users/myuser", ?NOT_FOUND),
    http_put_raw(Config, "/users/myuser", "Something not JSON", ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{flim, <<"flam">>}], ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{tags,     [<<"management">>]},
                                       {password, <<"myuser">>}],
             {group, '2xx'}),
    http_put(Config, "/users/myuser", [{password_hash, <<"not_hash">>}], ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    assert_item(#{name => <<"myuser">>, tags => [<<"management">>],
                  password_hash => <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>,
                  hashing_algorithm => <<"rabbit_password_hashing_sha256">>},
                http_get(Config, "/users/myuser")),

    http_put(Config, "/users/myuser", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                       {hashing_algorithm, <<"rabbit_password_hashing_md5">>},
                                       {tags, [<<"management">>]}], {group, '2xx'}),
    assert_item(#{name => <<"myuser">>, tags => [<<"management">>],
                  password_hash => <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>,
                  hashing_algorithm => <<"rabbit_password_hashing_md5">>},
                http_get(Config, "/users/myuser")),
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                                       {tags, [<<"administrator">>, <<"foo">>]}], {group, '2xx'}),
    assert_item(#{name => <<"myuser">>, tags => [<<"administrator">>, <<"foo">>]},
                http_get(Config, "/users/myuser")),
    assert_list(lists:sort([#{name => <<"myuser">>, tags => [<<"administrator">>, <<"foo">>]},
                 #{name => <<"guest">>, tags => [<<"administrator">>]}]),
                lists:sort(http_get(Config, "/users"))),
    test_auth(Config, ?OK, [auth_header("myuser", "password")]),
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                     {tags, []}], {group, '2xx'}),
    assert_item(#{name => <<"myuser">>, tags => []},
                http_get(Config, "/users/myuser")),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("myuser", "password")]),
    http_get(Config, "/users/myuser", ?NOT_FOUND),
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

connections_test(Config) ->
    {Conn, _Ch} = open_connection_and_channel(Config),
    LocalPort = local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w",
               [LocalPort, amqp_port(Config)])),
    timer:sleep(1500),
    Connection = http_get(Config, Path, ?OK),
    ?assert(maps:is_key(recv_oct, Connection)),
    ?assert(maps:is_key(garbage_collection, Connection)),
    ?assert(maps:is_key(send_oct_details, Connection)),
    ?assert(maps:is_key(reductions, Connection)),
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
    wait_until(Fun, 60),
    close_connection(Conn),
    passed.

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
                   arguments   => #{}},
                 #{name        => <<"foo">>,
                   vhost       => <<"downvhost">>,
                   state       => <<"stopped">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{}}], DownQueues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"downvhost">>,
                  state       => <<"stopped">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{}}, DownQueue),

    http_put(Config, "/queues/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/queues/%2F/bar",
             [{durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/queues/%2F/foo",
             [{durable, false}],
             ?BAD_REQUEST),

    http_put(Config, "/queues/%2F/baz", Good, {group, '2xx'}),
    Queues = http_get(Config, "/queues/%2F"),
    Queue = http_get(Config, "/queues/%2F/foo"),
    assert_list([#{name        => <<"baz">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{}},
                 #{name        => <<"foo">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{}}], Queues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"/">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{}}, Queue),

    http_delete(Config, "/queues/%2F/foo", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/baz", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/foo", ?NOT_FOUND),
    http_get(Config, "/queues/badvhost", ?NOT_FOUND),

    http_delete(Config, "/queues/downvhost/foo", {group, '2xx'}),
    http_delete(Config, "/queues/downvhost/bar", {group, '2xx'}),
    passed.

quorum_queues_test(Config) ->
    %% Test in a loop that no metrics are left behing after deleting a queue
    quorum_queues_test_loop(Config, 5).

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
    wait_until(fun() ->
                       Num = maps:get(messages, http_get(Config, "/queues/%2f/qq?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5"), undefined),
                       ct:pal("wait_until got ~w", [N]),
                       2 == Num
               end, 100),

    http_delete(Config, "/queues/%2f/qq", {group, '2xx'}),
    http_put(Config, "/queues/%2f/qq", Good, {group, '2xx'}),

    wait_until(fun() ->
                       0 == maps:get(messages, http_get(Config, "/queues/%2f/qq?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5"), undefined)
               end, 100),

    http_delete(Config, "/queues/%2f/qq", {group, '2xx'}),
    close_connection(Conn),
    quorum_queues_test_loop(Config, N-1).

queues_well_formed_json_test(Config) ->
    %% TODO This test should be extended to the whole API
    Good = [{durable, true}],
    http_put(Config, "/queues/%2F/foo", Good, {group, '2xx'}),
    http_put(Config, "/queues/%2F/baz", Good, {group, '2xx'}),

    Queues = http_get_as_proplist(Config, "/queues/%2F"),
    %% Ensure keys are unique
    [begin
         Q = rabbit_data_coercion:to_proplist(Q0),
         ?assertEqual(lists:sort(Q), lists:usort(Q))
     end || Q0 <- Queues],

    http_delete(Config, "/queues/%2F/foo", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/baz", {group, '2xx'}),
    passed.

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
    timer:sleep(1500),
    AssertLength = fun (Path, User, Len) ->
                           Res = http_get(Config, Path, User, User, ?OK),
                           ?assertEqual(Len, length(Res))
                   end,
    [begin
         AssertLength(P, "user", 1),
         AssertLength(P, "monitor", 3),
         AssertLength(P, "guest", 3)
     end || P <- ["/connections", "/channels", "/consumers", "/consumers/%2F"]],

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
    timer:sleep(1500),
    assert_list([#{exclusive       => false,
                   ack_required    => true,
                   active          => true,
                   activity_status => <<"up">>,
                   consumer_tag    => <<"my-ctag">>}], http_get(Config, "/consumers")),
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
    timer:sleep(1500),
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
    amqp_channel:close(Ch),
    timer:sleep(1500),
    assert_list([#{exclusive       => false,
                   ack_required    => false,
                   active          => true,
                   activity_status => <<"single_active">>,
                   consumer_tag    => <<"2">>}], http_get(Config, "/consumers")),
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
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, register, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, register_policy_validator, []).

unregister_parameters_and_policy_validator(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, unregister_policy_validator, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, unregister, []).

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

defs_vhost(Config, Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,
    ReplaceVHostInArgs = fun(M, V2) -> maps:map(fun(vhost, _) -> V2;
        (_, V1)    -> V1 end, M) end,

    %% Create test vhost
    http_put(Config, "/vhosts/test", none, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/test/guest", PermArgs, {group, '2xx'}),

    %% Test against default vhost
    defs_vhost(Config, Key, URI, Rep1, "%2F", "test", CreateMethod,
               ReplaceVHostInArgs(Args, <<"/">>), ReplaceVHostInArgs(Args, <<"test">>),
               fun(URI2) -> http_delete(Config, URI2, {group, '2xx'}) end),

    %% Test against test vhost
    defs_vhost(Config, Key, URI, Rep1, "test", "%2F", CreateMethod,
               ReplaceVHostInArgs(Args, <<"test">>), ReplaceVHostInArgs(Args, <<"/">>),
               fun(URI2) -> http_delete(Config, URI2, {group, '2xx'}) end),

    %% Remove test vhost
    http_delete(Config, "/vhosts/test", {group, '2xx'}).


defs_vhost(Config, Key, URI0, Rep1, VHost1, VHost2, CreateMethod, Args1, Args2,
           DeleteFun) ->
    %% Create the item
    URI2 = create(Config, CreateMethod, Rep1(URI0, VHost1), Args1),
    %% Make sure it ends up in definitions
    Definitions = http_get(Config, "/definitions/" ++ VHost1, ?OK),
    true = lists:any(fun(I) -> test_item(Args1, I) end, maps:get(Key, Definitions)),

    %% Make sure it is not in the other vhost
    Definitions0 = http_get(Config, "/definitions/" ++ VHost2, ?OK),
    false = lists:any(fun(I) -> test_item(Args2, I) end, maps:get(Key, Definitions0)),

    %% Post the definitions back
    http_post(Config, "/definitions/" ++ VHost2, Definitions, {group, '2xx'}),

    %% Make sure it is now in the other vhost
    Definitions1 = http_get(Config, "/definitions/" ++ VHost2, ?OK),
    true = lists:any(fun(I) -> test_item(Args2, I) end, maps:get(Key, Definitions1)),

    %% Delete it
    DeleteFun(URI2),
    URI3 = create(Config, CreateMethod, Rep1(URI0, VHost2), Args2),
    DeleteFun(URI3),
    passed.

definitions_vhost_test(Config) ->
    %% Ensures that definitions can be exported/imported from a single virtual
    %% host to another

    register_parameters_and_policy_validator(Config),

    defs_vhost(Config, queues, "/queues/<vhost>/my-queue", put,
               #{name    => <<"my-queue">>,
                 durable => true}),
    defs_vhost(Config, exchanges, "/exchanges/<vhost>/my-exchange", put,
               #{name => <<"my-exchange">>,
                 type => <<"direct">>}),
    defs_vhost(Config, bindings, "/bindings/<vhost>/e/amq.direct/e/amq.fanout", post,
               #{routing_key => <<"routing">>, arguments => #{}}),
    defs_vhost(Config, policies, "/policies/<vhost>/my-policy", put,
               #{vhost      => vhost,
                 name       => <<"my-policy">>,
                 pattern    => <<".*">>,
                 definition => #{testpos => [1, 2, 3]},
                 priority   => 1}),

    defs_vhost(Config, parameters, "/parameters/vhost-limits/<vhost>/limits", put,
               #{vhost      => vhost,
                 name       => <<"limits">>,
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
    ct:pal("Definitions35: ~p", [Definitions35]),
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
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbit,
                                                                   password_hashing_module,
                                                                   rabbit_password_hashing_sha512]),

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

aliveness_test(Config) ->
    #{status := <<"ok">>} = http_get(Config, "/aliveness-test/%2F", ?OK),
    http_get(Config, "/aliveness-test/foo", ?NOT_FOUND),
    http_delete(Config, "/queues/%2F/aliveness-test", {group, '2xx'}),
    passed.

arguments_test(Config) ->
    XArgs = [{type, <<"headers">>},
             {arguments, [{'alternate-exchange', <<"amq.direct">>}]}],
    QArgs = [{arguments, [{'x-expires', 1800000}]}],
    BArgs = [{routing_key, <<"">>},
             {arguments, [{'x-match', <<"all">>},
                          {foo, <<"bar">>}]}],
    http_delete(Config, "/exchanges/%2F/myexchange", {one_of, [201, 404]}),
    http_put(Config, "/exchanges/%2F/myexchange", XArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/arguments_test", QArgs, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/myexchange/q/arguments_test", BArgs, {group, '2xx'}),
    Definitions = http_get(Config, "/definitions", ?OK),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/arguments_test", {group, '2xx'}),
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),
    #{'alternate-exchange' := <<"amq.direct">>} =
        maps:get(arguments, http_get(Config, "/exchanges/%2F/myexchange", ?OK)),
    #{'x-expires' := 1800000} =
        maps:get(arguments, http_get(Config, "/queues/%2F/arguments_test", ?OK)),

    ArgsTable = [{<<"foo">>,longstr,<<"bar">>}, {<<"x-match">>, longstr, <<"all">>}],
    Hash = table_hash(ArgsTable),
    PropertiesKey = [$~] ++ Hash,

    assert_item(
        #{'x-match' => <<"all">>, foo => <<"bar">>},
        maps:get(arguments,
            http_get(Config, "/bindings/%2F/e/myexchange/q/arguments_test/" ++
            PropertiesKey, ?OK))
    ),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/arguments_test", {group, '2xx'}),
    passed.

table_hash(Table) ->
    binary_to_list(rabbit_mgmt_format:args_hash(Table)).

arguments_table_test(Config) ->
    Args = #{'upstreams' => [<<"amqp://localhost/%2F/upstream1">>,
                             <<"amqp://localhost/%2F/upstream2">>]},
    XArgs = #{type      => <<"headers">>,
              arguments => Args},
    http_delete(Config, "/exchanges/%2F/myexchange", {one_of, [201, 404]}),
    http_put(Config, "/exchanges/%2F/myexchange", XArgs, {group, '2xx'}),
    Definitions = http_get(Config, "/definitions", ?OK),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),
    Args = maps:get(arguments, http_get(Config, "/exchanges/%2F/myexchange", ?OK)),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
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
    http_post(Config, "/queues/%2F/q/actions", [{action, sync}], {group, '2xx'}),
    http_post(Config, "/queues/%2F/q/actions", [{action, cancel_sync}], {group, '2xx'}),
    http_post(Config, "/queues/%2F/q/actions", [{action, change_colour}], ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/q", {group, '2xx'}),
    passed.

exclusive_consumer_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue     = QName,
                                                exclusive = true}, self()),
    timer:sleep(1500), %% Sadly we need to sleep to let the stats update
    http_get(Config, "/queues/%2F/"), %% Just check we don't blow up
    close_channel(Ch),
    close_connection(Conn),
    passed.


exclusive_queue_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
    amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    timer:sleep(1500), %% Sadly we need to sleep to let the stats update
    Path = "/queues/%2F/" ++ rabbit_http_util:quote_plus(QName),
    Queue = http_get(Config, Path),
    assert_item(#{name        => QName,
                  vhost       => <<"/">>,
                  durable     => false,
                  auto_delete => false,
                  exclusive   => true,
                  arguments   => #{}}, Queue),
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

    %% for stats to update
    timer:sleep(1500),
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

    %% for stats to update
    timer:sleep(1500),

    Total     = length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list_names, [])),

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

    %% for stats to update
    timer:sleep(1500),

    FirstPage = http_get(Config, "/exchanges?page=1&name=test1", "non-admin", "non-admin", ?OK),

    ?assertEqual(8, maps:get(total_count, FirstPage)),
    ?assertEqual(1, maps:get(item_count, FirstPage)),
    ?assertEqual(1, maps:get(page, FirstPage)),
    ?assertEqual(100, maps:get(page_size, FirstPage)),
    ?assertEqual(1, maps:get(page_count, FirstPage)),
    assert_list([#{name => <<"test1">>, vhost => <<"vh1">>}
                ], maps:get(items, FirstPage)),
    http_delete(Config, "/exchanges/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/users/admin", {group, '2xx'}),
    http_delete(Config, "/users/non-admin", {group, '2xx'}),
    passed.



queue_pagination_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/guest", PermArgs, {group, '2xx'}),

    http_get(Config, "/queues/vh1?page=1&page_size=2", ?OK),

    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test2_reg", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/reg_test3", QArgs, {group, '2xx'}),

    %% for stats to update
    timer:sleep(1500),

    Total     = length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list_names, [])),

    PageOfTwo = http_get(Config, "/queues?page=1&page_size=2", ?OK),
    ?assertEqual(Total, maps:get(total_count, PageOfTwo)),
    ?assertEqual(Total, maps:get(filtered_count, PageOfTwo)),
    ?assertEqual(2, maps:get(item_count, PageOfTwo)),
    ?assertEqual(1, maps:get(page, PageOfTwo)),
    ?assertEqual(2, maps:get(page_size, PageOfTwo)),
    ?assertEqual(2, maps:get(page_count, PageOfTwo)),
    assert_list([#{name => <<"test0">>, vhost => <<"/">>},
                 #{name => <<"test2_reg">>, vhost => <<"/">>}
                ], maps:get(items, PageOfTwo)),

    SortedByName = http_get(Config, "/queues?sort=name&page=1&page_size=2", ?OK),
    ?assertEqual(Total, maps:get(total_count, SortedByName)),
    ?assertEqual(Total, maps:get(filtered_count, SortedByName)),
    ?assertEqual(2, maps:get(item_count, SortedByName)),
    ?assertEqual(1, maps:get(page, SortedByName)),
    ?assertEqual(2, maps:get(page_size, SortedByName)),
    ?assertEqual(2, maps:get(page_count, SortedByName)),
    assert_list([#{name => <<"reg_test3">>, vhost => <<"vh1">>},
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
                 #{name => <<"test1">>, vhost => <<"vh1">>},
                 #{name => <<"test2_reg">>, vhost => <<"/">>},
                 #{name => <<"reg_test3">>, vhost =><<"vh1">>}
                ], maps:get(items, FirstPage)),


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
                 #{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], maps:get(items, ReverseSortedByName)),


    ByName = http_get(Config, "/queues?page=1&page_size=2&name=reg", ?OK),
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
                           "/queues?page=1&page_size=2&name=%5E(?=%5Ereg)&use_regex=true",
                           ?OK),
    ?assertEqual(Total, maps:get(total_count, RegExByName)),
    ?assertEqual(1, maps:get(filtered_count, RegExByName)),
    ?assertEqual(1, maps:get(item_count, RegExByName)),
    ?assertEqual(1, maps:get(page, RegExByName)),
    ?assertEqual(2, maps:get(page_size, RegExByName)),
    ?assertEqual(1, maps:get(page_count, RegExByName)),
    assert_list([#{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], maps:get(items, RegExByName)),


    http_get(Config, "/queues?page=1000", ?BAD_REQUEST),
    http_get(Config, "/queues?page=-1", ?BAD_REQUEST),
    http_get(Config, "/queues?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/queues?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/test2_reg", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/reg_test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
    passed.

queue_pagination_columns_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),

    http_get(Config, "/queues/vh1?columns=name&page=1&page_size=2", ?OK),
    http_put(Config, "/queues/%2F/queue_a", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/queue_b", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/queue_c", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/queue_d", QArgs, {group, '2xx'}),
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

    ColumnNameVhost = http_get(Config, "/queues/vh1?columns=name&page=1&page_size=2", ?OK),
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
          vhost => <<"vh1">>},
        #{name  => <<"queue_d">>,
          vhost => <<"vh1">>}
    ], maps:get(items, ColumnsNameVhost)),


    http_delete(Config, "/queues/%2F/queue_a", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/queue_b", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/queue_c", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/queue_d", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
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
    timer:sleep(2000),
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
    timer:sleep(2000),

    http_get(Config, "/queues/%2F?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/queues/%2F?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
    http_get(Config, "/queues/%2F/test-001?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/queues/%2F/test-001?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_delete(Config, "/queues/%2F/test-001", {group, '2xx'}),

    %% Vhosts
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    timer:sleep(2000),

    http_get(Config, "/vhosts?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/vhosts?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
    http_get(Config, "/vhosts/vh1?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/vhosts/vh1?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

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
    timer:sleep(2000),
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
    timer:sleep(2000),
    assert_list([#{name => <<"test0">>,
                   consumer_capacity => 0,
                   consumer_utilisation => 0,
                   exclusive_consumer_tag => null,
                   recoverable_slaves => null}], http_get(Config, "/queues", ?OK)),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh129", {group, '2xx'}),
    passed.

columns_test(Config) ->
    Path = "/queues/%2F/columns.test",
    TTL = 30000,
    http_delete(Config, Path, [{group, '2xx'}, 404]),
    http_put(Config, Path, [{arguments, [{<<"x-message-ttl">>, TTL}]}],
             {group, '2xx'}),
    Item = #{arguments => #{'x-message-ttl' => TTL}, name => <<"columns.test">>},
    timer:sleep(2000),
    [Item] = http_get(Config, "/queues?columns=arguments.x-message-ttl,name", ?OK),
    Item = http_get(Config, "/queues/%2F/columns.test?columns=arguments.x-message-ttl,name", ?OK),
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
    timer:sleep(250),
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


-define(LARGE_BODY_BYTES, 25000000).

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
    timer:sleep(250),
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
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
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
    io:format("CORS test R: ~p~n", [R]),
    {ok, {_, HdNoCORS, _}} = R,
    io:format("CORS test HdNoCORS: ~p~n", [HdNoCORS]),
    false = lists:keymember("access-control-allow-origin", 1, HdNoCORS),
    %% The Vary header should include "Origin" regardless of CORS configuration.
    {_, "accept, accept-encoding, origin"} = lists:keyfind("vary", 1, HdNoCORS),
    %% Enable CORS.
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_management, cors_allow_origins, ["https://rabbitmq.com"]]),
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
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_management, cors_max_age, undefined]),
    %% We shouldn't receive max-age anymore.
    {ok, {_, HdNoMaxAgeCORS, _}} = req(Config, options, "/overview",
                                       [{"origin", "https://rabbitmq.com"}, auth_header("guest", "guest")]),
    false = lists:keymember("access-control-max-age", 1, HdNoMaxAgeCORS),

    %% Check OPTIONS method in all paths
    check_cors_all_endpoints(Config),
    %% Disable CORS again.
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_management, cors_allow_origins, []]),
    passed.

check_cors_all_endpoints(Config) ->
    Endpoints = get_all_http_endpoints(),

    [begin
        ct:pal("Options for ~p~n", [EP]),
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
            ok = rabbit_ct_broker_helpers:set_parameter(Config, 0, VHost, <<"vhost-limits">>, <<"limits">>, Param)
        end,
        Expected),

    Expected = http_get(Config, "/vhost-limits", ?OK),
    Limits1 = http_get(Config, "/vhost-limits/limit_test_vhost_1", ?OK),
    Limits2 = http_get(Config, "/vhost-limits/limit_test_vhost_2", ?OK),

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
    Map1 = http_get(Config, "/auth", ?OK),
    %% Defaults
    ?assertEqual(false, maps:get(enable_uaa, Map1)),
    ?assertEqual(<<>>, maps:get(uaa_client_id, Map1)),
    ?assertEqual(<<>>, maps:get(uaa_location, Map1)),
    %% Misconfiguration
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, enable_uaa, true]),
    Map2 = http_get(Config, "/auth", ?OK),
    ?assertEqual(false, maps:get(enable_uaa, Map2)),
    ?assertEqual(<<>>, maps:get(uaa_client_id, Map2)),
    ?assertEqual(<<>>, maps:get(uaa_location, Map2)),
    %% Valid config
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, uaa_client_id, "rabbit_user"]),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, uaa_location, "http://localhost:8080/uaa"]),
    Map3 = http_get(Config, "/auth", ?OK),
    ?assertEqual(true, maps:get(enable_uaa, Map3)),
    ?assertEqual(<<"rabbit_user">>, maps:get(uaa_client_id, Map3)),
    ?assertEqual(<<"http://localhost:8080/uaa">>, maps:get(uaa_location, Map3)),
    %% cleanup
    rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                 [rabbitmq_management, enable_uaa]).

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
    [Cookie, _Version] = binary:split(list_to_binary(proplists:get_value("set-cookie", Headers)),
                                      <<";">>, [global]),
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
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, disable_basic_auth, true]),
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
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, disable_basic_auth, 50]),
    %% Defaults to 'false' when config is invalid
    http_get(Config, "/overview", ?OK).

auth_attempts_test(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
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

    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, track_auth_attempt_source, true]),
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

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

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

wait_until(_Fun, 0) ->
    ?assert(wait_failed);
wait_until(Fun, N) ->
    case Fun() of
    true ->
        timer:sleep(1500);
    false ->
        timer:sleep(?COLLECT_INTERVAL + 100),
        wait_until(Fun, N - 1)
    end.

http_post_json(Config, Path, Body, Assertion) ->
    http_upload_raw(Config,  post, Path, Body, "guest", "guest",
                    Assertion, [{"content-type", "application/json"}]).

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
