%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1, open_unmanaged_connection/1]).
-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2,
                                assert_keys/2, assert_no_keys/2,
                                http_get/2, http_get/3, http_get/5,
                                http_put/4, http_put/6,
                                http_post/4, http_post/6,
                                http_delete/3, http_delete/5,
                                http_put_raw/4, http_post_accept_json/4,
                                req/4, auth_header/2,
                                amqp_port/1]).

-import(rabbit_misc, [pget/2]).

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               overview_test,
                               auth_test,
                               cluster_name_test,
                               nodes_test,
                               memory_test,
                               ets_tables_memory_test,
                               vhosts_test,
                               vhosts_trace_test,
                               users_test,
                               users_legacy_administrator_test,
                               permissions_validation_test,
                               permissions_list_test,
                               permissions_test,
                               connections_test,
                               multiple_invalid_connections_test,
                               exchanges_test,
                               queues_test,
                               bindings_test,
                               bindings_post_test,
                               bindings_e2e_test,
                               permissions_administrator_test,
                               permissions_vhost_test,
                               permissions_amqp_test,
                               permissions_connection_channel_consumer_test,
                               consumers_test,
                               definitions_test,
                               definitions_vhost_test,
                               definitions_password_test,
                               definitions_remove_things_test,
                               definitions_server_named_queue_test,
                               aliveness_test,
                               healthchecks_test,
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
                               queues_pagination_permissions_test,
                               samples_range_test,
                               sorting_test,
                               format_output_test,
                               columns_test,
                               get_test,
                               get_fail_test,
                               publish_test,
                               publish_accept_json_test,
                               publish_fail_test,
                               publish_base64_test,
                               publish_unrouted_test,
                               if_empty_unused_test,
                               parameters_test,
                               policy_test,
                               policy_permissions_test,
                               issue67_test,
                               extensions_test,
                               cors_test
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, ?MODULE}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
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


overview_test(Config) ->
    %% Rather crude, but this req doesn't say much and at least this means it
    %% didn't blow up.
    true = 0 < length(pget(listeners, http_get(Config, "/overview"))),
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags,     <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_get(Config, "/overview", "myuser", "myuser", ?OK),
    http_delete(Config, "/users/myuser", ?NO_CONTENT),

    passed.

cluster_name_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags,     <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/cluster-name", [{name, "foo"}], "myuser", "myuser", ?NOT_AUTHORISED),
    http_put(Config, "/cluster-name", [{name, "foo"}], ?NO_CONTENT),
    [{name, <<"foo">>}] = http_get(Config, "/cluster-name", "myuser", "myuser", ?OK),
    http_delete(Config, "/users/myuser", ?NO_CONTENT),
    passed.

nodes_test(Config) ->
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/monitor", [{password, <<"monitor">>},
                                        {tags, <<"monitoring">>}], [?CREATED, ?NO_CONTENT]),
    DiscNode = [{type, <<"disc">>}, {running, true}],
    assert_list([DiscNode], http_get(Config, "/nodes")),
    assert_list([DiscNode], http_get(Config, "/nodes", "monitor", "monitor", ?OK)),
    http_get(Config, "/nodes", "user", "user", ?NOT_AUTHORISED),
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(pget(name, Node)),
    assert_item(DiscNode, http_get(Config, Path, ?OK)),
    assert_item(DiscNode, http_get(Config, Path, "monitor", "monitor", ?OK)),
    http_get(Config, Path, "user", "user", ?NOT_AUTHORISED),
    http_delete(Config, "/users/user", ?NO_CONTENT),
    http_delete(Config, "/users/monitor", ?NO_CONTENT),
    passed.

memory_test(Config) ->
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(pget(name, Node)) ++ "/memory",
    Result = http_get(Config, Path, ?OK),
    assert_keys([memory], Result),
    Keys = [total, connection_readers, connection_writers, connection_channels,
            connection_other, queue_procs, queue_slave_procs, plugins,
            other_proc, mnesia, mgmt_db, msg_index, other_ets, binary, code,
            atom, other_system],
    assert_keys(Keys, pget(memory, Result)),
    http_get(Config, "/nodes/nonode/memory", ?NOT_FOUND),
    %% Relative memory as a percentage of the total
    Result1 = http_get(Config, Path ++ "/relative", ?OK),
    assert_keys([memory], Result1),
    Breakdown = pget(memory, Result1),
    assert_keys(Keys, Breakdown),
    assert_item([{total, 100}], Breakdown),
    assert_percentage(Breakdown),
    http_get(Config, "/nodes/nonode/memory/relative", ?NOT_FOUND),
    passed.

ets_tables_memory_test(Config) ->
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(pget(name, Node)) ++ "/memory/ets",
    Result = http_get(Config, Path, ?OK),
    assert_keys([ets_tables_memory], Result),
    NonMgmtKeys = [rabbit_vhost,rabbit_user_permission],
    Keys = [total, old_stats_fine_index,
            connection_stats_key_index, channel_stats_key_index,
            old_stats, node_node_stats, node_stats, consumers_by_channel,
            consumers_by_queue, channel_stats, connection_stats, queue_stats],
    assert_keys(Keys ++ NonMgmtKeys, pget(ets_tables_memory, Result)),
    http_get(Config, "/nodes/nonode/memory/ets", ?NOT_FOUND),
    %% Relative memory as a percentage of the total
    ResultRelative = http_get(Config, Path ++ "/relative", ?OK),
    assert_keys([ets_tables_memory], ResultRelative),
    Breakdown = pget(ets_tables_memory, ResultRelative),
    assert_keys(Keys, Breakdown),
    assert_item([{total, 100}], Breakdown),
    assert_percentage(Breakdown),
    http_get(Config, "/nodes/nonode/memory/ets/relative", ?NOT_FOUND),

    ResultMgmt = http_get(Config, Path ++ "/management", ?OK),
    assert_keys([ets_tables_memory], ResultMgmt),
    assert_keys(Keys, pget(ets_tables_memory, ResultMgmt)),
    assert_no_keys(NonMgmtKeys, pget(ets_tables_memory, ResultMgmt)),

    ResultMgmtRelative = http_get(Config, Path ++ "/management/relative", ?OK),
    assert_keys([ets_tables_memory], ResultMgmtRelative),
    assert_keys(Keys, pget(ets_tables_memory, ResultMgmtRelative)),
    assert_no_keys(NonMgmtKeys, pget(ets_tables_memory, ResultMgmtRelative)),
    assert_item([{total, 100}], pget(ets_tables_memory, ResultMgmtRelative)),
    assert_percentage(pget(ets_tables_memory, ResultMgmtRelative)),

    ResultUnknownFilter = http_get(Config, Path ++ "/blahblah", ?OK),
    [{ets_tables_memory, <<"no_tables">>}] = ResultUnknownFilter,
    passed.

assert_percentage(Breakdown) ->
    Total = lists:sum([P || {K, P} <- Breakdown, K =/= total]),
    Count = length(Breakdown) - 1,
    %% Rounding up and down can lose some digits. Never more than the number
    %% of items in the breakdown.
    case ((Total =< 100 + Count) andalso (Total >= 100 - Count)) of
        false ->
            throw({bad_percentage, Total, Breakdown});
        true ->
            ok
    end.

auth_test(Config) ->
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"">>}], [?CREATED, ?NO_CONTENT]),
    test_auth(Config, ?NOT_AUTHORISED, []),
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("user", "user")]),
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("guest", "gust")]),
    test_auth(Config, ?OK, [auth_header("guest", "guest")]),
    http_delete(Config, "/users/user", ?NO_CONTENT),
    passed.

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
vhosts_test(Config) ->
    assert_list([[{name, <<"/">>}]], http_get(Config, "/vhosts")),
    %% Create a new one
    http_put(Config, "/vhosts/myvhost", none, [?CREATED, ?NO_CONTENT]),
    %% PUT should be idempotent
    http_put(Config, "/vhosts/myvhost", none, ?NO_CONTENT),
    %% Check it's there
    assert_list([[{name, <<"/">>}], [{name, <<"myvhost">>}]],
                http_get(Config, "/vhosts")),
    %% Check individually
    assert_item([{name, <<"/">>}], http_get(Config, "/vhosts/%2f", ?OK)),
    assert_item([{name, <<"myvhost">>}],http_get(Config, "/vhosts/myvhost")),
    %% Delete it
    http_delete(Config, "/vhosts/myvhost", ?NO_CONTENT),
    %% It's not there
    http_get(Config, "/vhosts/myvhost", ?NOT_FOUND),
    http_delete(Config, "/vhosts/myvhost", ?NOT_FOUND),

    passed.

vhosts_trace_test(Config) ->
    http_put(Config, "/vhosts/myvhost", none, [?CREATED, ?NO_CONTENT]),
    Disabled = [{name,  <<"myvhost">>}, {tracing, false}],
    Enabled  = [{name,  <<"myvhost">>}, {tracing, true}],
    Disabled = http_get(Config, "/vhosts/myvhost"),
    http_put(Config, "/vhosts/myvhost", [{tracing, true}], ?NO_CONTENT),
    Enabled = http_get(Config, "/vhosts/myvhost"),
    http_put(Config, "/vhosts/myvhost", [{tracing, true}], ?NO_CONTENT),
    Enabled = http_get(Config, "/vhosts/myvhost"),
    http_put(Config, "/vhosts/myvhost", [{tracing, false}], ?NO_CONTENT),
    Disabled = http_get(Config, "/vhosts/myvhost"),
    http_delete(Config, "/vhosts/myvhost", ?NO_CONTENT),

    passed.

users_test(Config) ->
    assert_item([{name, <<"guest">>}, {tags, <<"administrator">>}],
                http_get(Config, "/whoami")),
    http_get(Config, "/users/myuser", ?NOT_FOUND),
    http_put_raw(Config, "/users/myuser", "Something not JSON", ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{flim, <<"flam">>}], ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/myuser", [{password_hash, <<"not_hash">>}], ?BAD_REQUEST),
    http_put(Config, "/users/myuser", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                       {tags, <<"management">>}], ?NO_CONTENT),
    assert_item([{name, <<"myuser">>}, {tags, <<"management">>},
                 {password_hash, <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                 {hashing_algorithm, <<"rabbit_password_hashing_sha256">>}],
                http_get(Config, "/users/myuser")),

    http_put(Config, "/users/myuser", [{password_hash,
                                        <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                                       {hashing_algorithm, <<"rabbit_password_hashing_md5">>},
                                       {tags, <<"management">>}], ?NO_CONTENT),
    assert_item([{name, <<"myuser">>}, {tags, <<"management">>},
                 {password_hash, <<"IECV6PZI/Invh0DL187KFpkO5Jc=">>},
                 {hashing_algorithm, <<"rabbit_password_hashing_md5">>}],
                http_get(Config, "/users/myuser")),
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                                       {tags, <<"administrator, foo">>}], ?NO_CONTENT),
    assert_item([{name, <<"myuser">>}, {tags, <<"administrator,foo">>}],
                http_get(Config, "/users/myuser")),
    assert_list([[{name, <<"myuser">>}, {tags, <<"administrator,foo">>}],
                 [{name, <<"guest">>}, {tags, <<"administrator">>}]],
                http_get(Config, "/users")),
    test_auth(Config, ?OK, [auth_header("myuser", "password")]),
    http_delete(Config, "/users/myuser", ?NO_CONTENT),
    test_auth(Config, ?NOT_AUTHORISED, [auth_header("myuser", "password")]),
    http_get(Config, "/users/myuser", ?NOT_FOUND),
    passed.

users_legacy_administrator_test(Config) ->
    http_put(Config, "/users/myuser1", [{administrator, <<"true">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/myuser2", [{administrator, <<"false">>}], [?CREATED, ?NO_CONTENT]),
    assert_item([{name, <<"myuser1">>}, {tags, <<"administrator">>}],
                http_get(Config, "/users/myuser1")),
    assert_item([{name, <<"myuser2">>}, {tags, <<"">>}],
                http_get(Config, "/users/myuser2")),
    http_delete(Config, "/users/myuser1", ?NO_CONTENT),
    http_delete(Config, "/users/myuser2", ?NO_CONTENT),
    passed.

permissions_validation_test(Config) ->
    Good = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/wrong/guest", Good, ?BAD_REQUEST),
    http_put(Config, "/permissions/%2f/wrong", Good, ?BAD_REQUEST),
    http_put(Config, "/permissions/%2f/guest",
             [{configure, <<"[">>}, {write, <<".*">>}, {read, <<".*">>}],
             ?BAD_REQUEST),
    http_put(Config, "/permissions/%2f/guest", Good, ?NO_CONTENT),
    passed.

permissions_list_test(Config) ->
    [[{user,<<"guest">>},
      {vhost,<<"/">>},
      {configure,<<".*">>},
      {write,<<".*">>},
      {read,<<".*">>}]] =
        http_get(Config, "/permissions"),

    http_put(Config, "/users/myuser1", [{password, <<"">>}, {tags, <<"administrator">>}],
             [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/myuser2", [{password, <<"">>}, {tags, <<"administrator">>}],
             [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/vhosts/myvhost1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/vhosts/myvhost2", none, [?CREATED, ?NO_CONTENT]),

    Perms = [{configure, <<"foo">>}, {write, <<"foo">>}, {read, <<"foo">>}],
    http_put(Config, "/permissions/myvhost1/myuser1", Perms, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/myvhost2/myuser1", Perms, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/myvhost1/myuser2", Perms, [?CREATED, ?NO_CONTENT]),

    4 = length(http_get(Config, "/permissions")),
    2 = length(http_get(Config, "/users/myuser1/permissions")),
    1 = length(http_get(Config, "/users/myuser2/permissions")),

    http_get(Config, "/users/notmyuser/permissions", ?NOT_FOUND),
    http_get(Config, "/vhosts/notmyvhost/permissions", ?NOT_FOUND),

    http_delete(Config, "/users/myuser1", ?NO_CONTENT),
    http_delete(Config, "/users/myuser2", ?NO_CONTENT),
    http_delete(Config, "/vhosts/myvhost1", ?NO_CONTENT),
    http_delete(Config, "/vhosts/myvhost2", ?NO_CONTENT),
    passed.

permissions_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"myuser">>}, {tags, <<"administrator">>}],
             [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/vhosts/myvhost", none, [?CREATED, ?NO_CONTENT]),

    http_put(Config, "/permissions/myvhost/myuser",
             [{configure, <<"foo">>}, {write, <<"foo">>}, {read, <<"foo">>}],
             [?CREATED, ?NO_CONTENT]),

    Permission = [{user,<<"myuser">>},
                  {vhost,<<"myvhost">>},
                  {configure,<<"foo">>},
                  {write,<<"foo">>},
                  {read,<<"foo">>}],
    Default = [{user,<<"guest">>},
               {vhost,<<"/">>},
               {configure,<<".*">>},
               {write,<<".*">>},
               {read,<<".*">>}],
    Permission = http_get(Config, "/permissions/myvhost/myuser"),
    assert_list([Permission, Default], http_get(Config, "/permissions")),
    assert_list([Permission], http_get(Config, "/users/myuser/permissions")),
    http_delete(Config, "/permissions/myvhost/myuser", ?NO_CONTENT),
    http_get(Config, "/permissions/myvhost/myuser", ?NOT_FOUND),

    http_delete(Config, "/users/myuser", ?NO_CONTENT),
    http_delete(Config, "/vhosts/myvhost", ?NO_CONTENT),
    passed.

connections_test(Config) ->
    {Conn, _Ch} = open_connection_and_channel(Config),
    LocalPort = local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A~w",
               [LocalPort, amqp_port(Config)])),
    http_get(Config, Path, ?OK),
    http_delete(Config, Path, ?NO_CONTENT),
    %% TODO rabbit_reader:shutdown/2 returns before the connection is
    %% closed. It may not be worth fixing.
    timer:sleep(200),
    http_get(Config, Path, ?NOT_FOUND),
    close_connection(Conn),
    passed.

multiple_invalid_connections_test(Config) ->
    Count = 100,
    spawn_invalid(Config, Count),
    Page0 = http_get(Config, "/connections?page=1&page_size=100", ?OK),
    wait_for_answers(Count),
    Page1 = http_get(Config, "/connections?page=1&page_size=100", ?OK),
    ?assertEqual(0, proplists:get_value(total_count, Page0)),
    ?assertEqual(0, proplists:get_value(total_count, Page1)),
    passed.

test_auth(Config, Code, Headers) ->
    {ok, {{_, Code, _}, _, _}} = req(Config, get, "/overview", Headers),
    passed.

exchanges_test(Config) ->
    %% Can pass booleans or strings
    Good = [{type, <<"direct">>}, {durable, <<"true">>}],
    http_put(Config, "/vhosts/myvhost", none, [?CREATED, ?NO_CONTENT]),
    http_get(Config, "/exchanges/myvhost/foo", ?NOT_AUTHORISED),
    http_put(Config, "/exchanges/myvhost/foo", Good, ?NOT_AUTHORISED),
    http_put(Config, "/permissions/myvhost/guest",
             [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
             [?CREATED, ?NO_CONTENT]),
    http_get(Config, "/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put(Config, "/exchanges/myvhost/foo", Good, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/exchanges/myvhost/foo", Good, ?NO_CONTENT),
    http_get(Config, "/exchanges/%2f/foo", ?NOT_FOUND),
    assert_item([{name,<<"foo">>},
                 {vhost,<<"myvhost">>},
                 {type,<<"direct">>},
                 {durable,true},
                 {auto_delete,false},
                 {internal,false},
                 {arguments,[]}],
                http_get(Config, "/exchanges/myvhost/foo")),

    http_put(Config, "/exchanges/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/exchanges/myvhost/bar", [{type, <<"bad_exchange_type">>}],
             ?BAD_REQUEST),
    http_put(Config, "/exchanges/myvhost/bar", [{type, <<"direct">>},
                                                {durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/exchanges/myvhost/foo", [{type, <<"direct">>}],
             ?BAD_REQUEST),

    http_delete(Config, "/exchanges/myvhost/foo", ?NO_CONTENT),
    http_delete(Config, "/exchanges/myvhost/foo", ?NOT_FOUND),

    http_delete(Config, "/vhosts/myvhost", ?NO_CONTENT),
    http_get(Config, "/exchanges/badvhost", ?NOT_FOUND),
    passed.

queues_test(Config) ->
    Good = [{durable, true}],
    http_get(Config, "/queues/%2f/foo", ?NOT_FOUND),
    http_put(Config, "/queues/%2f/foo", Good, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/foo", Good, ?NO_CONTENT),
    http_get(Config, "/queues/%2f/foo", ?OK),

    http_put(Config, "/queues/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/queues/%2f/bar",
             [{durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/queues/%2f/foo",
             [{durable, false}],
             ?BAD_REQUEST),

    http_put(Config, "/queues/%2f/baz", Good, [?CREATED, ?NO_CONTENT]),

    Queues = http_get(Config, "/queues/%2f"),
    Queue = http_get(Config, "/queues/%2f/foo"),
    assert_list([[{name,        <<"foo">>},
                  {vhost,       <<"/">>},
                  {durable,     true},
                  {auto_delete, false},
                  {exclusive,   false},
                  {arguments,   []}],
                 [{name,        <<"baz">>},
                  {vhost,       <<"/">>},
                  {durable,     true},
                  {auto_delete, false},
                  {exclusive,   false},
                  {arguments,   []}]], Queues),
    assert_item([{name,        <<"foo">>},
                 {vhost,       <<"/">>},
                 {durable,     true},
                 {auto_delete, false},
                 {exclusive,   false},
                 {arguments,   []}], Queue),

    http_delete(Config, "/queues/%2f/foo", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/baz", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/foo", ?NOT_FOUND),
    http_get(Config, "/queues/badvhost", ?NOT_FOUND),
    passed.

bindings_test(Config) ->
    XArgs = [{type, <<"direct">>}],
    QArgs = [],
    http_put(Config, "/exchanges/%2f/myexchange", XArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/myqueue", QArgs, [?CREATED, ?NO_CONTENT]),
    BArgs = [{routing_key, <<"routing">>}, {arguments, []}],
    http_post(Config, "/bindings/%2f/e/myexchange/q/myqueue", BArgs, [?CREATED, ?NO_CONTENT]),
    http_get(Config, "/bindings/%2f/e/myexchange/q/myqueue/routing", ?OK),
    http_get(Config, "/bindings/%2f/e/myexchange/q/myqueue/rooting", ?NOT_FOUND),
    Binding =
        [{source,<<"myexchange">>},
         {vhost,<<"/">>},
         {destination,<<"myqueue">>},
         {destination_type,<<"queue">>},
         {routing_key,<<"routing">>},
         {arguments,[]},
         {properties_key,<<"routing">>}],
    DBinding =
        [{source,<<"">>},
         {vhost,<<"/">>},
         {destination,<<"myqueue">>},
         {destination_type,<<"queue">>},
         {routing_key,<<"myqueue">>},
         {arguments,[]},
         {properties_key,<<"myqueue">>}],
    Binding = http_get(Config, "/bindings/%2f/e/myexchange/q/myqueue/routing"),
    assert_list([Binding],
                http_get(Config, "/bindings/%2f/e/myexchange/q/myqueue")),
    assert_list([Binding, DBinding],
                http_get(Config, "/queues/%2f/myqueue/bindings")),
    assert_list([Binding],
                http_get(Config, "/exchanges/%2f/myexchange/bindings/source")),
    http_delete(Config, "/bindings/%2f/e/myexchange/q/myqueue/routing", ?NO_CONTENT),
    http_delete(Config, "/bindings/%2f/e/myexchange/q/myqueue/routing", ?NOT_FOUND),
    http_delete(Config, "/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    http_get(Config, "/bindings/badvhost", ?NOT_FOUND),
    http_get(Config, "/bindings/badvhost/myqueue/myexchange/routing", ?NOT_FOUND),
    http_get(Config, "/bindings/%2f/e/myexchange/q/myqueue/routing", ?NOT_FOUND),
    passed.

bindings_post_test(Config) ->
    XArgs = [{type, <<"direct">>}],
    QArgs = [],
    BArgs = [{routing_key, <<"routing">>}, {arguments, [{foo, <<"bar">>}]}],
    http_put(Config, "/exchanges/%2f/myexchange", XArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/myqueue", QArgs, [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/bindings/%2f/e/myexchange/q/badqueue", BArgs, ?NOT_FOUND),
    http_post(Config, "/bindings/%2f/e/badexchange/q/myqueue", BArgs, ?NOT_FOUND),
    Headers1 = http_post(Config, "/bindings/%2f/e/myexchange/q/myqueue", [], [?CREATED, ?NO_CONTENT]),
    "../../../../%2F/e/myexchange/q/myqueue/~" = pget("location", Headers1),
    Headers2 = http_post(Config, "/bindings/%2f/e/myexchange/q/myqueue", BArgs, [?CREATED, ?NO_CONTENT]),
    PropertiesKey = "routing~V4mGFgnPNrdtRmluZIxTDA",
    PropertiesKeyBin = list_to_binary(PropertiesKey),
    "../../../../%2F/e/myexchange/q/myqueue/" ++ PropertiesKey =
        pget("location", Headers2),
    URI = "/bindings/%2F/e/myexchange/q/myqueue/" ++ PropertiesKey,
    [{source,<<"myexchange">>},
     {vhost,<<"/">>},
     {destination,<<"myqueue">>},
     {destination_type,<<"queue">>},
     {routing_key,<<"routing">>},
     {arguments,[{foo,<<"bar">>}]},
     {properties_key,PropertiesKeyBin}] = http_get(Config, URI, ?OK),
    http_get(Config, URI ++ "x", ?NOT_FOUND),
    http_delete(Config, URI, ?NO_CONTENT),
    http_delete(Config, "/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

bindings_e2e_test(Config) ->
    BArgs = [{routing_key, <<"routing">>}, {arguments, []}],
    http_post(Config, "/bindings/%2f/e/amq.direct/e/badexchange", BArgs, ?NOT_FOUND),
    http_post(Config, "/bindings/%2f/e/badexchange/e/amq.fanout", BArgs, ?NOT_FOUND),
    Headers = http_post(Config, "/bindings/%2f/e/amq.direct/e/amq.fanout", BArgs, [?CREATED, ?NO_CONTENT]),
    "../../../../%2F/e/amq.direct/e/amq.fanout/routing" =
        pget("location", Headers),
    [{source,<<"amq.direct">>},
     {vhost,<<"/">>},
     {destination,<<"amq.fanout">>},
     {destination_type,<<"exchange">>},
     {routing_key,<<"routing">>},
     {arguments,[]},
     {properties_key,<<"routing">>}] =
        http_get(Config, "/bindings/%2f/e/amq.direct/e/amq.fanout/routing", ?OK),
    http_delete(Config, "/bindings/%2f/e/amq.direct/e/amq.fanout/routing", ?NO_CONTENT),
    http_post(Config, "/bindings/%2f/e/amq.direct/e/amq.headers", BArgs, [?CREATED, ?NO_CONTENT]),
    Binding =
        [{source,<<"amq.direct">>},
         {vhost,<<"/">>},
         {destination,<<"amq.headers">>},
         {destination_type,<<"exchange">>},
         {routing_key,<<"routing">>},
         {arguments,[]},
         {properties_key,<<"routing">>}],
    Binding = http_get(Config, "/bindings/%2f/e/amq.direct/e/amq.headers/routing"),
    assert_list([Binding],
                http_get(Config, "/bindings/%2f/e/amq.direct/e/amq.headers")),
    assert_list([Binding],
                http_get(Config, "/exchanges/%2f/amq.direct/bindings/source")),
    assert_list([Binding],
                http_get(Config, "/exchanges/%2f/amq.headers/bindings/destination")),
    http_delete(Config, "/bindings/%2f/e/amq.direct/e/amq.headers/routing", ?NO_CONTENT),
    http_get(Config, "/bindings/%2f/e/amq.direct/e/amq.headers/rooting", ?NOT_FOUND),
    passed.

permissions_administrator_test(Config) ->
    http_put(Config, "/users/isadmin", [{password, <<"isadmin">>},
                                        {tags, <<"administrator">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/notadmin", [{password, <<"notadmin">>},
                                         {tags, <<"administrator">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/notadmin", [{password, <<"notadmin">>},
                                         {tags, <<"management">>}], ?NO_CONTENT),
    Test =
        fun(Path) ->
                http_get(Config, Path, "notadmin", "notadmin", ?NOT_AUTHORISED),
                http_get(Config, Path, "isadmin", "isadmin", ?OK),
                http_get(Config, Path, "guest", "guest", ?OK)
        end,
    %% All users can get a list of vhosts. It may be filtered.
    %%Test("/vhosts"),
    Test("/vhosts/%2f"),
    Test("/vhosts/%2f/permissions"),
    Test("/users"),
    Test("/users/guest"),
    Test("/users/guest/permissions"),
    Test("/permissions"),
    Test("/permissions/%2f/guest"),
    http_delete(Config, "/users/notadmin", ?NO_CONTENT),
    http_delete(Config, "/users/isadmin", ?NO_CONTENT),
    passed.

permissions_vhost_test(Config) ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/vhosts/myvhost1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/vhosts/myvhost2", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/myvhost1/myuser", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/myvhost1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/myvhost2/guest", PermArgs, [?CREATED, ?NO_CONTENT]),
    assert_list([[{name, <<"/">>}],
                 [{name, <<"myvhost1">>}],
                 [{name, <<"myvhost2">>}]], http_get(Config, "/vhosts", ?OK)),
    assert_list([[{name, <<"myvhost1">>}]],
                http_get(Config, "/vhosts", "myuser", "myuser", ?OK)),
    http_put(Config, "/queues/myvhost1/myqueue", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/myvhost2/myqueue", QArgs, [?CREATED, ?NO_CONTENT]),
    Test1 =
        fun(Path) ->
                Results = http_get(Config, Path, "myuser", "myuser", ?OK),
                [case pget(vhost, Result) of
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
    http_delete(Config, "/vhosts/myvhost1", ?NO_CONTENT),
    http_delete(Config, "/vhosts/myvhost2", ?NO_CONTENT),
    http_delete(Config, "/users/myuser", ?NO_CONTENT),
    passed.

permissions_amqp_test(Config) ->
    %% Just test that it works at all, not that it works in all possible cases.
    QArgs = [],
    PermArgs = [{configure, <<"foo.*">>}, {write, <<"foo.*">>},
                {read,      <<"foo.*">>}],
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/%2f/myuser", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/bar-queue", QArgs, "myuser", "myuser",
             ?NOT_AUTHORISED),
    http_put(Config, "/queues/%2f/bar-queue", QArgs, "nonexistent", "nonexistent",
             ?NOT_AUTHORISED),
    http_delete(Config, "/users/myuser", ?NO_CONTENT),
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
                 "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A~w",
                 [LocalPort, Port]),
    ChPath = rabbit_misc:format(
               "/channels/127.0.0.1%3A~w%20->%20127.0.0.1%3A~w%20(1)",
               [LocalPort, Port]),
    ConnChPath = rabbit_misc:format(
                   "/connections/127.0.0.1%3A~w%20->%20127.0.0.1%3A~w/channels",
                   [LocalPort, Port]),
    {Conn, ConnPath, ChPath, ConnChPath}.

permissions_connection_channel_consumer_test(Config) ->
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/%2f/user", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/monitor", [{password, <<"monitor">>},
                                        {tags, <<"monitoring">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/%2f/monitor", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/test", [], [?CREATED, ?NO_CONTENT]),

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
                           ?assertEqual(Len,
                                        length(http_get(Config, Path, User, User, ?OK)))
                   end,
    [begin
         AssertLength(P, "user", 1),
         AssertLength(P, "monitor", 3),
         AssertLength(P, "guest", 3)
     end || P <- ["/connections", "/channels", "/consumers", "/consumers/%2f"]],

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
    AssertClose(AdmConn, "guest", ?NO_CONTENT),
    AssertClose(MonConn, "guest", ?NO_CONTENT),
    AssertClose(UserConn, "user", ?NO_CONTENT),

    http_delete(Config, "/users/user", ?NO_CONTENT),
    http_delete(Config, "/users/monitor", ?NO_CONTENT),
    http_get(Config, "/connections/foo", ?NOT_FOUND),
    http_get(Config, "/channels/foo", ?NOT_FOUND),
    http_delete(Config, "/queues/%2f/test", ?NO_CONTENT),
    passed.




consumers_test(Config) ->
    http_put(Config, "/queues/%2f/test", [], [?CREATED, ?NO_CONTENT]),
    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(
      Ch, #'basic.consume'{queue        = <<"test">>,
                           no_ack       = false,
                           consumer_tag = <<"my-ctag">> }, self()),
    assert_list([[{exclusive,    false},
                  {ack_required, true},
                  {consumer_tag, <<"my-ctag">>}]], http_get(Config, "/consumers")),
    amqp_connection:close(Conn),
    http_delete(Config, "/queues/%2f/test", ?NO_CONTENT),
    passed.

defs(Config, Key, URI, CreateMethod, Args) ->
    defs(Config, Key, URI, CreateMethod, Args,
         fun(URI2) -> http_delete(Config, URI2, ?NO_CONTENT) end).

defs_v(Config, Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,
    Rep2 = fun (L, V2) -> lists:keymap(fun (vhost) -> V2;
                                           (V)     -> V end, 2, L) end,
    %% Test against default vhost
    defs(Config, Key, Rep1(URI, "%2f"), CreateMethod, Rep2(Args, <<"/">>)),

    %% Test against new vhost
    http_put(Config, "/vhosts/test", none, [?CREATED, ?NO_CONTENT]),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/test/guest", PermArgs, [?CREATED, ?NO_CONTENT]),
    defs(Config, Key, Rep1(URI, "test"), CreateMethod, Rep2(Args, <<"test">>),
         fun(URI2) -> http_delete(Config, URI2, ?NO_CONTENT),
                      http_delete(Config, "/vhosts/test", ?NO_CONTENT) end).

create(Config, CreateMethod, URI, Args) ->
    case CreateMethod of
        put        -> http_put(Config, URI, Args, [?CREATED, ?NO_CONTENT]),
                      URI;
        put_update -> http_put(Config, URI, Args, ?NO_CONTENT),
                      URI;
        post       -> Headers = http_post(Config, URI, Args, [?CREATED, ?NO_CONTENT]),
                      rabbit_web_dispatch_util:unrelativise(
                        URI, pget("location", Headers))
    end.

defs(Config, Key, URI, CreateMethod, Args, DeleteFun) ->
    %% Create the item
    URI2 = create(Config, CreateMethod, URI, Args),
    %% Make sure it ends up in definitions
    Definitions = http_get(Config, "/definitions", ?OK),
    true = lists:any(fun(I) -> test_item(Args, I) end, pget(Key, Definitions)),

    %% Delete it
    DeleteFun(URI2),

    %% Post the definitions back, it should get recreated in correct form
    http_post(Config, "/definitions", Definitions, ?CREATED),
    assert_item(Args, http_get(Config, URI2, ?OK)),

    %% And delete it again
    DeleteFun(URI2),

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
           [{name,    <<"my-queue">>},
            {durable, true}]),
    defs_v(Config, exchanges, "/exchanges/<vhost>/my-exchange", put,
           [{name, <<"my-exchange">>},
            {type, <<"direct">>}]),
    defs_v(Config, bindings, "/bindings/<vhost>/e/amq.direct/e/amq.fanout", post,
           [{routing_key, <<"routing">>}, {arguments, []}]),
    defs_v(Config, policies, "/policies/<vhost>/my-policy", put,
           [{vhost,      vhost},
            {name,       <<"my-policy">>},
            {pattern,    <<".*">>},
            {definition, [{testpos, [1, 2, 3]}]},
            {priority,   1}]),
    defs_v(Config, parameters, "/parameters/test/<vhost>/good", put,
           [{vhost,     vhost},
            {component, <<"test">>},
            {name,      <<"good">>},
            {value,     <<"ignore">>}]),
    defs(Config, users, "/users/myuser", put,
         [{name,          <<"myuser">>},
          {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
          {hashing_algorithm, <<"rabbit_password_hashing_sha256">>},
          {tags,          <<"management">>}]),
    defs(Config, vhosts, "/vhosts/myvhost", put,
         [{name, <<"myvhost">>}]),
    defs(Config, permissions, "/permissions/%2f/guest", put,
         [{user,      <<"guest">>},
          {vhost,     <<"/">>},
          {configure, <<"c">>},
          {write,     <<"w">>},
          {read,      <<"r">>}]),

    %% We just messed with guest's permissions
    http_put(Config, "/permissions/%2f/guest",
             [{configure, <<".*">>},
              {write,     <<".*">>},
              {read,      <<".*">>}], [?CREATED, ?NO_CONTENT]),
    BrokenConfig =
        [{users,       []},
         {vhosts,      []},
         {permissions, []},
         {queues,      []},
         {exchanges,   [[{name,        <<"amq.direct">>},
                         {vhost,       <<"/">>},
                         {type,        <<"definitely not direct">>},
                         {durable,     true},
                         {auto_delete, false},
                         {arguments,   []}
                        ]]},
         {bindings,    []}],
    http_post(Config, "/definitions", BrokenConfig, ?BAD_REQUEST),

    unregister_parameters_and_policy_validator(Config),
    passed.

defs_vhost(Config, Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,
    Rep2 = fun (L, V2) -> lists:keymap(fun (vhost) -> V2;
                                           (V)     -> V end, 2, L) end,

    %% Create test vhost
    http_put(Config, "/vhosts/test", none, [?CREATED, ?NO_CONTENT]),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/test/guest", PermArgs, [?CREATED, ?NO_CONTENT]),

    %% Test against default vhost
    defs_vhost(Config, Key, URI, Rep1, "%2f", "test", CreateMethod,
               Rep2(Args, <<"/">>), Rep2(Args, <<"test">>),
               fun(URI2) -> http_delete(Config, URI2, [?NO_CONTENT, ?CREATED]) end),

    %% Test against test vhost
    defs_vhost(Config, Key, URI, Rep1, "test", "%2f", CreateMethod,
               Rep2(Args, <<"test">>), Rep2(Args, <<"/">>),
               fun(URI2) -> http_delete(Config, URI2, [?NO_CONTENT, ?CREATED]) end),

    %% Remove test vhost
    http_delete(Config, "/vhosts/test", ?NO_CONTENT).


defs_vhost(Config, Key, URI0, Rep1, VHost1, VHost2, CreateMethod, Args1, Args2,
           DeleteFun) ->
    %% Create the item
    URI2 = create(Config, CreateMethod, Rep1(URI0, VHost1), Args1),
    %% Make sure it ends up in definitions
    Definitions = http_get(Config, "/definitions/" ++ VHost1, ?OK),
    true = lists:any(fun(I) -> test_item(Args1, I) end, pget(Key, Definitions)),

    %% Make sure it is not in the other vhost
    Definitions0 = http_get(Config, "/definitions/" ++ VHost2, ?OK),
    false = lists:any(fun(I) -> test_item(Args2, I) end, pget(Key, Definitions0)),

    %% Post the definitions back
    http_post(Config, "/definitions/" ++ VHost2, Definitions, [?NO_CONTENT, ?CREATED]),

    %% Make sure it is now in the other vhost
    Definitions1 = http_get(Config, "/definitions/" ++ VHost2, ?OK),
    true = lists:any(fun(I) -> test_item(Args2, I) end, pget(Key, Definitions1)),

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
               [{name,    <<"my-queue">>},
                {durable, true}]),
    defs_vhost(Config, exchanges, "/exchanges/<vhost>/my-exchange", put,
               [{name, <<"my-exchange">>},
                {type, <<"direct">>}]),
    defs_vhost(Config, bindings, "/bindings/<vhost>/e/amq.direct/e/amq.fanout", post,
               [{routing_key, <<"routing">>}, {arguments, []}]),
    defs_vhost(Config, policies, "/policies/<vhost>/my-policy", put,
               [{vhost,      vhost},
                {name,       <<"my-policy">>},
                {pattern,    <<".*">>},
                {definition, [{testpos, [1, 2, 3]}]},
                {priority,   1}]),

    Upload =
        [{queues,      []},
         {exchanges,   []},
         {policies,    []},
         {bindings,    []}],
    http_post(Config, "/definitions/othervhost", Upload, ?BAD_REQUEST),

    unregister_parameters_and_policy_validator(Config),
    passed.

definitions_password_test(Config) ->
                                                % Import definitions from 3.5.x
    Config35 = [{rabbit_version, <<"3.5.4">>},
                {users, [[{name,          <<"myuser">>},
                          {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
                          {tags,          <<"management">>}]
                        ]}],
    Expected35 = [{name,          <<"myuser">>},
                  {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
                  {hashing_algorithm, <<"rabbit_password_hashing_md5">>},
                  {tags,          <<"management">>}],
    http_post(Config, "/definitions", Config35, ?CREATED),
    Definitions35 = http_get(Config, "/definitions", ?OK),
    Users35 = pget(users, Definitions35),
    true = lists:any(fun(I) -> test_item(Expected35, I) end, Users35),

    %% Import definitions from from 3.6.0
    Config36 = [{rabbit_version, <<"3.6.0">>},
                {users, [[{name,          <<"myuser">>},
                          {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
                          {tags,          <<"management">>}]
                        ]}],
    Expected36 = [{name,          <<"myuser">>},
                  {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
                  {hashing_algorithm, <<"rabbit_password_hashing_sha256">>},
                  {tags,          <<"management">>}],
    http_post(Config, "/definitions", Config36, ?CREATED),

    Definitions36 = http_get(Config, "/definitions", ?OK),
    Users36 = pget(users, Definitions36),
    true = lists:any(fun(I) -> test_item(Expected36, I) end, Users36),

    %% No hashing_algorithm provided
    ConfigDefault = [{rabbit_version, <<"3.6.1">>},
                     {users, [[{name,          <<"myuser">>},
                               {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
                               {tags,          <<"management">>}]
                             ]}],
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbit,
                                                                   password_hashing_module,
                                                                   rabbit_password_hashing_sha512]),

    ExpectedDefault = [{name,          <<"myuser">>},
                       {password_hash, <<"WAbU0ZIcvjTpxM3Q3SbJhEAM2tQ=">>},
                       {hashing_algorithm, <<"rabbit_password_hashing_sha512">>},
                       {tags,          <<"management">>}],
    http_post(Config, "/definitions", ConfigDefault, ?CREATED),

    DefinitionsDefault = http_get(Config, "/definitions", ?OK),
    UsersDefault = pget(users, DefinitionsDefault),

    true = lists:any(fun(I) -> test_item(ExpectedDefault, I) end, UsersDefault),
    passed.

definitions_remove_things_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"my-exclusive">>,
                                            exclusive = true }),
    http_get(Config, "/queues/%2f/my-exclusive", ?OK),
    Definitions = http_get(Config, "/definitions", ?OK),
    [] = pget(queues, Definitions),
    [] = pget(exchanges, Definitions),
    [] = pget(bindings, Definitions),
    amqp_channel:close(Ch),
    close_connection(Conn),
    passed.

definitions_server_named_queue_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
        amqp_channel:call(Ch, #'queue.declare'{}),
    close_channel(Ch),
    close_connection(Conn),
    Path = "/queues/%2f/" ++ mochiweb_util:quote_plus(QName),
    http_get(Config, Path, ?OK),
    Definitions = http_get(Config, "/definitions", ?OK),
    http_delete(Config, Path, ?NO_CONTENT),
    http_get(Config, Path, ?NOT_FOUND),
    http_post(Config, "/definitions", Definitions, [?CREATED, ?NO_CONTENT]),
    http_get(Config, Path, ?OK),
    http_delete(Config, Path, ?NO_CONTENT),
    passed.

aliveness_test(Config) ->
    [{status, <<"ok">>}] = http_get(Config, "/aliveness-test/%2f", ?OK),
    http_get(Config, "/aliveness-test/foo", ?NOT_FOUND),
    http_delete(Config, "/queues/%2f/aliveness-test", ?NO_CONTENT),
    passed.

healthchecks_test(Config) ->
    [{status, <<"ok">>}] = http_get(Config, "/healthchecks/node", ?OK),
    http_get(Config, "/healthchecks/node/foo", ?NOT_FOUND),
    passed.

arguments_test(Config) ->
    XArgs = [{type, <<"headers">>},
             {arguments, [{'alternate-exchange', <<"amq.direct">>}]}],
    QArgs = [{arguments, [{'x-expires', 1800000}]}],
    BArgs = [{routing_key, <<"">>},
             {arguments, [{'x-match', <<"all">>},
                          {foo, <<"bar">>}]}],
    http_put(Config, "/exchanges/%2f/myexchange", XArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/myqueue", QArgs, [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/bindings/%2f/e/myexchange/q/myqueue", BArgs, [?CREATED, ?NO_CONTENT]),
    Definitions = http_get(Config, "/definitions", ?OK),
    http_delete(Config, "/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    http_post(Config, "/definitions", Definitions, ?CREATED),
    [{'alternate-exchange', <<"amq.direct">>}] =
        pget(arguments, http_get(Config, "/exchanges/%2f/myexchange", ?OK)),
    [{'x-expires', 1800000}] =
        pget(arguments, http_get(Config, "/queues/%2f/myqueue", ?OK)),
    true = lists:sort([{'x-match', <<"all">>}, {foo, <<"bar">>}]) =:=
	lists:sort(pget(arguments,
			http_get(Config, "/bindings/%2f/e/myexchange/q/myqueue/" ++
				     "~nXOkVwqZzUOdS9_HcBWheg", ?OK))),
    http_delete(Config, "/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

arguments_table_test(Config) ->
    Args = [{'upstreams', [<<"amqp://localhost/%2f/upstream1">>,
                           <<"amqp://localhost/%2f/upstream2">>]}],
    XArgs = [{type, <<"headers">>},
             {arguments, Args}],
    http_put(Config, "/exchanges/%2f/myexchange", XArgs, [?CREATED, ?NO_CONTENT]),
    Definitions = http_get(Config, "/definitions", ?OK),
    http_delete(Config, "/exchanges/%2f/myexchange", ?NO_CONTENT),
    http_post(Config, "/definitions", Definitions, ?CREATED),
    Args = pget(arguments, http_get(Config, "/exchanges/%2f/myexchange", ?OK)),
    http_delete(Config, "/exchanges/%2f/myexchange", ?NO_CONTENT),
    passed.

queue_purge_test(Config) ->
    QArgs = [],
    http_put(Config, "/queues/%2f/myqueue", QArgs, [?CREATED, ?NO_CONTENT]),
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
    http_delete(Config, "/queues/%2f/myqueue/contents", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/badqueue/contents", ?NOT_FOUND),
    http_delete(Config, "/queues/%2f/exclusive/contents", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2f/exclusive", ?BAD_REQUEST),
    #'basic.get_empty'{} =
        amqp_channel:call(Ch, #'basic.get'{queue = <<"myqueue">>}),
    close_channel(Ch),
    close_connection(Conn),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

queue_actions_test(Config) ->
    http_put(Config, "/queues/%2f/q", [], [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/queues/%2f/q/actions", [{action, sync}], ?NO_CONTENT),
    http_post(Config, "/queues/%2f/q/actions", [{action, cancel_sync}], ?NO_CONTENT),
    http_post(Config, "/queues/%2f/q/actions", [{action, change_colour}], ?BAD_REQUEST),
    http_delete(Config, "/queues/%2f/q", ?NO_CONTENT),
    passed.

exclusive_consumer_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue     = QName,
                                                exclusive = true}, self()),
    timer:sleep(1000), %% Sadly we need to sleep to let the stats update
    http_get(Config, "/queues/%2f/"), %% Just check we don't blow up
    close_channel(Ch),
    close_connection(Conn),
    passed.


exclusive_queue_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{ queue = QName } =
	amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    timer:sleep(1000), %% Sadly we need to sleep to let the stats update
    Path = "/queues/%2f/" ++ mochiweb_util:quote_plus(QName),
    Queue = http_get(Config, Path),
    assert_item([{name,         QName},
		 {vhost,       <<"/">>},
		 {durable,     false},
		 {auto_delete, false},
		 {exclusive,   true},
		 {arguments,   []}], Queue),
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

    timer:sleep(1000), %% Sadly we need to sleep to let the stats update
    PageOfTwo = http_get(Config, "/connections?page=1&page_size=2", ?OK),
    ?assertEqual(3, proplists:get_value(total_count, PageOfTwo)),
    ?assertEqual(3, proplists:get_value(filtered_count, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(item_count, PageOfTwo)),
    ?assertEqual(1, proplists:get_value(page, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(page_size, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(page_count, PageOfTwo)),


    TwoOfTwo = http_get(Config, "/channels?page=2&page_size=2", ?OK),
    ?assertEqual(3, proplists:get_value(total_count, TwoOfTwo)),
    ?assertEqual(3, proplists:get_value(filtered_count, TwoOfTwo)),
    ?assertEqual(1, proplists:get_value(item_count, TwoOfTwo)),
    ?assertEqual(2, proplists:get_value(page, TwoOfTwo)),
    ?assertEqual(2, proplists:get_value(page_size, TwoOfTwo)),
    ?assertEqual(2, proplists:get_value(page_count, TwoOfTwo)),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    amqp_channel:close(Ch1),
    amqp_connection:close(Conn1),
    amqp_channel:close(Ch2),
    amqp_connection:close(Conn2),

    passed.

exchanges_pagination_test(Config) ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_get(Config, "/exchanges/vh1?page=1&page_size=2", ?OK),
    http_put(Config, "/exchanges/%2f/test0", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/exchanges/vh1/test1", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/exchanges/%2f/test2_reg", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/exchanges/vh1/reg_test3", QArgs, [?CREATED, ?NO_CONTENT]),
    PageOfTwo = http_get(Config, "/exchanges?page=1&page_size=2", ?OK),
    ?assertEqual(19, proplists:get_value(total_count, PageOfTwo)),
    ?assertEqual(19, proplists:get_value(filtered_count, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(item_count, PageOfTwo)),
    ?assertEqual(1, proplists:get_value(page, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(page_size, PageOfTwo)),
    ?assertEqual(10, proplists:get_value(page_count, PageOfTwo)),
    assert_list([[{name, <<"">>}, {vhost, <<"/">>}],
		 [{name, <<"amq.direct">>}, {vhost, <<"/">>}]
		], proplists:get_value(items, PageOfTwo)),

    ByName = http_get(Config, "/exchanges?page=1&page_size=2&name=reg", ?OK),
    ?assertEqual(19, proplists:get_value(total_count, ByName)),
    ?assertEqual(2, proplists:get_value(filtered_count, ByName)),
    ?assertEqual(2, proplists:get_value(item_count, ByName)),
    ?assertEqual(1, proplists:get_value(page, ByName)),
    ?assertEqual(2, proplists:get_value(page_size, ByName)),
    ?assertEqual(1, proplists:get_value(page_count, ByName)),
    assert_list([[{name, <<"test2_reg">>}, {vhost, <<"/">>}],
		 [{name, <<"reg_test3">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, ByName)),


    RegExByName = http_get(Config,
                           "/exchanges?page=1&page_size=2&name=^(?=^reg)&use_regex=true",
                           ?OK),
    ?assertEqual(19, proplists:get_value(total_count, RegExByName)),
    ?assertEqual(1, proplists:get_value(filtered_count, RegExByName)),
    ?assertEqual(1, proplists:get_value(item_count, RegExByName)),
    ?assertEqual(1, proplists:get_value(page, RegExByName)),
    ?assertEqual(2, proplists:get_value(page_size, RegExByName)),
    ?assertEqual(1, proplists:get_value(page_count, RegExByName)),
    assert_list([[{name, <<"reg_test3">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, RegExByName)),


    http_get(Config, "/exchanges?page=1000", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=-1", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=1&page_size=not_an_intger_value", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/exchanges?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/exchanges/%2f/test0", ?NO_CONTENT),
    http_delete(Config, "/exchanges/vh1/test1", ?NO_CONTENT),
    http_delete(Config, "/exchanges/%2f/test2_reg", ?NO_CONTENT),
    http_delete(Config, "/exchanges/vh1/reg_test3", ?NO_CONTENT),
    http_delete(Config, "/vhosts/vh1", ?NO_CONTENT),
    passed.

exchanges_pagination_permissions_test(Config) ->
    http_put(Config, "/users/admin",   [{password, <<"admin">>},
                                        {tags, <<"administrator">>}], [?CREATED, ?NO_CONTENT]),
    Perms = [{configure, <<".*">>},
	     {write,     <<".*">>},
	     {read,      <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/admin",   Perms, [?CREATED, ?NO_CONTENT]),
    QArgs = [],
    http_put(Config, "/exchanges/%2f/test0", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/exchanges/vh1/test1", QArgs, "admin","admin", [?CREATED, ?NO_CONTENT]),
    FirstPage = http_get(Config, "/exchanges?page=1&name=test1","admin","admin", ?OK),
    ?assertEqual(8, proplists:get_value(total_count, FirstPage)),
    ?assertEqual(1, proplists:get_value(item_count, FirstPage)),
    ?assertEqual(1, proplists:get_value(page, FirstPage)),
    ?assertEqual(100, proplists:get_value(page_size, FirstPage)),
    ?assertEqual(1, proplists:get_value(page_count, FirstPage)),
    assert_list([[{name, <<"test1">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, FirstPage)),
    http_delete(Config, "/exchanges/%2f/test0", ?NO_CONTENT),
    http_delete(Config, "/exchanges/vh1/test1","admin","admin", ?NO_CONTENT),
    http_delete(Config, "/users/admin", ?NO_CONTENT),
    passed.



queue_pagination_test(Config) ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),

    http_get(Config, "/queues/vh1?page=1&page_size=2", ?OK),

    http_put(Config, "/queues/%2f/test0", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/vh1/test1", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/test2_reg", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/vh1/reg_test3", QArgs, [?CREATED, ?NO_CONTENT]),
    PageOfTwo = http_get(Config, "/queues?page=1&page_size=2", ?OK),
    ?assertEqual(4, proplists:get_value(total_count, PageOfTwo)),
    ?assertEqual(4, proplists:get_value(filtered_count, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(item_count, PageOfTwo)),
    ?assertEqual(1, proplists:get_value(page, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(page_size, PageOfTwo)),
    ?assertEqual(2, proplists:get_value(page_count, PageOfTwo)),
    assert_list([[{name, <<"test0">>}, {vhost, <<"/">>}],
		 [{name, <<"test2_reg">>}, {vhost, <<"/">>}]
		], proplists:get_value(items, PageOfTwo)),

    SortedByName = http_get(Config, "/queues?sort=name&page=1&page_size=2", ?OK),
    ?assertEqual(4, proplists:get_value(total_count, SortedByName)),
    ?assertEqual(4, proplists:get_value(filtered_count, SortedByName)),
    ?assertEqual(2, proplists:get_value(item_count, SortedByName)),
    ?assertEqual(1, proplists:get_value(page, SortedByName)),
    ?assertEqual(2, proplists:get_value(page_size, SortedByName)),
    ?assertEqual(2, proplists:get_value(page_count, SortedByName)),
    assert_list([[{name, <<"reg_test3">>}, {vhost, <<"vh1">>}],
		 [{name, <<"test0">>}, {vhost, <<"/">>}]
		], proplists:get_value(items, SortedByName)),


    FirstPage = http_get(Config, "/queues?page=1", ?OK),
    ?assertEqual(4, proplists:get_value(total_count, FirstPage)),
    ?assertEqual(4, proplists:get_value(filtered_count, FirstPage)),
    ?assertEqual(4, proplists:get_value(item_count, FirstPage)),
    ?assertEqual(1, proplists:get_value(page, FirstPage)),
    ?assertEqual(100, proplists:get_value(page_size, FirstPage)),
    ?assertEqual(1, proplists:get_value(page_count, FirstPage)),
    assert_list([[{name, <<"test0">>}, {vhost, <<"/">>}],
		 [{name, <<"test1">>}, {vhost, <<"vh1">>}],
		 [{name, <<"test2_reg">>}, {vhost, <<"/">>}],
		 [{name, <<"reg_test3">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, FirstPage)),


    ReverseSortedByName = http_get(Config,
                                   "/queues?page=2&page_size=2&sort=name&sort_reverse=true",
                                   ?OK),
    ?assertEqual(4, proplists:get_value(total_count, ReverseSortedByName)),
    ?assertEqual(4, proplists:get_value(filtered_count, ReverseSortedByName)),
    ?assertEqual(2, proplists:get_value(item_count, ReverseSortedByName)),
    ?assertEqual(2, proplists:get_value(page, ReverseSortedByName)),
    ?assertEqual(2, proplists:get_value(page_size, ReverseSortedByName)),
    ?assertEqual(2, proplists:get_value(page_count, ReverseSortedByName)),
    assert_list([[{name, <<"test0">>}, {vhost, <<"/">>}],
		 [{name, <<"reg_test3">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, ReverseSortedByName)),


    ByName = http_get(Config, "/queues?page=1&page_size=2&name=reg", ?OK),
    ?assertEqual(4, proplists:get_value(total_count, ByName)),
    ?assertEqual(2, proplists:get_value(filtered_count, ByName)),
    ?assertEqual(2, proplists:get_value(item_count, ByName)),
    ?assertEqual(1, proplists:get_value(page, ByName)),
    ?assertEqual(2, proplists:get_value(page_size, ByName)),
    ?assertEqual(1, proplists:get_value(page_count, ByName)),
    assert_list([[{name, <<"test2_reg">>}, {vhost, <<"/">>}],
		 [{name, <<"reg_test3">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, ByName)),

    RegExByName = http_get(Config,
                           "/queues?page=1&page_size=2&name=^(?=^reg)&use_regex=true",
                           ?OK),
    ?assertEqual(4, proplists:get_value(total_count, RegExByName)),
    ?assertEqual(1, proplists:get_value(filtered_count, RegExByName)),
    ?assertEqual(1, proplists:get_value(item_count, RegExByName)),
    ?assertEqual(1, proplists:get_value(page, RegExByName)),
    ?assertEqual(2, proplists:get_value(page_size, RegExByName)),
    ?assertEqual(1, proplists:get_value(page_count, RegExByName)),
    assert_list([[{name, <<"reg_test3">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, RegExByName)),


    http_get(Config, "/queues?page=1000", ?BAD_REQUEST),
    http_get(Config, "/queues?page=-1", ?BAD_REQUEST),
    http_get(Config, "/queues?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=not_an_intger_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/queues?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2f/test0", ?NO_CONTENT),
    http_delete(Config, "/queues/vh1/test1", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/test2_reg", ?NO_CONTENT),
    http_delete(Config, "/queues/vh1/reg_test3", ?NO_CONTENT),
    http_delete(Config, "/vhosts/vh1", ?NO_CONTENT),
    passed.

queues_pagination_permissions_test(Config) ->
    http_put(Config, "/users/admin",   [{password, <<"admin">>},
                                        {tags, <<"administrator">>}], [?CREATED, ?NO_CONTENT]),
    Perms = [{configure, <<".*">>},
	     {write,     <<".*">>},
	     {read,      <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/admin",   Perms, [?CREATED, ?NO_CONTENT]),
    QArgs = [],
    http_put(Config, "/queues/%2f/test0", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/vh1/test1", QArgs, "admin","admin", [?CREATED, ?NO_CONTENT]),
    FirstPage = http_get(Config, "/queues?page=1","admin","admin", ?OK),
    ?assertEqual(1, proplists:get_value(total_count, FirstPage)),
    ?assertEqual(1, proplists:get_value(item_count, FirstPage)),
    ?assertEqual(1, proplists:get_value(page, FirstPage)),
    ?assertEqual(100, proplists:get_value(page_size, FirstPage)),
    ?assertEqual(1, proplists:get_value(page_count, FirstPage)),
    assert_list([[{name, <<"test1">>}, {vhost, <<"vh1">>}]
		], proplists:get_value(items, FirstPage)),
    http_delete(Config, "/queues/%2f/test0", ?NO_CONTENT),
    http_delete(Config, "/queues/vh1/test1","admin","admin", ?NO_CONTENT),
    http_delete(Config, "/users/admin", ?NO_CONTENT),
    passed.

samples_range_test(Config) ->
    
    {Conn, Ch} = open_connection_and_channel(Config),

    %% Channels.

    [ConnInfo | _] = http_get(Config, "/channels?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/channels?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    {_, ConnDetails} = lists:keyfind(connection_details, 1, ConnInfo),
    {_, ConnName0} = lists:keyfind(name, 1, ConnDetails),
    ConnName = http_uri:encode(binary_to_list(ConnName0)),
    ChanName = ConnName ++ http_uri:encode(" (1)"),

    http_get(Config, "/channels/" ++ ChanName ++ "?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/channels/" ++ ChanName ++ "?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_get(Config, "/vhosts/%2f/channels?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/vhosts/%2f/channels?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Connections.

    http_get(Config, "/connections?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/connections?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_get(Config, "/connections/" ++ ConnName ++ "?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/connections/" ++ ConnName ++ "?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_get(Config, "/connections/" ++ ConnName ++ "/channels?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/connections/" ++ ConnName ++ "/channels?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_get(Config, "/vhosts/%2f/connections?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/vhosts/%2f/connections?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),

    %% Exchanges.

    http_get(Config, "/exchanges?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/exchanges?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_get(Config, "/exchanges/%2f/amq.direct?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/exchanges/%2f/amq.direct?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Nodes.

    http_get(Config, "/nodes?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/nodes?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Overview.

    http_get(Config, "/overview?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/overview?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    %% Queues.

    http_put(Config, "/queues/%2f/test0", [], [?CREATED, ?NO_CONTENT]),

    http_get(Config, "/queues/%2f?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/queues/%2f?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
    http_get(Config, "/queues/%2f/test0?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/queues/%2f/test0?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_delete(Config, "/queues/%2f/test0", ?NO_CONTENT),

    %% Vhosts.

    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),

    http_get(Config, "/vhosts?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/vhosts?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),
    http_get(Config, "/vhosts/vh1?lengths_age=60&lengths_incr=1", ?OK),
    http_get(Config, "/vhosts/vh1?lengths_age=6000&lengths_incr=1", ?BAD_REQUEST),

    http_delete(Config, "/vhosts/vh1", ?NO_CONTENT),

    passed.

sorting_test(Config) ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/test0", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/vh1/test1", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/test2", QArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/vh1/test3", QArgs, [?CREATED, ?NO_CONTENT]),
    assert_list([[{name, <<"test0">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test3">>}]], http_get(Config, "/queues", ?OK)),
    assert_list([[{name, <<"test0">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test3">>}]], http_get(Config, "/queues?sort=name", ?OK)),
    assert_list([[{name, <<"test0">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test3">>}]], http_get(Config, "/queues?sort=vhost", ?OK)),
    assert_list([[{name, <<"test3">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test0">>}]], http_get(Config, "/queues?sort_reverse=true", ?OK)),
    assert_list([[{name, <<"test3">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test0">>}]], http_get(Config, "/queues?sort=name&sort_reverse=true", ?OK)),
    assert_list([[{name, <<"test3">>}],
                 [{name, <<"test1">>}],
                 [{name, <<"test2">>}],
                 [{name, <<"test0">>}]], http_get(Config, "/queues?sort=vhost&sort_reverse=true", ?OK)),
    %% Rather poor but at least test it doesn't blow up with dots
    http_get(Config, "/queues?sort=owner_pid_details.name", ?OK),
    http_delete(Config, "/queues/%2f/test0", ?NO_CONTENT),
    http_delete(Config, "/queues/vh1/test1", ?NO_CONTENT),
    http_delete(Config, "/queues/%2f/test2", ?NO_CONTENT),
    http_delete(Config, "/queues/vh1/test3", ?NO_CONTENT),
    http_delete(Config, "/vhosts/vh1", ?NO_CONTENT),
    passed.

format_output_test(Config) ->
    QArgs = [],
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/test0", QArgs, [?CREATED, ?NO_CONTENT]),
    assert_list([[{name, <<"test0">>},
		  {consumer_utilisation, null},
		  {exclusive_consumer_tag, null},
		  {recoverable_slaves, null}]], http_get(Config, "/queues", ?OK)),
    http_delete(Config, "/queues/%2f/test0", ?NO_CONTENT),
    http_delete(Config, "/vhosts/vh1", ?NO_CONTENT),
    passed.

columns_test(Config) ->
    http_put(Config, "/queues/%2f/test", [{arguments, [{<<"foo">>, <<"bar">>}]}],
             [?CREATED, ?NO_CONTENT]),
    [List] = http_get(Config, "/queues?columns=arguments.foo,name", ?OK),
    [{arguments, [{foo, <<"bar">>}]}, {name, <<"test">>}] = lists:sort(List),
    [{arguments, [{foo, <<"bar">>}]}, {name, <<"test">>}] =
        lists:sort(http_get(Config, "/queues/%2f/test?columns=arguments.foo,name", ?OK)),
    http_delete(Config, "/queues/%2f/test", ?NO_CONTENT),
    passed.

get_test(Config) ->
    %% Real world example...
    Headers = [{<<"x-forwarding">>, array,
                [{table,
                  [{<<"uri">>, longstr,
                    <<"amqp://localhost/%2f/upstream">>}]}]}],
    http_put(Config, "/queues/%2f/myqueue", [], [?CREATED, ?NO_CONTENT]),
    {Conn, Ch} = open_connection_and_channel(Config),
    Publish = fun (Payload) ->
                      amqp_channel:cast(
                        Ch, #'basic.publish'{exchange = <<>>,
                                             routing_key = <<"myqueue">>},
                        #amqp_msg{props = #'P_basic'{headers = Headers},
                                  payload = Payload})
              end,
    Publish(<<"1aaa">>),
    Publish(<<"2aaa">>),
    Publish(<<"3aaa">>),
    amqp_channel:close(Ch),
    close_connection(Conn),
    [Msg] = http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  false},
                                                          {count,    1},
                                                          {encoding, auto},
                                                          {truncate, 1}], ?OK),
    false         = pget(redelivered, Msg),
    <<>>          = pget(exchange,    Msg),
    <<"myqueue">> = pget(routing_key, Msg),
    <<"1">>       = pget(payload,     Msg),
    [{'x-forwarding',
      [[{uri,<<"amqp://localhost/%2f/upstream">>}]]}] =
        pget(headers, pget(properties, Msg)),

    [M2, M3] = http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  true},
                                                             {count,    5},
                                                             {encoding, auto}], ?OK),
    <<"2aaa">> = pget(payload, M2),
    <<"3aaa">> = pget(payload, M3),
    2 = length(http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  false},
                                                             {count,    5},
                                                             {encoding, auto}], ?OK)),
    [] = http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  false},
                                                       {count,    5},
                                                       {encoding, auto}], ?OK),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

get_fail_test(Config) ->
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                                       {tags, <<"management">>}], ?NO_CONTENT),
    http_put(Config, "/queues/%2f/myqueue", [], [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/queues/%2f/myqueue/get",
              [{requeue,  false},
               {count,    1},
               {encoding, auto}], "myuser", "password", ?NOT_AUTHORISED),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    http_delete(Config, "/users/myuser", ?NO_CONTENT),
    passed.

publish_test(Config) ->
    Headers = [{'x-forwarding', [[{uri,<<"amqp://localhost/%2f/upstream">>}]]}],
    Msg = msg(<<"myqueue">>, Headers, <<"Hello world">>),
    http_put(Config, "/queues/%2f/myqueue", [], [?CREATED, ?NO_CONTENT]),
    ?assertEqual([{routed, true}],
                 http_post(Config, "/exchanges/%2f/amq.default/publish", Msg, ?OK)),
    [Msg2] = http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  false},
                                                           {count,    1},
                                                           {encoding, auto}], ?OK),
    assert_item(Msg, Msg2),
    http_post(Config, "/exchanges/%2f/amq.default/publish", Msg2, ?OK),
    [Msg3] = http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  false},
                                                           {count,    1},
                                                           {encoding, auto}], ?OK),
    assert_item(Msg, Msg3),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

publish_accept_json_test(Config) ->
    Headers = [{'x-forwarding', [[{uri, <<"amqp://localhost/%2f/upstream">>}]]}],
    Msg = msg(<<"myqueue">>, Headers, <<"Hello world">>),
    http_put(Config, "/queues/%2f/myqueue", [], [?CREATED, ?NO_CONTENT]),
    ?assertEqual([{routed, true}],
		 http_post_accept_json(Config, "/exchanges/%2f/amq.default/publish",
				       Msg, ?OK)),

    [Msg2] = http_post_accept_json(Config, "/queues/%2f/myqueue/get",
				   [{requeue, false},
				    {count, 1},
				    {encoding, auto}], ?OK),
    assert_item(Msg, Msg2),
    http_post_accept_json(Config, "/exchanges/%2f/amq.default/publish", Msg2, ?OK),
    [Msg3] = http_post_accept_json(Config, "/queues/%2f/myqueue/get",
				   [{requeue, false},
				    {count, 1},
				    {encoding, auto}], ?OK),
    assert_item(Msg, Msg3),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

publish_fail_test(Config) ->
    Msg = msg(<<"myqueue">>, [], <<"Hello world">>),
    http_put(Config, "/queues/%2f/myqueue", [], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/myuser", [{password, <<"password">>},
                                       {tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/exchanges/%2f/amq.default/publish", Msg, "myuser", "password",
              ?NOT_AUTHORISED),
    Msg2 = [{exchange,         <<"">>},
            {routing_key,      <<"myqueue">>},
            {properties,       [{user_id, <<"foo">>}]},
            {payload,          <<"Hello world">>},
            {payload_encoding, <<"string">>}],
    http_post(Config, "/exchanges/%2f/amq.default/publish", Msg2, ?BAD_REQUEST),
    Msg3 = [{exchange,         <<"">>},
            {routing_key,      <<"myqueue">>},
            {properties,       []},
            {payload,          [<<"not a string">>]},
            {payload_encoding, <<"string">>}],
    http_post(Config, "/exchanges/%2f/amq.default/publish", Msg3, ?BAD_REQUEST),
    MsgTemplate = [{exchange,         <<"">>},
                   {routing_key,      <<"myqueue">>},
                   {payload,          <<"Hello world">>},
                   {payload_encoding, <<"string">>}],
    [http_post(Config, "/exchanges/%2f/amq.default/publish",
               [{properties, [BadProp]} | MsgTemplate], ?BAD_REQUEST)
     || BadProp <- [{priority,   <<"really high">>},
                    {timestamp,  <<"recently">>},
                    {expiration, 1234}]],
    http_delete(Config, "/users/myuser", ?NO_CONTENT),
    passed.

publish_base64_test(Config) ->
    Msg     = msg(<<"myqueue">>, [], <<"YWJjZA==">>, <<"base64">>),
    BadMsg1 = msg(<<"myqueue">>, [], <<"flibble">>,  <<"base64">>),
    BadMsg2 = msg(<<"myqueue">>, [], <<"YWJjZA==">>, <<"base99">>),
    http_put(Config, "/queues/%2f/myqueue", [], [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/exchanges/%2f/amq.default/publish", Msg, ?OK),
    http_post(Config, "/exchanges/%2f/amq.default/publish", BadMsg1, ?BAD_REQUEST),
    http_post(Config, "/exchanges/%2f/amq.default/publish", BadMsg2, ?BAD_REQUEST),
    [Msg2] = http_post(Config, "/queues/%2f/myqueue/get", [{requeue,  false},
                                                           {count,    1},
                                                           {encoding, auto}], ?OK),
    ?assertEqual(<<"abcd">>, pget(payload, Msg2)),
    http_delete(Config, "/queues/%2f/myqueue", ?NO_CONTENT),
    passed.

publish_unrouted_test(Config) ->
    Msg = msg(<<"hmmm">>, [], <<"Hello world">>),
    ?assertEqual([{routed, false}],
                 http_post(Config, "/exchanges/%2f/amq.default/publish", Msg, ?OK)).

if_empty_unused_test(Config) ->
    http_put(Config, "/exchanges/%2f/test", [], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/queues/%2f/test", [], [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/bindings/%2f/e/test/q/test", [], [?CREATED, ?NO_CONTENT]),
    http_post(Config, "/exchanges/%2f/amq.default/publish",
              msg(<<"test">>, [], <<"Hello world">>), ?OK),
    http_delete(Config, "/queues/%2f/test?if-empty=true", ?BAD_REQUEST),
    http_delete(Config, "/exchanges/%2f/test?if-unused=true", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2f/test/contents", ?NO_CONTENT),

    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = <<"test">> }, self()),
    http_delete(Config, "/queues/%2f/test?if-unused=true", ?BAD_REQUEST),
    amqp_connection:close(Conn),

    http_delete(Config, "/queues/%2f/test?if-empty=true", ?NO_CONTENT),
    http_delete(Config, "/exchanges/%2f/test?if-unused=true", ?NO_CONTENT),
    passed.

parameters_test(Config) ->
    register_parameters_and_policy_validator(Config),

    http_put(Config, "/parameters/test/%2f/good", [{value, <<"ignore">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/parameters/test/%2f/maybe", [{value, <<"good">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/parameters/test/%2f/maybe", [{value, <<"bad">>}], ?BAD_REQUEST),
    http_put(Config, "/parameters/test/%2f/bad", [{value, <<"good">>}], ?BAD_REQUEST),
    http_put(Config, "/parameters/test/um/good", [{value, <<"ignore">>}], ?NOT_FOUND),

    Good = [{vhost,     <<"/">>},
            {component, <<"test">>},
            {name,      <<"good">>},
            {value,     <<"ignore">>}],
    Maybe = [{vhost,     <<"/">>},
             {component, <<"test">>},
             {name,      <<"maybe">>},
             {value,     <<"good">>}],
    List = [Good, Maybe],

    assert_list(List, http_get(Config, "/parameters")),
    assert_list(List, http_get(Config, "/parameters/test")),
    assert_list(List, http_get(Config, "/parameters/test/%2f")),
    assert_list([],   http_get(Config, "/parameters/oops")),
    http_get(Config, "/parameters/test/oops", ?NOT_FOUND),

    assert_item(Good,  http_get(Config, "/parameters/test/%2f/good", ?OK)),
    assert_item(Maybe, http_get(Config, "/parameters/test/%2f/maybe", ?OK)),

    http_delete(Config, "/parameters/test/%2f/good", ?NO_CONTENT),
    http_delete(Config, "/parameters/test/%2f/maybe", ?NO_CONTENT),
    http_delete(Config, "/parameters/test/%2f/bad", ?NOT_FOUND),

    0 = length(http_get(Config, "/parameters")),
    0 = length(http_get(Config, "/parameters/test")),
    0 = length(http_get(Config, "/parameters/test/%2f")),
    unregister_parameters_and_policy_validator(Config),
    passed.

policy_test(Config) ->
    register_parameters_and_policy_validator(Config),
    PolicyPos  = [{vhost,      <<"/">>},
                  {name,       <<"policy_pos">>},
                  {pattern,    <<".*">>},
                  {definition, [{testpos,[1,2,3]}]},
                  {priority,   10}],
    PolicyEven = [{vhost,      <<"/">>},
                  {name,       <<"policy_even">>},
                  {pattern,    <<".*">>},
                  {definition, [{testeven,[1,2,3,4]}]},
                  {priority,   10}],
    http_put(Config,
             "/policies/%2f/policy_pos",
             lists:keydelete(key, 1, PolicyPos),
             [?CREATED, ?NO_CONTENT]),
    http_put(Config,
             "/policies/%2f/policy_even",
             lists:keydelete(key, 1, PolicyEven),
             [?CREATED, ?NO_CONTENT]),
    assert_item(PolicyPos,  http_get(Config, "/policies/%2f/policy_pos",  ?OK)),
    assert_item(PolicyEven, http_get(Config, "/policies/%2f/policy_even", ?OK)),
    List = [PolicyPos, PolicyEven],
    assert_list(List, http_get(Config, "/policies",     ?OK)),
    assert_list(List, http_get(Config, "/policies/%2f", ?OK)),

    http_delete(Config, "/policies/%2f/policy_pos", ?NO_CONTENT),
    http_delete(Config, "/policies/%2f/policy_even", ?NO_CONTENT),
    0 = length(http_get(Config, "/policies")),
    0 = length(http_get(Config, "/policies/%2f")),
    unregister_parameters_and_policy_validator(Config),
    passed.

policy_permissions_test(Config) ->
    register_parameters_and_policy_validator(Config),

    http_put(Config, "/users/admin",  [{password, <<"admin">>},
                                       {tags, <<"administrator">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/mon",    [{password, <<"mon">>},
                                       {tags, <<"monitoring">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/policy", [{password, <<"policy">>},
                                       {tags, <<"policymaker">>}], [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/users/mgmt",   [{password, <<"mgmt">>},
                                       {tags, <<"management">>}], [?CREATED, ?NO_CONTENT]),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put(Config, "/vhosts/v", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/v/admin",  Perms, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/v/mon",    Perms, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/v/policy", Perms, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/v/mgmt",   Perms, [?CREATED, ?NO_CONTENT]),

    Policy = [{pattern,    <<".*">>},
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
    Param = [{value, <<"">>}],

    http_put(Config, "/policies/%2f/HA", Policy, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/parameters/test/%2f/good", Param, [?CREATED, ?NO_CONTENT]),

    Pos = fun (U) ->
                  Expected = case U of "admin" -> [?CREATED, ?NO_CONTENT]; _ -> ?NO_CONTENT end,
                  http_put(Config, "/policies/v/HA",        Policy, U, U, Expected),
                  http_put(Config,
                           "/parameters/test/v/good",       Param, U, U, ?NO_CONTENT),
                  1 = length(http_get(Config, "/policies",          U, U, ?OK)),
                  1 = length(http_get(Config, "/parameters/test",   U, U, ?OK)),
                  1 = length(http_get(Config, "/parameters",        U, U, ?OK)),
                  1 = length(http_get(Config, "/policies/v",        U, U, ?OK)),
                  1 = length(http_get(Config, "/parameters/test/v", U, U, ?OK)),
                  http_get(Config, "/policies/v/HA",                U, U, ?OK),
                  http_get(Config, "/parameters/test/v/good",       U, U, ?OK)
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
                http_put(Config, "/policies/%2f/HA",  Policy, U, U, ?NOT_AUTHORISED),
                http_put(Config,
                         "/parameters/test/%2f/good", Param, U, U, ?NOT_AUTHORISED),
                http_get(Config, "/policies/%2f/HA",          U, U, ?NOT_AUTHORISED),
                http_get(Config, "/parameters/test/%2f/good", U, U, ?NOT_AUTHORISED)
        end,

    [Neg(U) || U <- ["mon", "mgmt"]],
    [Pos(U) || U <- ["admin", "policy"]],
    [AlwaysNeg(U) || U <- ["mon", "mgmt", "admin", "policy"]],

    %% This one is deliberately different between admin and policymaker.
    http_put(Config, "/parameters/test/v/admin", Param, "admin", "admin", [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/parameters/test/v/admin", Param, "policy", "policy",
             ?BAD_REQUEST),

    http_delete(Config, "/vhosts/v", ?NO_CONTENT),
    http_delete(Config, "/users/admin", ?NO_CONTENT),
    http_delete(Config, "/users/mon", ?NO_CONTENT),
    http_delete(Config, "/users/policy", ?NO_CONTENT),
    http_delete(Config, "/users/mgmt", ?NO_CONTENT),
    http_delete(Config, "/policies/%2f/HA", ?NO_CONTENT),

    unregister_parameters_and_policy_validator(Config),
    passed.

issue67_test(Config)->
    {ok, {{_, 401, _}, Headers, _}} = req(Config, get, "/queues",
                                          [auth_header("user_no_access", "password_no_access")]),
    ?assertEqual("application/json",
                 proplists:get_value("content-type",Headers)),
    passed.

extensions_test(Config) ->
    [[{javascript,<<"dispatcher.js">>}]] = http_get(Config, "/extensions", ?OK),
    passed.

cors_test(Config) ->
    %% With CORS disabled. No header should be received.
    {ok, {_, HdNoCORS, _}} = req(Config, get, "/overview", [auth_header("guest", "guest")]),
    false = lists:keymember("access-control-allow-origin", 1, HdNoCORS),
    %% The Vary header should include "Origin" regardless of CORS configuration.
    {_, "Accept-Encoding, origin"} = lists:keyfind("vary", 1, HdNoCORS),
    %% Enable CORS.
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_management, cors_allow_origins, ["http://rabbitmq.com"]]),
    %% We should only receive allow-origin and allow-credentials from GET.
    {ok, {_, HdGetCORS, _}} = req(Config, get, "/overview",
                                  [{"origin", "http://rabbitmq.com"}, auth_header("guest", "guest")]),
    true = lists:keymember("access-control-allow-origin", 1, HdGetCORS),
    true = lists:keymember("access-control-allow-credentials", 1, HdGetCORS),
    false = lists:keymember("access-control-expose-headers", 1, HdGetCORS),
    false = lists:keymember("access-control-max-age", 1, HdGetCORS),
    false = lists:keymember("access-control-allow-methods", 1, HdGetCORS),
    false = lists:keymember("access-control-allow-headers", 1, HdGetCORS),
    %% We should receive allow-origin, allow-credentials and allow-methods from OPTIONS.
    {ok, {_, HdOptionsCORS, _}} = req(Config, options, "/overview",
                                      [{"origin", "http://rabbitmq.com"}, auth_header("guest", "guest")]),
    true = lists:keymember("access-control-allow-origin", 1, HdOptionsCORS),
    true = lists:keymember("access-control-allow-credentials", 1, HdOptionsCORS),
    false = lists:keymember("access-control-expose-headers", 1, HdOptionsCORS),
    true = lists:keymember("access-control-max-age", 1, HdOptionsCORS),
    true = lists:keymember("access-control-allow-methods", 1, HdOptionsCORS),
    false = lists:keymember("access-control-allow-headers", 1, HdOptionsCORS),
    %% We should receive allow-headers when request-headers is sent.
    {ok, {_, HdAllowHeadersCORS, _}} = req(Config, options, "/overview",
                                           [{"origin", "http://rabbitmq.com"},
                                            auth_header("guest", "guest"),
                                            {"access-control-request-headers", "x-piggy-bank"}]),
    {_, "x-piggy-bank"} = lists:keyfind("access-control-allow-headers", 1, HdAllowHeadersCORS),
    %% Disable preflight request caching.
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_management, cors_max_age, undefined]),
    %% We shouldn't receive max-age anymore.
    {ok, {_, HdNoMaxAgeCORS, _}} = req(Config, options, "/overview",
                                       [{"origin", "http://rabbitmq.com"}, auth_header("guest", "guest")]),
    false = lists:keymember("access-control-max-age", 1, HdNoMaxAgeCORS),
    %% Disable CORS again.
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_management, cors_allow_origins, []]),
    passed.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

msg(Key, Headers, Body) ->
    msg(Key, Headers, Body, <<"string">>).

msg(Key, Headers, Body, Enc) ->
    [{exchange,         <<"">>},
     {routing_key,      Key},
     {properties,       [{delivery_mode, 2},
                         {headers,       Headers}]},
     {payload,          Body},
     {payload_encoding, Enc}].

local_port(Conn) ->
    [{sock, Sock}] = amqp_connection:info(Conn, [sock]),
    {ok, Port} = inet:port(Sock),
    Port.

spawn_invalid(_Config, 0) ->
    ok;
spawn_invalid(Config, N) ->
    Self = self(),
    spawn(fun() ->
                  timer:sleep(rand_compat:uniform(250)),
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
