%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_http_init_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_broker_helpers, [rpc/4]).
-import(rabbit_mgmt_test_util, [http_get/2, http_get/5, http_put/4, http_delete/3]).

-compile([export_all, nowarn_export_all]).

all() ->
    [
        unauthorized_test,
        init_settings_test,
        init_monitor_nodes_test,
        init_monitor_vhosts_test,
        invalid_range_test
    ].

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    http_put(Config, "/users/test_user", [{password, <<"test_user">>}, {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/test_monitor", [{password, <<"test_monitor">>}, {tags, <<"monitoring">>}], {group, '2xx'}),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    http_delete(Config, "/users/test_user", {group, '2xx'}),
    http_delete(Config, "/users/test_monitor", {group, '2xx'}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

unauthorized_test(Config) ->
    Headers = [rabbit_mgmt_test_util:auth_header("unknown_user", "bad_password")],
    {ok, {{_Version, 401, Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(Config, 0, get, "/init", Headers),
    ?assertEqual("Unauthorized", Reason),

    Decoded = rabbit_json:decode(Body),
    ?assertEqual(<<"not_authorized">>, maps:get(<<"error">>, Decoded)),
    ?assertEqual(<<"Not_Authorized">>, maps:get(<<"reason">>, Decoded)),
    passed.

init_settings_test(Config) ->
    Headers = [rabbit_mgmt_test_util:auth_header("test_user", "test_user")],
    {ok, {{_Version, 200, _Reason}, ResponseHeaders, Body}} = rabbit_mgmt_test_util:req(Config, 0, get, "/init", Headers),

    ContentType = proplists:get_value("content-type", ResponseHeaders),
    ?assertEqual("application/json", [C || C <- ContentType, C =/= $\s]),

    CacheControl = proplists:get_value("cache-control", ResponseHeaders),
    ?assertMatch({match, _}, re:run(list_to_binary(CacheControl), <<"no-cache">>)),
    ?assertMatch({match, _}, re:run(list_to_binary(CacheControl), <<"no-store">>)),

    Pragma = proplists:get_value("pragma", ResponseHeaders),
    ?assertEqual("no-cache", Pragma),

    Expires = proplists:get_value("expires", ResponseHeaders),
    ?assertEqual("0", Expires),

    JsonBody = rabbit_json:decode(Body),

    Settings = maps:get(<<"settings">>, JsonBody),
    ?assertMatch(#{<<"cluster_name">> := _}, Settings),

    ?assertMatch(#{<<"product_info">> := _}, Settings),
    ?assertMatch(#{<<"definitions">> := _}, Settings),

    %% test_user has no permissions on any virtual host, so the list is empty.
    Vhosts = maps:get(<<"vhosts">>, JsonBody),
    ?assert(is_list(Vhosts)),

    Nodes = maps:get(<<"nodes">>, JsonBody),
    ?assertEqual(null, Nodes),
    passed.

init_monitor_nodes_test(Config) ->
    Headers = [rabbit_mgmt_test_util:auth_header("test_monitor", "test_monitor")],
    {ok, {{_Version, 200, _Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(Config, 0, get, "/init", Headers),

    JsonBody = rabbit_json:decode(Body),

    Nodes = maps:get(<<"nodes">>, JsonBody),
    ?assert(is_list(Nodes)),
    passed.

init_monitor_vhosts_test(Config) ->
    %% The UI's virtual host selector reads .name off each entry, so a list
    %% of bare names would not do.
    Headers = [rabbit_mgmt_test_util:auth_header("test_monitor", "test_monitor")],
    {ok, {{_Version, 200, _Reason}, _Headers, Body}} = rabbit_mgmt_test_util:req(Config, 0, get, "/init", Headers),

    JsonBody = rabbit_json:decode(Body),
    Vhosts = maps:get(<<"vhosts">>, JsonBody),

    ?assertNotEqual([], Vhosts),
    [?assertMatch(#{<<"name">> := _}, V) || V <- Vhosts],
    ?assert(lists:any(fun(V) -> maps:get(<<"name">>, V) =:= <<"/">> end, Vhosts)),
    passed.

invalid_range_test(Config) ->
    http_get(Config, "/init?msg_rates_age=60&msg_rates_incr=0", "test_user", "test_user", 400),
    passed.
