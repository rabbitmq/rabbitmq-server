%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_nodes_version_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_broker_helpers, [rpc/4]).
-import(rabbit_ct_helpers, [eventually/3]).
-import(rabbit_mgmt_test_util, [http_get/2, http_get/3]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     {group, single_node}
    ].

groups() ->
    [
     {single_node, [], [
                        nodes_list_includes_version_fields,
                        version_fields_match_expected_values
                       ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    inets:stop(),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
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
%% Test cases
%% -------------------------------------------------------------------

nodes_list_includes_version_fields(Config) ->
    eventually(
      ?_assertMatch(
         [#{rabbitmq_version := _, erlang_version := _, erlang_full_version := _}],
         http_get(Config, "/nodes")),
      1000, 30),
    passed.

version_fields_match_expected_values(Config) ->
    ExpectedRmqVersion = list_to_binary(
        rpc(Config, rabbit, base_product_version, [])),
    ExpectedErlangVersion = list_to_binary(
        rpc(Config, rabbit_misc, otp_release, [])),
    ExpectedErlangFullVersion = list_to_binary(
        rpc(Config, rabbit_misc, otp_system_version, [])),

    [Node] = http_get(Config, "/nodes"),
    ?assertEqual(ExpectedRmqVersion, maps:get(rabbitmq_version, Node)),
    ?assertEqual(ExpectedErlangVersion, maps:get(erlang_version, Node)),
    ?assertEqual(ExpectedErlangFullVersion, maps:get(erlang_full_version, Node)),

    Path = "/nodes/" ++ binary_to_list(maps:get(name, Node)),
    NodeInfo = http_get(Config, Path, ?OK),
    ?assertEqual(ExpectedRmqVersion, maps:get(rabbitmq_version, NodeInfo)),
    ?assertEqual(ExpectedErlangVersion, maps:get(erlang_version, NodeInfo)),
    ?assertEqual(ExpectedErlangFullVersion, maps:get(erlang_full_version, NodeInfo)),
    passed.
