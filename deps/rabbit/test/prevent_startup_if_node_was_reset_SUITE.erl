%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Test suite for the prevent_startup_if_node_was_reset feature.
%% This feature helps detect potential data loss scenarios by maintaining
%% a marker file to track if a node has been initialized before.

-module(prevent_startup_if_node_was_reset_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         prevent_startup_if_node_was_reset_disabled/1,
         prevent_startup_if_node_was_reset_enabled/1
        ]).

all() ->
    [
        {group, single_node}
    ].

groups() ->
    [
        {single_node, [], [
            prevent_startup_if_node_was_reset_disabled,
            prevent_startup_if_node_was_reset_enabled
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Groupname, Config) ->
    Config0 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, false},
        {rmq_nodename_suffix, Groupname},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(
        Config0,
        rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()
    ).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
            rabbit_ct_broker_helpers:teardown_steps()
    ).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

prevent_startup_if_node_was_reset_disabled(Config) ->
    % When feature is disabled (default), node should start normally
    DataDir = rabbit_ct_broker_helpers:get_node_config(Config, 0, data_dir),
    MarkerFile = filename:join(DataDir, "node_initialized.marker"),
    % Setting is disabled so no marker file should be present
    ?assertNot(filelib:is_file(MarkerFile)),

    % Restarting the node should work fine
    ok = stop_app(Config),
    set_env(Config, false),
    ok = start_app(Config),
    % Still no marker file
    ?assertNot(filelib:is_file(MarkerFile)),
    ok.

prevent_startup_if_node_was_reset_enabled(Config) ->
    DataDir = rabbit_ct_broker_helpers:get_node_config(Config, 0, data_dir),
    MarkerFile = filename:join(DataDir, "node_initialized.marker"),

    ok = stop_app(Config),
    set_env(Config, true),
    ok = start_app(Config),
    % Setting is enabled so marker file should be present after initial startup
    ?assert(filelib:is_file(MarkerFile)),

    % Restarting the node should be fine, as there is a marker file
    % and corresponding schema data (consistent state)

    ok = stop_app(Config),
    ok = start_app(Config),

    SchemaFile = schema_file(Config),

    ?assert(filelib:is_file(MarkerFile)),

    % Stop the node and remove the present schema to simulate data loss
    ok = stop_app(Config),
    file:delete(SchemaFile),
    % Node should fail to start because marker exists but schema is missing,
    % indicating potential data loss or corruption
    ?assertMatch(
        {error, 69, _},
        start_app(Config)
    ),
    ok.

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

stop_app(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, ["stop_app"]) of
        {ok, _} -> ok;
        Error -> Error
    end.

start_app(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, ["start_app"]) of
        {ok, _} -> ok;
        Error -> Error
    end.

schema_file(Config) ->
    DataDir = rabbit_ct_broker_helpers:get_node_config(Config, 0, data_dir),
    NodeName = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    filename:join([DataDir, "coordination", NodeName, "names.dets"]).

set_env(Config, Bool) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = rpc:call(Node, application, set_env, [rabbit, prevent_startup_if_node_was_reset, Bool]).
