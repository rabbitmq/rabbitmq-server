%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_amqp091_server_properties_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 5000).
-define(TIMEOUT, 30000).
-define(TIMEOUT_CHANNEL_EXCEPTION, 5000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          configurable_server_properties
       ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            ClusterSize = 2,
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize}
              ]),
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps());
        false ->
            rabbit_ct_helpers:run_steps(Config, [])
    end.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

init_per_testcase(Testcase, Config) ->
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    Q = rabbit_data_coercion:to_binary(io_lib:format("~p_~p", [Group, Testcase])),
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, Q}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

configurable_server_properties(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, configurable_server_properties1, [Config]).

configurable_server_properties1(_Config) ->
    %% List of the names of the built-in properties do we expect to find
    BuiltInPropNames = [<<"product">>, <<"version">>, <<"platform">>,
                        <<"copyright">>, <<"information">>],

    Protocol = rabbit_framing_amqp_0_9_1,

    %% Verify that the built-in properties are initially present
    ActualPropNames = [Key || {Key, longstr, _} <-
                                  rabbit_reader:server_properties(Protocol)],
    true = lists:all(fun (X) -> lists:member(X, ActualPropNames) end,
                     BuiltInPropNames),

    %% Get the initial server properties configured in the environment
    {ok, ServerProperties} = application:get_env(rabbit, server_properties),

    %% Helper functions
    ConsProp = fun (X) -> application:set_env(rabbit,
                                              server_properties,
                                              [X | ServerProperties]) end,
    IsPropPresent =
        fun (X) ->
                lists:member(X, rabbit_reader:server_properties(Protocol))
        end,

    %% Add a wholly new property of the simplified {KeyAtom, StringValue} form
    NewSimplifiedProperty = {NewHareKey, NewHareVal} = {hare, "soup"},
    ConsProp(NewSimplifiedProperty),
    %% Do we find hare soup, appropriately formatted in the generated properties?
    ExpectedHareImage = {list_to_binary(atom_to_list(NewHareKey)),
                         longstr,
                         list_to_binary(NewHareVal)},
    true = IsPropPresent(ExpectedHareImage),

    %% Add a wholly new property of the {BinaryKey, Type, Value} form
    %% and check for it
    NewProperty = {<<"new-bin-key">>, signedint, -1},
    ConsProp(NewProperty),
    %% Do we find the new property?
    true = IsPropPresent(NewProperty),

    %% Add a property that clobbers a built-in, and verify correct clobbering
    {NewVerKey, NewVerVal} = NewVersion = {version, "X.Y.Z."},
    {BinNewVerKey, BinNewVerVal} = {list_to_binary(atom_to_list(NewVerKey)),
                                    list_to_binary(NewVerVal)},
    ConsProp(NewVersion),
    ClobberedServerProps = rabbit_reader:server_properties(Protocol),
    %% Is the clobbering insert present?
    true = IsPropPresent({BinNewVerKey, longstr, BinNewVerVal}),
    %% Is the clobbering insert the only thing with the clobbering key?
    [{BinNewVerKey, longstr, BinNewVerVal}] =
        [E || {K, longstr, _V} = E <- ClobberedServerProps, K =:= BinNewVerKey],

    application:set_env(rabbit, server_properties, ServerProperties),
    passed.
