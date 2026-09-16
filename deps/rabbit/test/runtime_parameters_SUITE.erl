%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(runtime_parameters_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        test_limits,
        boots_with_legacy_atom_keyed_internal_cluster_id,
        set_global_writes_atom_key_until_feature_flag_is_enabled
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).


end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

test_limits(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                 test_limits1, [Config]).

test_limits1(_Config) ->
    dummy_runtime_parameters:register(),
    application:set_env(rabbit, runtime_parameters, [{limits, [{<<"test">>, 1}]}]),
    E  = {error_string, "Validation failed\n\ncomponent test is limited to 1\n"},
    ok = rabbit_runtime_parameters:set_any(<<"/">>, <<"test">>, <<"good">>, <<"">>, none),
    E  = rabbit_runtime_parameters:set_any(<<"/">>, <<"test">>, <<"good">>, <<"">>, none),
    dummy_runtime_parameters:unregister().

%% The key used to be an atom; make sure we can handle that
boots_with_legacy_atom_keyed_internal_cluster_id(Config) ->
    LegacyId = <<"legacy-cluster-id-DMmlVMdAbItqeyl8_8bdMw">>,
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, ?MODULE, seed_legacy_internal_cluster_id, [LegacyId]),
    ok = rabbit_ct_broker_helpers:restart_broker(Config, 0),
    LegacyId = rabbit_ct_broker_helpers:rpc(
                 Config, 0, rabbit_nodes, persistent_cluster_id, []).

seed_legacy_internal_cluster_id(Value) ->
    ok = rabbit_db_rtparams:delete(<<"internal_cluster_id">>),
    _ = rabbit_db_rtparams:set(internal_cluster_id, Value),
    ok.

set_global_writes_atom_key_until_feature_flag_is_enabled(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, ?MODULE,
           set_global_writes_atom_key_until_feature_flag_is_enabled1, []).

set_global_writes_atom_key_until_feature_flag_is_enabled1() ->
    Name = <<"zzz_ff_gate_test">>,
    AtomKey = binary_to_atom(Name, utf8),
    AtomPath = rabbit_db_rtparams:khepri_global_rp_path(AtomKey),
    BinaryPath = rabbit_db_rtparams:khepri_global_rp_path(Name),

    ok = meck:new(rabbit_feature_flags, [passthrough, no_link]),
    try
        ok = meck:expect(rabbit_feature_flags, is_enabled,
                         fun('rabbitmq_4.4.0') -> false;
                            (Other) -> meck:passthrough([Other])
                         end),
        ok = rabbit_runtime_parameters:set_global(Name, <<"v1">>, <<"acting-user">>),
        {ok, _} = rabbit_khepri:get(AtomPath),
        {error, {khepri, node_not_found, _}} = rabbit_khepri:get(BinaryPath),

        ok = meck:expect(rabbit_feature_flags, is_enabled,
                         fun('rabbitmq_4.4.0') -> true;
                            (Other) -> meck:passthrough([Other])
                         end),
        ok = rabbit_runtime_parameters:set_global(Name, <<"v2">>, <<"acting-user">>),
        {ok, _} = rabbit_khepri:get(BinaryPath),
        {error, {khepri, node_not_found, _}} = rabbit_khepri:get(AtomPath)
    after
        meck:unload(rabbit_feature_flags),
        rabbit_runtime_parameters:clear_global(Name, <<"acting-user">>)
    end,
    ok.
