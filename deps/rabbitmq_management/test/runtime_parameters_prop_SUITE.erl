%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(runtime_parameters_prop_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [http_put/4]).

-compile([export_all, nowarn_export_all]).

-define(NUM_TESTS, 50).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
         put_does_not_create_atoms_prop
     ]}
    ].

%% -------------------------------------------------------------------
%% Setup / teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbitmq_management, [
                    {sample_retention_policies,
                        [{global,   [{605, 1}]},
                         {basic,    [{605, 1}]},
                         {detailed, [{10, 1}]}]}]}),
    rabbit_ct_helpers:run_setup_steps(
        Config2,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
        Config,
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
%% Test cases.
%% -------------------------------------------------------------------

put_does_not_create_atoms_prop(Config) ->
    Fun = fun() -> prop_put_does_not_create_atom(Config) end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], ?NUM_TESTS).

prop_put_does_not_create_atom(Config) ->
    ?FORALL(
        N, pos_integer(),
        begin
            Component = iolist_to_binary(
                          ["zzz_no_atom_prop_comp_", integer_to_list(N)]),
            case atom_exists_on_broker(Config, Component) of
                true ->
                    true;
                false ->
                    http_put(Config,
                             "/parameters/"
                             ++ binary_to_list(Component) ++ "/%2F/p",
                             [{value, <<"v">>}],
                             ?BAD_REQUEST),
                    not atom_exists_on_broker(Config, Component)
            end
        end).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

%% binary_to_existing_atom/2 raises badarg when the atom is absent;
%% rabbit_ct_broker_helpers:rpc/5 uses erpc, which re-raises
%% remote exceptions locally rather than returning {badrpc, _}.
atom_exists_on_broker(Config, Name) when is_binary(Name) ->
    try rabbit_ct_broker_helpers:rpc(
          Config, 0, erlang, binary_to_existing_atom, [Name, utf8]) of
        A when is_atom(A) -> true
    catch
        _:_ -> false
    end.
