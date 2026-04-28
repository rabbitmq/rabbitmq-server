%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(runtime_parameters_http_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [http_get/3, http_put/4, http_delete/3]).

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
         put_unknown_component_returns_bad_request,
         put_unknown_component_does_not_create_atom,
         delete_unknown_component_does_not_create_atom,
         crud_lifecycle
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

put_unknown_component_returns_bad_request(Config) ->
    http_put(Config,
             "/parameters/zzz_unknown_component/%2F/p",
             [{value, <<"v">>}],
             ?BAD_REQUEST).

put_unknown_component_does_not_create_atom(Config) ->
    Component = <<"zzz_no_atom_put_component">>,
    false = atom_exists_on_broker(Config, Component),
    http_put(Config,
             "/parameters/" ++ binary_to_list(Component) ++ "/%2F/p",
             [{value, <<"v">>}],
             ?BAD_REQUEST),
    false = atom_exists_on_broker(Config, Component).

delete_unknown_component_does_not_create_atom(Config) ->
    Component = <<"zzz_no_atom_del_component">>,
    false = atom_exists_on_broker(Config, Component),
    http_delete(Config,
                "/parameters/" ++ binary_to_list(Component) ++ "/%2F/p",
                ?NOT_FOUND),
    false = atom_exists_on_broker(Config, Component).

crud_lifecycle(Config) ->
    register_test_component(Config),
    try
        http_put(Config,
                 "/parameters/test/%2F/good",
                 [{value, <<"ignore">>}],
                 {group, '2xx'}),
        #{vhost     := <<"/">>,
          component := <<"test">>,
          name      := <<"good">>,
          value     := <<"ignore">>} =
            http_get(Config, "/parameters/test/%2F/good", ?OK),
        http_delete(Config,
                    "/parameters/test/%2F/good",
                    {group, '2xx'})
    after
        unregister_test_component(Config)
    end.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

register_test_component(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_mgmt_runtime_parameters_util, register, []).

unregister_test_component(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_mgmt_runtime_parameters_util, unregister, []).

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
