%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(global_parameters_http_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
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
         get_nonexistent_returns_not_found,
         get_does_not_create_atom,
         delete_nonexistent_returns_not_found,
         delete_does_not_create_atom,
         put_does_not_create_atom,
         crud_lifecycle,
         name_at_limit_is_accepted,
         name_over_limit_is_rejected,
         value_under_limit_is_accepted,
         value_over_limit_is_rejected,
         cluster_name_can_still_be_set,
         cluster_name_over_limit_is_rejected,
         configured_limits_are_used,
         plugin_style_name_works,
         get_does_not_create_atoms_prop,
         put_does_not_create_atoms_prop,
         legacy_atom_keyed_parameter_is_readable,
         legacy_atom_keyed_parameter_set_migrates_to_binary_key,
         legacy_atom_keyed_parameter_delete_removes_the_atom_keyed_record
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

get_nonexistent_returns_not_found(Config) ->
    http_get(Config, "/global-parameters/test_nonexistent_param", ?NOT_FOUND).

get_does_not_create_atom(Config) ->
    Name = <<"zzz_no_atom_get_test">>,
    false = atom_exists_on_broker(Config, Name),
    http_get(Config, "/global-parameters/" ++ binary_to_list(Name), ?NOT_FOUND),
    false = atom_exists_on_broker(Config, Name).

delete_nonexistent_returns_not_found(Config) ->
    http_delete(Config, "/global-parameters/test_nonexistent_param_del", ?NOT_FOUND).

delete_does_not_create_atom(Config) ->
    Name = <<"zzz_no_atom_del_test">>,
    false = atom_exists_on_broker(Config, Name),
    http_delete(Config, "/global-parameters/" ++ binary_to_list(Name), ?NOT_FOUND),
    false = atom_exists_on_broker(Config, Name).

put_does_not_create_atom(Config) ->
    Name = <<"zzz_no_atom_put_test">>,
    false = atom_exists_on_broker(Config, Name),
    http_put(Config,
             "/global-parameters/" ++ binary_to_list(Name),
             [{value, <<"x">>}],
             {group, '2xx'}),
    false = atom_exists_on_broker(Config, Name),
    http_delete(Config, "/global-parameters/" ++ binary_to_list(Name), {group, '2xx'}).

crud_lifecycle(Config) ->
    http_put(Config,
             "/global-parameters/test_crud_param",
             [{value, [{a, <<"b">>}]}],
             {group, '2xx'}),
    #{name  := <<"test_crud_param">>,
      value := #{a := <<"b">>}} =
        http_get(Config, "/global-parameters/test_crud_param", ?OK),
    http_delete(Config, "/global-parameters/test_crud_param", {group, '2xx'}),
    http_get(Config, "/global-parameters/test_crud_param", ?NOT_FOUND).

name_at_limit_is_accepted(Config) ->
    Name = lists:duplicate(1024, $a),
    Path = "/global-parameters/" ++ Name,
    http_put(Config, Path, [{value, <<"x">>}], {group, '2xx'}),
    http_get(Config, Path, ?OK),
    http_delete(Config, Path, {group, '2xx'}).

name_over_limit_is_rejected(Config) ->
    Name = lists:duplicate(1025, $a),
    Path = "/global-parameters/" ++ Name,
    http_put(Config, Path, [{value, <<"x">>}], ?BAD_REQUEST),
    http_get(Config, Path, ?NOT_FOUND).

value_under_limit_is_accepted(Config) ->
    Path = "/global-parameters/test_value_under_limit",
    Value = binary:copy(<<"a">>, 131072 - 64),
    http_put(Config, Path, [{value, Value}], {group, '2xx'}),
    #{value := Value} = http_get(Config, Path, ?OK),
    http_delete(Config, Path, {group, '2xx'}).

value_over_limit_is_rejected(Config) ->
    Path = "/global-parameters/test_value_over_limit",
    Value = binary:copy(<<"a">>, 131072 + 1),
    Body = rabbit_json:encode(#{value => Value}),
    {ok, {{_, 400, _}, _, ResBody}} =
        rabbit_mgmt_test_util:req(Config, 0, put, Path,
                                  [rabbit_mgmt_test_util:auth_header("guest", "guest")],
                                  Body),
    nomatch = binary:match(iolist_to_binary(ResBody), <<"aaaa">>),
    http_get(Config, Path, ?NOT_FOUND).

cluster_name_can_still_be_set(Config) ->
    Path = "/global-parameters/cluster_name",
    http_put(Config, Path, [{value, <<"limits-test">>}], {group, '2xx'}),
    #{value := <<"limits-test">>} = http_get(Config, Path, ?OK).

cluster_name_over_limit_is_rejected(Config) ->
    http_put(Config, "/cluster-name",
             [{name, binary:copy(<<"a">>, 131073)}], ?BAD_REQUEST),
    http_put(Config, "/cluster-name", [{name, <<"limits-test-2">>}], {group, '2xx'}),
    #{name := <<"limits-test-2">>} = http_get(Config, "/cluster-name", ?OK).

configured_limits_are_used(Config) ->
    Limits = [{global, [{max_name_length, 8}, {max_value_size, 64}]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, runtime_parameters, Limits]),
    try
        http_put(Config, "/global-parameters/abcdefgh", [{value, <<"x">>}], {group, '2xx'}),
        http_put(Config, "/global-parameters/abcdefghi", [{value, <<"x">>}], ?BAD_REQUEST),
        http_put(Config, "/global-parameters/abcdefgh",
                 [{value, binary:copy(<<"a">>, 100)}], ?BAD_REQUEST),
        http_delete(Config, "/global-parameters/abcdefgh", {group, '2xx'})
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                          [rabbit, runtime_parameters])
    end.

%% Plugin-style underscore-separated names must work for both write and read.
plugin_style_name_works(Config) ->
    Name = "test_plugin_upstream_param",
    http_put(Config,
             "/global-parameters/" ++ Name,
             [{value, <<"http://upstream:5672">>}],
             {group, '2xx'}),
    #{name := <<"test_plugin_upstream_param">>} =
        http_get(Config, "/global-parameters/" ++ Name, ?OK),
    http_delete(Config, "/global-parameters/" ++ Name, {group, '2xx'}).

get_does_not_create_atoms_prop(Config) ->
    Fun = fun() -> prop_get_does_not_create_atom(Config) end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 50).

prop_get_does_not_create_atom(Config) ->
    ?FORALL(
        N, pos_integer(),
        begin
            Name = iolist_to_binary(["zzz_no_atom_prop_", integer_to_list(N)]),
            case atom_exists_on_broker(Config, Name) of
                true ->
                    true;
                false ->
                    http_get(Config,
                             "/global-parameters/" ++ binary_to_list(Name),
                             ?NOT_FOUND),
                    not atom_exists_on_broker(Config, Name)
            end
        end).

put_does_not_create_atoms_prop(Config) ->
    Fun = fun() -> prop_put_does_not_create_atom(Config) end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 50).

prop_put_does_not_create_atom(Config) ->
    ?FORALL(
        N, pos_integer(),
        begin
            Name = iolist_to_binary(["zzz_no_atom_put_prop_", integer_to_list(N)]),
            case atom_exists_on_broker(Config, Name) of
                true ->
                    true;
                false ->
                    Path = "/global-parameters/" ++ binary_to_list(Name),
                    http_put(Config, Path, [{value, <<"x">>}], {group, '2xx'}),
                    Result = not atom_exists_on_broker(Config, Name),
                    http_delete(Config, Path, {group, '2xx'}),
                    Result
            end
        end).

%% -------------------------------------------------------------------
%% Upgrade compatibility: older nodes stored global runtime
%% parameter names as atoms.
%% -------------------------------------------------------------------

legacy_atom_keyed_parameter_is_readable(Config) ->
    Name = <<"zzz_legacy_atom_get_test">>,
    AtomKey = seed_legacy_atom_keyed_parameter(Config, Name, <<"legacy-value">>),
    Path = "/global-parameters/" ++ binary_to_list(Name),

    #{name := Name, value := <<"legacy-value">>} = http_get(Config, Path, ?OK),

    %% list_global() must not show the same logical parameter twice
    %% while only the legacy atom-keyed record exists.
    1 = count_occurrences(Name, http_get(Config, "/global-parameters", ?OK)),

    http_delete(Config, Path, {group, '2xx'}),
    undefined = get_on_broker(Config, AtomKey).

legacy_atom_keyed_parameter_set_migrates_to_binary_key(Config) ->
    Name = <<"zzz_legacy_atom_set_test">>,
    AtomKey = seed_legacy_atom_keyed_parameter(Config, Name, <<"legacy-value">>),
    Path = "/global-parameters/" ++ binary_to_list(Name),

    %% Writing a new value must not leave the old atom-keyed record
    %% behind as a stale duplicate alongside the new binary-keyed one.
    http_put(Config, Path, [{value, <<"new-value">>}], {group, '2xx'}),

    #{name := Name, value := <<"new-value">>} = http_get(Config, Path, ?OK),
    1 = count_occurrences(Name, http_get(Config, "/global-parameters", ?OK)),
    undefined = get_on_broker(Config, AtomKey),

    http_delete(Config, Path, {group, '2xx'}),
    http_get(Config, Path, ?NOT_FOUND).

legacy_atom_keyed_parameter_delete_removes_the_atom_keyed_record(Config) ->
    Name = <<"zzz_legacy_atom_delete_test">>,
    AtomKey = seed_legacy_atom_keyed_parameter(Config, Name, <<"legacy-value">>),
    Path = "/global-parameters/" ++ binary_to_list(Name),

    %% DELETE looks the record up by binary name but must remove it
    %% under whatever key it is actually stored under, not silently
    %% no-op because that key happens to be the legacy atom.
    http_delete(Config, Path, {group, '2xx'}),

    http_get(Config, Path, ?NOT_FOUND),
    undefined = get_on_broker(Config, AtomKey).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

seed_legacy_atom_keyed_parameter(Config, Name, Value) ->
    AtomKey = binary_to_atom(Name, utf8),
    new = rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbit_db_rtparams, set, [AtomKey, Value]),
    AtomKey.

get_on_broker(Config, Key) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_rtparams, get, [Key]).

count_occurrences(Name, Params) ->
    length([P || P <- Params, maps:get(name, P) =:= Name]).

%% `binary_to_existing_atom/2` raises badarg when the atom is absent.
%% rabbit_ct_broker_helpers:rpc/4 is backed by erpc:call/4, which
%% re-raises remote error exceptions as `error:{exception, Reason, _}`.
atom_exists_on_broker(Config, Name) when is_binary(Name) ->
    try rabbit_ct_broker_helpers:rpc(
          Config, 0, erlang, binary_to_existing_atom, [Name, utf8]) of
        A when is_atom(A) -> true
    catch
        error:{exception, badarg, _} -> false
    end.
