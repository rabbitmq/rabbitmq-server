%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(definition_import_SUITE).

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, roundtrip}
    ].

groups() ->
    [
        {roundtrip, [], [
            export_import_round_trip
        ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config.
end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, Group}
                                                   ]),
    rabbit_ct_helpers:run_setup_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%
%% Tests
%%

export_import_round_trip(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
      false ->
        import_file_case(Config, "case1"),
        Defs = export(Config),
        import_raw(Config, rabbit_json:encode(Defs));
      _ ->
        %% skip the test in mixed version mode
        {skip, "Should not run in mixed version environments"}
    end.

%%
%% Implementation
%%

import_file_case(Config, CaseName) ->
    CasePath = filename:join([
        ?config(data_dir, Config),
        CaseName ++ ".json"
    ]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_import_case, [CasePath]),
    ok.


import_raw(Config, Body) ->
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_definitions, import_raw, [Body]) of
        ok -> ok;
        {error, E} ->
            ct:pal("Import of JSON definitions ~tp failed: ~tp~n", [Body, E]),
            ct:fail({expected_failure, Body, E})
    end.

export(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_export, []).

run_export() ->
    rabbit_definitions:all_definitions().

run_directory_import_case(Path, Expected) ->
    ct:pal("Will load definitions from files under ~tp~n", [Path]),
    Result = rabbit_definitions:maybe_load_definitions_from(true, Path),
    case Expected of
        ok ->
            ok = Result;
        error ->
            ?assertMatch({error, {failed_to_import_definitions, _, _}}, Result)
    end.

run_import_case(Path) ->
   {ok, Body} = file:read_file(Path),
   ct:pal("Successfully loaded a definition to import from ~tp~n", [Path]),
   case rabbit_definitions:import_raw(Body) of
     ok -> ok;
     {error, E} ->
       ct:pal("Import case ~tp failed: ~tp~n", [Path, E]),
       ct:fail({expected_failure, Path, E})
   end.

run_invalid_import_case(Path) ->
   {ok, Body} = file:read_file(Path),
   ct:pal("Successfully loaded a definition file at ~tp~n", [Path]),
   case rabbit_definitions:import_raw(Body) of
     ok ->
       ct:pal("Expected import case ~tp to fail~n", [Path]),
       ct:fail({expected_failure, Path});
     {error, _E} -> ok
   end.

run_invalid_import_case_if_unchanged(Path) ->
    Mod = rabbit_definitions_import_local_filesystem,
    ct:pal("Successfully loaded a definition to import from ~tp~n", [Path]),
    case rabbit_definitions:maybe_load_definitions_from_local_filesystem_if_unchanged(Mod, false, Path) of
        ok ->
            ct:pal("Expected import case ~tp to fail~n", [Path]),
            ct:fail({expected_failure, Path});
        {error, _E} -> ok
    end.

queue_lookup(Config, VHost, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [rabbit_misc:r(VHost, queue, Name)]).

vhost_lookup(Config, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, lookup, [VHost]).

user_lookup(Config, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, lookup_user, [User]).

delete_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"CT tests">>]).
