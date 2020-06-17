%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This test suite covers the definitions import function
%% outside of the context of HTTP API (e.g. when definitions are
%% imported on boot).

-module(definition_import_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, import_on_a_running_node},
     {group, import_on_a_booting_node}
    ].

groups() ->
    [
     {import_on_a_running_node, [], [
                               %% Note: to make it easier to see which case failed,
                               %% these are intentionally not folded into a single case.
                               %% If generation becomes an alternative worth considering for these tests,
                               %% we'll just add a case that drives PropEr.
                               import_case1,
                               import_case2,
                               import_case3,
                               import_case4,
                               import_case5,
                               import_case6,
                               import_case7,
                               import_case8,
                               import_case9,
                               import_case10,
                               import_case11,
                               import_case12
                              ]},
        {import_on_a_booting_node, [], [
            import_on_boot_case1
        ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
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

init_per_testcase(import_on_boot_case1 = Testcase, Config) ->
    CasePath = filename:join(?config(data_dir, Config), "case5.json"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 1}
      ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
      {rabbit, [
          {load_definitions, CasePath}
      ]}),
    rabbit_ct_helpers:run_steps(Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%
%% Tests
%%

import_case1(Config) -> import_file_case(Config, "case1").
import_case2(Config) -> import_file_case(Config, "case2").
import_case3(Config) -> import_file_case(Config, "case3").
import_case4(Config) -> import_file_case(Config, "case4").
import_case6(Config) -> import_file_case(Config, "case6").
import_case7(Config) -> import_file_case(Config, "case7").
import_case8(Config) -> import_file_case(Config, "case8").

import_case9(Config) -> import_from_directory_case(Config, "case9").

import_case10(Config) -> import_from_directory_case_fails(Config, "case10").

import_case5(Config) ->
    import_file_case(Config, "case5"),
    ?assertEqual(rabbit_ct_broker_helpers:rpc(Config, 0,
                                              rabbit_runtime_parameters, value_global,
                                              [mqtt_port_to_vhost_mapping]),
                 %% expect a proplist, see rabbitmq/rabbitmq-management#528
                 [{<<"1883">>,<<"/">>},
                  {<<"1884">>,<<"vhost2">>}]).

import_case11(Config) -> import_file_case(Config, "case11").
import_case12(Config) -> import_invalid_file_case(Config, "failing_case12").

import_on_boot_case1(Config) ->
    %% see case5.json
    VHost = <<"vhost2">>,
    %% verify that vhost2 eventually starts
    case rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, await_running_on_all_nodes, [VHost, 3000]) of
        ok -> ok;
        {error, timeout} -> ct:fail("virtual host ~p was not imported on boot", [VHost])
    end.

%%
%% Implementation
%%

import_file_case(Config, CaseName) ->
    CasePath = filename:join(?config(data_dir, Config), CaseName ++ ".json"),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_import_case, [CasePath]),
    ok.

import_invalid_file_case(Config, CaseName) ->
    CasePath = filename:join(?config(data_dir, Config), CaseName ++ ".json"),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, run_invalid_import_case, [CasePath]),
    ok.

import_from_directory_case(Config, CaseName) ->
    import_from_directory_case_expect(Config, CaseName, ok).

import_from_directory_case_fails(Config, CaseName) ->
    import_from_directory_case_expect(Config, CaseName, error).

import_from_directory_case_expect(Config, CaseName, Expected) ->
    CasePath = filename:join(?config(data_dir, Config), CaseName),
    ?assert(filelib:is_dir(CasePath)),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 ?MODULE, run_directory_import_case,
                                 [CasePath, Expected]),
    ok.

run_directory_import_case(Path, Expected) ->
    ct:pal("Will load definitions from files under ~p~n", [Path]),
    Result = rabbit_definitions:maybe_load_definitions_from(true, Path),
    case Expected of
        ok ->
            ok = Result;
        error ->
            ?assertMatch({error, {failed_to_import_definitions, _, _}}, Result)
    end.

run_import_case(Path) ->
   {ok, Body} = file:read_file(Path),
   ct:pal("Successfully loaded a definition to import from ~p~n", [Path]),
   case rabbit_definitions:import_raw(Body) of
     ok -> ok;
     {error, E} ->
       ct:pal("Import case ~p failed: ~p~n", [Path, E]),
       ct:fail({failure, Path, E})
   end.

run_invalid_import_case(Path) ->
   {ok, Body} = file:read_file(Path),
   ct:pal("Successfully loaded a definition to import from ~p~n", [Path]),
   case rabbit_definitions:import_raw(Body) of
     ok ->
       ct:pal("Expected import case ~p to fail~n", [Path]),
       ct:fail({failure, Path});
     {error, _E} -> ok
   end.
