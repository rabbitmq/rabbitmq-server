%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_registry_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
     {group, lookup}
    ].

groups() ->
    [
     {lookup, [], [lookup_type_module,
                   lookup_type_name
                  ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    {ok, RPid} = rabbit_registry:start_link(),
    [{registry_pid, RPid} |  Config].

end_per_testcase(_Testcase, Config) ->
    RPid = ?config(registry_pid, Config),
    gen_server:stop(RPid),
    ok.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

lookup_type_module(_Config) ->
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(runtime_parameter, <<"param">>)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(runtime_parameter, param)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(runtime_parameter, runtime_parameter_test)),

    ok = rabbit_registry:register(runtime_parameter, <<"param">>, rabbit_runtime_parameter_registry_test),

    ?assertMatch({ok, rabbit_runtime_parameter_registry_test}, rabbit_registry:lookup_type_module(runtime_parameter, <<"param">>)),
    ?assertMatch({ok, rabbit_runtime_parameter_registry_test}, rabbit_registry:lookup_type_module(runtime_parameter, param)),
    ?assertMatch({ok, rabbit_runtime_parameter_registry_test}, rabbit_registry:lookup_type_module(runtime_parameter, rabbit_runtime_parameter_registry_test)),

    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(runtime_parameter, another_param)).

lookup_type_name(_Config) ->
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_name(runtime_parameter, <<"param">>)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(runtime_parameter, param)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(runtime_parameter, runtime_parameter_test)),

    ok = rabbit_registry:register(runtime_parameter, <<"param">>, rabbit_runtime_parameter_registry_test),

    ?assertMatch({ok, <<"param">>}, rabbit_registry:lookup_type_name(runtime_parameter, <<"param">>)),
    ?assertMatch({ok, <<"param">>}, rabbit_registry:lookup_type_name(runtime_parameter, param)),
    ?assertMatch({ok, <<"param">>}, rabbit_registry:lookup_type_name(runtime_parameter, rabbit_runtime_parameter_registry_test)),

    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_name(runtime_parameter, another_param)).


%% -------------------------------------------------------------------
%% Utility.
%% -------------------------------------------------------------------
