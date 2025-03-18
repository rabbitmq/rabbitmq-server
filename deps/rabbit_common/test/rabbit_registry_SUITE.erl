%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_registry_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbit/include/mc.hrl").

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

lookup_type_module(Config) ->
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(queue, <<"classic">>)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(queue, classic)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(queue, rabbit_classic_queue)),

    ok = rabbit_registry:register(queue, <<"classic">>, rabbit_classic_queue),

    ?assertMatch({ok, rabbit_classic_queue}, rabbit_registry:lookup_type_module(queue, <<"classic">>)),
    ?assertMatch({ok, rabbit_classic_queue}, rabbit_registry:lookup_type_module(queue, classic)),
    ?assertMatch({ok, rabbit_classic_queue}, rabbit_registry:lookup_type_module(queue, rabbit_classic_queue)),

    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(queue, quorum)).

lookup_type_name(Config) ->
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_name(queue, <<"classic">>)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(queue, classic)),
    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_module(queue, rabbit_classic_queue)),

    ok = rabbit_registry:register(queue, <<"classic">>, rabbit_classic_queue),

    ?assertMatch({ok, <<"classic">>}, rabbit_registry:lookup_type_name(queue, <<"classic">>)),
    ?assertMatch({ok, <<"classic">>}, rabbit_registry:lookup_type_name(queue, classic)),
    ?assertMatch({ok, <<"classic">>}, rabbit_registry:lookup_type_name(queue, rabbit_classic_queue)),

    ?assertMatch({error, not_found}, rabbit_registry:lookup_type_name(queue, quorum)).


%% -------------------------------------------------------------------
%% Utility.
%% -------------------------------------------------------------------
