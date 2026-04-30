%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_direct_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([all/0, groups/0]).
-export([init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).
-export([direct_connection_registered/1]).

all() ->
    [{group, tests}].

groups() ->
    [{tests, [], [direct_connection_registered]}].

%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, Testcase}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------

direct_connection_registered(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    BeforeLocal = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list_local, []),
    BeforeList = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list, []),
    ?assertEqual([], BeforeLocal),
    ?assertEqual([], BeforeList),

    Params = #amqp_params_direct{node         = Node,
                               virtual_host = <<"/">>,
                               username     = <<"guest">>,
                               password     = <<"guest">>},
    {ok, Conn} = amqp_connection:start(Params),
    true = is_pid(Conn),

    AfterLocal = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list_local, []),
    AfterList = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list, []),
    ?assertEqual([Conn], AfterLocal),
    ?assertEqual([Conn], AfterList),

    ok = amqp_connection:close(Conn),

    FinalLocal = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list_local, []),
    FinalList = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_direct, list, []),
    ?assertEqual([], FinalLocal),
    ?assertEqual([], FinalList),
    ok.
