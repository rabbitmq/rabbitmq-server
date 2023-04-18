%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(runtime_parameters_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        test_limits
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
    E  = {error_string, "Validation failed\n\ncomponent test is limited to 1 per node\n"},
    ok = rabbit_runtime_parameters:set_any(<<"/">>, <<"test">>, <<"good">>, <<"">>, none),
    E  = rabbit_runtime_parameters:set_any(<<"/">>, <<"test">>, <<"good">>, <<"">>, none),
    dummy_runtime_parameters:unregister().
