%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(config_schema_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile(export_all).

all() ->
    [
        run_snippets,
        quorum_queue_unique_names_invalid
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:run_setup_steps(Config),
    rabbit_ct_config_schema:init_schemas(rabbit, Config1).


end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

run_snippets(Config) ->
    rabbit_ct_config_schema:run_snippets(Config).

quorum_queue_unique_names_invalid(_Config) ->
    SchemaFiles = rabbit_prelaunch_conf:find_cuttlefish_schemas(rabbit_env:get_context()),
    Schema = cuttlefish_schema:files(SchemaFiles),
    Conf = [{["quorum_queue", "unique_names"], "invalid"}],
    ?assertMatch({error, _, {errorlist, [_ | _]}},
                 cuttlefish_generator:map(Schema, Conf)).
