%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_default_queue_type_SUITE).

-include_lib("common_test/include/ct.hrl").

-import(rabbit_ct_broker_helpers, [rpc/5]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     inject_dqt_undefined_binary,
     inject_dqt_preserves_existing_metadata
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 1}
    ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, Testcase).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

%% When default_queue_type is the binary <<"undefined">> (exported from an older versions),
%% inject the default. See rabbitmq/rabbitmq-server#10469
inject_dqt_undefined_binary(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    Input = #{default_queue_type => <<"undefined">>, name => <<"/">>},
    #{default_queue_type := Expected,
      metadata := #{default_queue_type := Expected}} =
        rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),
    ok.

%% Existing metadata keys should be preserved when injecting DQT
inject_dqt_preserves_existing_metadata(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    Input = #{
        default_queue_type => undefined,
        name => <<"/">>,
        metadata => #{
            description => <<"test vhost">>,
            tags => [replicate]
        }
    },
    #{metadata := #{default_queue_type := Expected,
                    description := <<"test vhost">>,
                    tags := [replicate]}} =
        rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),
    ok.
