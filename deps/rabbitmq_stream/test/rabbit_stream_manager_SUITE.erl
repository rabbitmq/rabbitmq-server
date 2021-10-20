%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_manager_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [{group, non_parallel_tests}].

groups() ->
    [{non_parallel_tests, [], [manage_super_stream]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            rabbit_ct_helpers:log_environment(),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config1 =
        rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]),
    Config2 =
        rabbit_ct_helpers:set_config(Config1,
                                     {rabbitmq_ct_tls_verify, verify_none}),
    Config3 =
        rabbit_ct_helpers:set_config(Config2, {rabbitmq_stream, verify_none}),
    rabbit_ct_helpers:run_setup_steps(Config3,
                                      [fun(StepConfig) ->
                                          rabbit_ct_helpers:merge_app_env(StepConfig,
                                                                          {rabbit,
                                                                           [{core_metrics_gc_interval,
                                                                             1000}]})
                                       end,
                                       fun(StepConfig) ->
                                          rabbit_ct_helpers:merge_app_env(StepConfig,
                                                                          {rabbitmq_stream,
                                                                           [{connection_negotiation_step_timeout,
                                                                             500}]})
                                       end]
                                      ++ rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

manage_super_stream(Config) ->
    % create super stream
    ?assertEqual(ok,
                 create_super_stream(Config,
                                     <<"invoices">>,
                                     [<<"invoices-0">>, <<"invoices-1">>,
                                      <<"invoices-2">>],
                                     [<<"0">>, <<"1">>, <<"2">>])),
    % get the correct partitions
    ?assertEqual({ok,
                  [<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>]},
                 partitions(Config, <<"invoices">>)),

    [?assertEqual({ok, [Partition]},
                  route(Config, RoutingKey, <<"invoices">>))
     || {Partition, RoutingKey}
            <- [{<<"invoices-0">>, <<"0">>}, {<<"invoices-1">>, <<"1">>},
                {<<"invoices-2">>, <<"2">>}]],

    % get an error if trying to re-create it
    ?assertMatch({error, _},
                 create_super_stream(Config,
                                     <<"invoices">>,
                                     [<<"invoices-0">>, <<"invoices-1">>,
                                      <<"invoices-2">>],
                                     [<<"0">>, <<"1">>, <<"2">>])),

    % can delete it
    ?assertEqual(ok, delete_super_stream(Config, <<"invoices">>)),

    % create a stream with the same name as a potential partition
    ?assertMatch({ok, _}, create_stream(Config, <<"invoices-1">>)),

    % cannot create the super stream because a partition already exists
    ?assertMatch({error, _},
                 create_super_stream(Config,
                                     <<"invoices">>,
                                     [<<"invoices-0">>, <<"invoices-1">>,
                                      <<"invoices-2">>],
                                     [<<"0">>, <<"1">>, <<"2">>])),

    ok.

create_super_stream(Config, Name, Partitions, RKs) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 create_super_stream,
                                 [<<"/">>,
                                  Name,
                                  Partitions,
                                  #{},
                                  RKs,
                                  <<"guest">>]).

delete_super_stream(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 delete_super_stream,
                                 [<<"/">>, Name, <<"guest">>]).

create_stream(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 create,
                                 [<<"/">>, Name, [], <<"guest">>]).

partitions(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 partitions,
                                 [<<"/">>, Name]).

route(Config, RoutingKey, SuperStream) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 route,
                                 [RoutingKey, <<"/">>, SuperStream]).
