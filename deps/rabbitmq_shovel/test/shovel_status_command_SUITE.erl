%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(shovel_status_command_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(CMD, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ShovelStatusCommand').

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               run_not_started,
                               output_not_started,
                               run_starting,
                               output_starting,
                               run_running,
                               output_running
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
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
%% Testcases.
%% -------------------------------------------------------------------
run_not_started(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A},
    {stream, []} = ?CMD:run([], Opts).

output_not_started(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A},
    {stream, []} = ?CMD:output({stream, []}, Opts).

run_starting(Config) ->
    shovel_test_utils:set_param_nowait(
      Config,
      <<"test">>, [{<<"src-queue">>,  <<"src">>},
                   {<<"dest-queue">>, <<"dest">>}]),
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A},
    case ?CMD:run([], Opts) of
        {stream, [{{<<"/">>, <<"test">>}, dynamic, starting, _}]} ->
            ok;
        {stream, []} ->
            throw(shovel_not_found);
        {stream, [{{<<"/">>, <<"test">>}, dynamic, {running, _}, _}]} ->
            ct:pal("Shovel is already running, starting could not be tested!")
    end,
    shovel_test_utils:clear_param(Config, <<"test">>).

output_starting(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A},
    {stream, [#{vhost := <<"/">>, name := <<"test">>, type := dynamic,
                state := starting, last_changed := <<"2016-11-17 10:00:00">>}]}
        = ?CMD:output({stream, [{{<<"/">>, <<"test">>}, dynamic, starting,
                                 {{2016, 11, 17}, {10, 00, 00}}}]}, Opts),
    shovel_test_utils:clear_param(Config, <<"test">>).

run_running(Config) ->
    shovel_test_utils:set_param(
      Config,
      <<"test">>, [{<<"src-queue">>,  <<"src">>},
                   {<<"dest-queue">>, <<"dest">>}]),
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A},
    {stream, [{{<<"/">>, <<"test">>}, dynamic, {running, _}, _}]}
        = ?CMD:run([], Opts),
    shovel_test_utils:clear_param(Config, <<"test">>).

output_running(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A},
    {stream, [#{vhost := <<"/">>, name := <<"test">>, type := dynamic,
                state := running, source := <<"amqp://server-1">>,
                destination := <<"amqp://server-2">>,
                termination_reason := <<>>,
                last_changed := <<"2016-11-17 10:00:00">>}]} =
        ?CMD:output({stream, [{{<<"/">>, <<"test">>}, dynamic,
                               {running, [{src_uri, <<"amqp://server-1">>},
                                          {dest_uri, <<"amqp://server-2">>}]},
                               {{2016, 11, 17}, {10, 00, 00}}}]}, Opts),
    shovel_test_utils:clear_param(Config, <<"test">>).
