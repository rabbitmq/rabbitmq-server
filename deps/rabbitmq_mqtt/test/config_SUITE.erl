%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module(config_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_broker_helpers, [rpc/5]).

all() ->
    [
     {group, mnesia}
    ].

groups() ->
    [
     {mnesia, [shuffle],
      [
       rabbitmq_default,
       environment_set,
       flag_set
      ]}
    ].

suite() ->
    [{timetrap, {seconds, 30}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(rabbitmq_default = Test, Config) ->
    init_per_testcase0(Test, Config);
init_per_testcase(environment_set = Test, Config0) ->
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {mnesia, [{dump_log_write_threshold, 25000},
                                  {dump_log_time_threshold, 60000}]}),
    init_per_testcase0(Test, Config);
init_per_testcase(flag_set = Test, Config0) ->
    Config = [{additional_erl_args, "-mnesia dump_log_write_threshold 15000"} | Config0],
    init_per_testcase0(Test, Config).

init_per_testcase0(Testcase, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(Config0, {rmq_nodename_suffix, Testcase}),
    Config = rabbit_ct_helpers:run_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config0) ->
    Config = rabbit_ct_helpers:testcase_finished(Config0, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

%% The MQTT plugin expects Mnesia dump_log_write_threshold to be increased
%% from 1000 (Mnesia default) to 5000 (RabbitMQ default).
rabbitmq_default(Config) ->
    ?assertEqual(5_000,
                 rpc(Config, 0, mnesia, system_info, [dump_log_write_threshold])),
    ?assertEqual(90_000,
                 rpc(Config, 0, mnesia, system_info, [dump_log_time_threshold])).

%% User configured setting in advanced.config should be respected.
environment_set(Config) ->
    ?assertEqual(25_000,
                 rpc(Config, 0, mnesia, system_info, [dump_log_write_threshold])),
    ?assertEqual(60_000,
                 rpc(Config, 0, mnesia, system_info, [dump_log_time_threshold])).

%% User configured setting in RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS should be respected.
flag_set(Config) ->
    ?assertEqual(15_000,
                 rpc(Config, 0, mnesia, system_info, [dump_log_write_threshold])),
    ?assertEqual(90_000,
                 rpc(Config, 0, mnesia, system_info, [dump_log_time_threshold])).
