%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(reader_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([nowarn_export_all, export_all]).
-compile(export_all).

-import(rabbit_ct_client_helpers, [open_connection/2
                                   ]).

all() ->
    [
     {group, cluster_size_1}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

groups() ->
    [
     {cluster_size_1, [], all_tests()}
    ].

all_tests() ->
    [
     successful_connection_and_alarm_registration,
     successful_connection_and_alarm_registration_after_scheduled_check,
     successful_connection_and_failed_alarm_registration_before_check,
     unsuccessful_connection_and_failed_alarm_registration_after_check
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

init_per_group_common(Group, Config, Size) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, Size}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

successful_connection_and_alarm_registration(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    _Conn1 = open_connection(Config, A),
    ReaderPid = get_reader_pid(Config),
    ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, ReaderPid)),
    passed.

successful_connection_and_alarm_registration_after_scheduled_check(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    _Conn1 = open_connection(Config, A),

    Timeout = 500,
    rabbit_ct_broker_helpers:set_alarms_registration_check_timeout(Config, Timeout),
    timer:sleep(Timeout + 500),

    ReaderPid = get_reader_pid(Config),
    ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, ReaderPid)),
    passed.

successful_connection_and_failed_alarm_registration_before_check(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:terminate_rabbit_alarm(Config),

    Timeout = 10_000,
    rabbit_ct_broker_helpers:set_alarms_registration_check_timeout(Config, Timeout),

    _Conn1 = open_connection(Config, A),
    ReaderPid = get_reader_pid(Config),

    try
        ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, ReaderPid))
    catch
        _:{_,{noproc,{_,_,[rabbit_alarm,_,_,_]}}} ->
            ok
    end,

    ?assert(rabbit_ct_broker_helpers:is_process_alive(Config, ReaderPid)),
    passed.

unsuccessful_connection_and_failed_alarm_registration_after_check(Config) ->
    ok = rabbit_ct_broker_helpers:restart_broker(Config),
    ?assert(rabbit_ct_broker_helpers:is_process_alive(Config, rabbit_alarm)),
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:terminate_rabbit_alarm(Config),

    Timeout = 500,
    rabbit_ct_broker_helpers:set_alarms_registration_check_timeout(Config, Timeout),

    _Conn1 = open_connection(Config, A),
    ReaderPid = get_reader_pid(Config),

    %% delay > timeout, and let connection fail with {noproc, rabbit_alarm}
    timer:sleep(Timeout + 500),

    try
        ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, ReaderPid))
    catch
        _:{_,{noproc,{_,_,[rabbit_alarm,_,_,_]}}} ->
            ok
    end,

    ?assertNot(rabbit_ct_broker_helpers:is_process_alive(Config, ReaderPid)),
    passed.

get_reader_pid(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_reader_pid1, []).

get_reader_pid1() ->
    lists:last(rabbit_networking:local_connections()).

get_reader_state(Config, ReaderPid) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_reader_state1, [ReaderPid]).

get_reader_state1(ReaderPid) ->
    sys:get_state(ReaderPid).
