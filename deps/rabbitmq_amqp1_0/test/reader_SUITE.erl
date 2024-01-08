%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(reader_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
        alarm_registration,
        alarm_registration_after_scheduled_check,
        failed_alarm_registration_before_check,
        failed_connection_and_failed_alarm_registration_after_check
                 ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [
                         {rmq_nodename_suffix, Suffix},
                         {amqp10_client_library, Group}
                        ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

alarm_registration(Config) ->
    Connection = open_connection(Config, ?FUNCTION_NAME),
    Amqp10ReaderPid = get_amqp10_reader_pid(Config),
    ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, 0, Amqp10ReaderPid)),
    ok = amqp10_client:close_connection(Connection).

alarm_registration_after_scheduled_check(Config) ->
    Connection = open_connection(Config, ?FUNCTION_NAME),

    Timeout = 500,
    rabbit_ct_broker_helpers:set_alarms_registration_check_timeout(Config, 0, Timeout),
    timer:sleep(Timeout + 500),

    Amqp10ReaderPid = get_amqp10_reader_pid(Config),
    ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, 0, Amqp10ReaderPid)),

    ok = amqp10_client:close_connection(Connection).

failed_alarm_registration_before_check(Config) ->
    rabbit_ct_broker_helpers:terminate_rabbit_alarm(Config),

    Timeout = 10_000,
    rabbit_ct_broker_helpers:set_alarms_registration_check_timeout(Config, 0, Timeout),

    Connection = open_connection(Config, ?FUNCTION_NAME),
    Amqp10ReaderPid = get_amqp10_reader_pid(Config),

    try
        ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, 0, Amqp10ReaderPid))
    catch
        _:{_,{noproc,{_,_,[rabbit_alarm,_,_,_]}}} ->
            ok
    end,

    ?assert(rabbit_ct_broker_helpers:is_process_alive(Config, Amqp10ReaderPid)),

    ok = amqp10_client:close_connection(Connection).

failed_connection_and_failed_alarm_registration_after_check(Config) ->
    ok = rabbit_ct_broker_helpers:restart_broker(Config),
    ?assert(rabbit_ct_broker_helpers:is_process_alive(Config, rabbit_alarm)),
    rabbit_ct_broker_helpers:terminate_rabbit_alarm(Config),

    Timeout = 500,
    rabbit_ct_broker_helpers:set_alarms_registration_check_timeout(Config, 0, Timeout),

    Connection = open_connection(Config, ?FUNCTION_NAME),
    Amqp10ReaderPid = get_amqp10_reader_pid(Config),

    %% delay > timeout, and let connection fail with {noproc, rabbit_alarm}
    timer:sleep(Timeout + 500),

    try
        ?assert(rabbit_ct_broker_helpers:is_pid_registered_to_resource_alarms(Config, 0, Amqp10ReaderPid))
    catch
        _:{_,{noproc,{_,_,[rabbit_alarm,_,_,_]}}} ->
            ok
    end,

    ?assertNot(rabbit_ct_broker_helpers:is_process_alive(Config, Amqp10ReaderPid)),
    ok = amqp10_client:close_connection(Connection).

%% -------------------------------------------------------------------

get_amqp10_reader_pid(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_amqp10_reader_pid1, []).

get_amqp10_reader_pid1() ->
    lists:last(rabbit_amqp1_0:list()).

open_connection(Config, Name) ->
    Container = atom_to_binary(Name, utf8),
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    
    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, _Session} = amqp10_client:begin_session(Connection),
    Connection.


