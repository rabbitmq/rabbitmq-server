%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(signal_handling_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         send_sighup/1,
         send_sigterm/1,
         send_sigtstp/1
        ]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, signal_sent_to_pid_in_pidfile},
     {group, signal_sent_to_pid_from_os_getpid}
    ].

groups() ->
    Signals = [sighup,
               sigterm,
               sigtstp],
    Tests = [list_to_existing_atom(rabbit_misc:format("send_~s", [Signal]))
             || Signal <- Signals],
    [
     {signal_sent_to_pid_in_pidfile, [], Tests},
     {signal_sent_to_pid_from_os_getpid, [], Tests}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case os:type() of
        {unix, _} ->
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config);
        _ ->
            {skip, "This testsuite is only relevant on Unix"}
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 1,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [
                 {rmq_nodename_suffix, Testcase},
                 {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
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

send_sighup(Config) ->
    {PidFile, Pid} = get_pidfile_and_pid(Config),

    %% A SIGHUP signal should be ignored and the node should still be
    %% running.
    send_signal(Pid, "HUP"),
    timer:sleep(10000),
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ?assert(rabbit_ct_broker_helpers:rpc(Config, A, rabbit, is_running, [])),
    ?assert(filelib:is_regular(PidFile)).

send_sigterm(Config) ->
    {PidFile, Pid} = get_pidfile_and_pid(Config),

    %% After sending a SIGTERM to the process, we expect the node to
    %% exit.
    send_signal(Pid, "TERM"),
    wait_for_node_exit(Pid),

    %% After a clean exit, the PID file should be removed.
    ?assertNot(filelib:is_regular(PidFile)).

send_sigtstp(Config) ->
    {PidFile, Pid} = get_pidfile_and_pid(Config),

    %% A SIGHUP signal should be ignored and the node should still be
    %% running.
    send_signal(Pid, "TSTP"),
    timer:sleep(10000),
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ?assert(rabbit_ct_broker_helpers:rpc(Config, A, rabbit, is_running, [])),
    ?assert(filelib:is_regular(PidFile)).

get_pidfile_and_pid(Config) ->
    PidFile = rabbit_ct_broker_helpers:get_node_config(Config, 0, pid_file),
    ?assert(filelib:is_regular(PidFile)),

    %% We send the signal to either the process referenced in the
    %% PID file or the Erlang VM process. Up-to 3.8.x, they can be
    %% the different process because the PID file may reference the
    %% rabbitmq-server(8) script wrapper.
    [{name, Group} | _] = ?config(tc_group_properties, Config),
    Pid = case Group of
              signal_sent_to_pid_in_pidfile ->
                  {ok, P} = file:read_file(PidFile),
                  string:trim(P, trailing, [$\r,$\n]);
              signal_sent_to_pid_from_os_getpid ->
                  A = rabbit_ct_broker_helpers:get_node_config(
                        Config, 0, nodename),
                  rabbit_ct_broker_helpers:rpc(Config, A, os, getpid, [])
          end,
    {PidFile, Pid}.

send_signal(Pid, Signal) ->
    Cmd = ["kill",
           "-" ++ Signal,
           Pid],
    ?assertMatch({ok, _}, rabbit_ct_helpers:exec(Cmd)).

wait_for_node_exit(Pid) ->
    case rabbit_misc:is_os_process_alive(Pid) of
        true ->
            timer:sleep(1000),
            wait_for_node_exit(Pid);
        false ->
            ok
    end.
