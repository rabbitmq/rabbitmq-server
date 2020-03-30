%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_disk_monitor_mocks_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          disk_monitor,
          disk_monitor_enable
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

disk_monitor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, disk_monitor1, [Config]).

disk_monitor1(_Config) ->
    %% Issue: rabbitmq-server #91
    %% os module could be mocked using 'unstick', however it may have undesired
    %% side effects in following tests. Thus, we mock at rabbit_misc level
    ok = meck:new(rabbit_misc, [passthrough]),
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> "\n" end),
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [1000]),
    meck:unload(rabbit_misc),
    passed.

disk_monitor_enable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, disk_monitor_enable1, [Config]).

disk_monitor_enable1(_Config) ->
    ok = meck:new(rabbit_misc, [passthrough]),
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> "\n" end),
    application:set_env(rabbit, disk_monitor_failure_retries, 20000),
    application:set_env(rabbit, disk_monitor_failure_retry_interval, 100),
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [1000]),
    undefined = rabbit_disk_monitor:get_disk_free(),
    Cmd = case os:type() of
              {win32, _} -> " Le volume dans le lecteur C n’a pas de nom.\n"
                            " Le numéro de série du volume est 707D-5BDC\n"
                            "\n"
                            " Répertoire de C:\Users\n"
                            "\n"
                            "10/12/2015  11:01    <DIR>          .\n"
                            "10/12/2015  11:01    <DIR>          ..\n"
                            "               0 fichier(s)                0 octets\n"
                            "               2 Rép(s)  758537121792 octets libres\n";
              _          -> "Filesystem 1024-blocks      Used Available Capacity  iused     ifree %iused  Mounted on\n"
                            "/dev/disk1   975798272 234783364 740758908    25% 58759839 185189727   24%   /\n"
          end,
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> Cmd end),
    timer:sleep(1000),
    Bytes = 740758908 * 1024,
    Bytes = rabbit_disk_monitor:get_disk_free(),
    meck:unload(rabbit_misc),
    application:set_env(rabbit, disk_monitor_failure_retries, 10),
    application:set_env(rabbit, disk_monitor_failure_retry_interval, 120000),
    passed.
