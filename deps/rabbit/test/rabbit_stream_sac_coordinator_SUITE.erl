%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_sac_coordinator_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/src/rabbit_stream_sac_coordinator.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

%% replicate eunit like test resolution
all_tests() ->
    [F
     || {F, _} <- ?MODULE:module_info(functions),
        re:run(atom_to_list(F), "_test$") /= nomatch].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

ensure_monitors_test(_) ->
    GroupId = {<<"/">>, <<"stream">>, <<"app">>},
    Group =
        #group{partition_index = -1,
               consumers =
                   [#consumer{pid = self(),
                              subscription_id = 0,
                              active = true},
                    #consumer{pid = self(),
                              subscription_id = 1,
                              active = false}]},
    State0 =
        #rabbit_stream_sac_coordinator{groups = #{GroupId => Group},
                                       pids_groups = #{}},
    Monitors0 = #{},
    Command0 =
        {register_consumer, <<"/">>, <<"stream">>, -1, <<"app">>, self(), 0},
    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups1} = State1,
     Monitors1, Effects1} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command0,
                                                      State0,
                                                      Monitors0,
                                                      []),
    ?assertEqual(1, maps:size(PidsGroups1)),
    ?assertEqual(1,
                 sets:size(
                     maps:get(self(), PidsGroups1))),
    ?assertEqual(#{self() => {self(), sac}}, Monitors1),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects1),
    Command1 =
        {register_consumer, <<"/">>, <<"stream">>, -1, <<"app">>, self(), 1},

    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups2} = State2,
     Monitors2, Effects2} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command1,
                                                      State1,
                                                      Monitors1,
                                                      []),
    ?assertEqual(1, maps:size(PidsGroups2)),
    ?assertEqual(1,
                 sets:size(
                     maps:get(self(), PidsGroups2))),
    ?assertEqual(#{self() => {self(), sac}}, Monitors2),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects2),
    Group2 =
        #group{partition_index = -1,
               consumers =
                   [#consumer{pid = self(),
                              subscription_id = 1,
                              active = true}]},

    Command2 =
        {unregister_consumer, <<"/">>, <<"stream">>, <<"app">>, self(), 0},

    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups3} = State3,
     Monitors3, Effects3} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command2,
                                                      State2#rabbit_stream_sac_coordinator{groups
                                                                                               =
                                                                                               #{GroupId
                                                                                                     =>
                                                                                                     Group2}},
                                                      Monitors2,
                                                      []),
    ?assertEqual(1, maps:size(PidsGroups3)),
    ?assertEqual(1,
                 sets:size(
                     maps:get(self(), PidsGroups3))),
    ?assertEqual(#{self() => {self(), sac}}, Monitors3),
    ?assertEqual([], Effects3),

    Command3 =
        {unregister_consumer, <<"/">>, <<"stream">>, <<"app">>, self(), 1},

    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups4} = _State4,
     Monitors4, Effects4} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command3,
                                                      State3#rabbit_stream_sac_coordinator{groups
                                                                                               =
                                                                                               #{}},
                                                      Monitors3,
                                                      []),
    ?assertEqual(0, maps:size(PidsGroups4)),
    ?assertEqual(0, maps:size(Monitors4)),
    ?assertEqual([{demonitor, process, self()}], Effects4),

    ok.

handle_connection_down_test(_) ->
    GroupId = {<<"/">>, <<"stream">>, <<"app">>},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group =
        #group{partition_index = -1,
               consumers =
                   [#consumer{pid = Pid0,
                              subscription_id = 0,
                              active = true},
                    #consumer{pid = Pid1,
                              subscription_id = 1,
                              active = false},
                    #consumer{pid = Pid0,
                              subscription_id = 2,
                              active = false}]},
    State0 =
        #rabbit_stream_sac_coordinator{groups = #{GroupId => Group},
                                       pids_groups =
                                           #{Pid0 => sets:from_list([GroupId]),
                                             Pid1 =>
                                                 sets:from_list([GroupId])}},

    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups1,
                                    groups = Groups1} =
         State1,
     Effects1} =
        rabbit_stream_sac_coordinator:handle_connection_down(Pid0, State0),
    ?assertEqual(1, maps:size(PidsGroups1)),
    ?assertEqual(1,
                 sets:size(
                     maps:get(Pid1, PidsGroups1))),
    ?assertEqual([{mod_call,
                   rabbit_stream_sac_coordinator,
                   send_message,
                   [Pid1,
                    {sac,
                     {{subscription_id, 1}, {active, true}, {extra, []}}}]}],
                 Effects1),
    ?assertEqual(#{GroupId =>
                       #group{partition_index = -1,
                              consumers =
                                  [#consumer{pid = Pid1,
                                             subscription_id = 1,
                                             active = true}]}},
                 Groups1),
    {#rabbit_stream_sac_coordinator{pids_groups = PidsGroups2,
                                    groups = Groups2} =
         _State2,
     Effects2} =
        rabbit_stream_sac_coordinator:handle_connection_down(Pid1, State1),
    ?assertEqual(0, maps:size(PidsGroups2)),
    ?assertEqual([], Effects2),
    ?assertEqual(0, maps:size(Groups2)),

    ok.
