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

-define(STATE, rabbit_stream_sac_coordinator).

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
        cgroup([consumer(self(), 0, true), consumer(self(), 1, false)]),
    State0 = state(#{GroupId => Group}),
    Monitors0 = #{},
    Command0 =
        register_consumer_command(<<"stream">>, -1, <<"app">>, self(), 0),
    {#?STATE{pids_groups = PidsGroups1} = State1, Monitors1, Effects1} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command0,
                                                      State0,
                                                      Monitors0,
                                                      []),
    assertSize(1, PidsGroups1),
    assertSize(1, maps:get(self(), PidsGroups1)),
    ?assertEqual(#{self() => {self(), sac}}, Monitors1),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects1),

    Command1 =
        register_consumer_command(<<"stream">>, -1, <<"app">>, self(), 1),

    {#?STATE{pids_groups = PidsGroups2} = State2, Monitors2, Effects2} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command1,
                                                      State1,
                                                      Monitors1,
                                                      []),
    assertSize(1, PidsGroups2),
    assertSize(1, maps:get(self(), PidsGroups2)),
    ?assertEqual(#{self() => {self(), sac}}, Monitors2),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects2),
    Group2 = cgroup([consumer(self(), 1, true)]),

    Command2 =
        unregister_consumer_command(<<"stream">>, <<"app">>, self(), 0),

    {#?STATE{pids_groups = PidsGroups3} = State3, Monitors3, Effects3} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command2,
                                                      State2#?STATE{groups =
                                                                        #{GroupId
                                                                              =>
                                                                              Group2}},
                                                      Monitors2,
                                                      []),
    assertSize(1, PidsGroups3),
    assertSize(1, maps:get(self(), PidsGroups3)),
    ?assertEqual(#{self() => {self(), sac}}, Monitors3),
    ?assertEqual([], Effects3),

    Command3 =
        unregister_consumer_command(<<"stream">>, <<"app">>, self(), 1),

    {#?STATE{pids_groups = PidsGroups4} = _State4, Monitors4, Effects4} =
        rabbit_stream_sac_coordinator:ensure_monitors(Command3,
                                                      State3#?STATE{groups =
                                                                        #{}},
                                                      Monitors3,
                                                      []),
    assertEmpty(PidsGroups4),
    assertEmpty(Monitors4),
    ?assertEqual([{demonitor, process, self()}], Effects4),

    ok.

handle_connection_down_test(_) ->
    GroupId = {<<"/">>, <<"stream">>, <<"app">>},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group =
        cgroup([consumer(Pid0, 0, true), consumer(Pid1, 1, false),
                consumer(Pid0, 2, false)]),
    State0 =
        state(#{GroupId => Group},
              #{Pid0 => sets:from_list([GroupId]),
                Pid1 => sets:from_list([GroupId])}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = State1,
     Effects1} =
        rabbit_stream_sac_coordinator:handle_connection_down(Pid0, State0),
    assertSize(1, PidsGroups1),
    assertSize(1, maps:get(Pid1, PidsGroups1)),
    ?assertEqual([{mod_call,
                   rabbit_stream_sac_coordinator,
                   send_message,
                   [Pid1,
                    {sac,
                     {{subscription_id, 1}, {active, true}, {extra, []}}}]}],
                 Effects1),
    ?assertEqual(#{GroupId => cgroup([consumer(Pid1, 1, true)])},
                 Groups1),
    {#?STATE{pids_groups = PidsGroups2, groups = Groups2} = _State2,
     Effects2} =
        rabbit_stream_sac_coordinator:handle_connection_down(Pid1, State1),
    assertEmpty(PidsGroups2),
    assertEmpty(Effects2),
    assertEmpty(Groups2),

    ok.

assertSize(Expected, []) ->
    ?assertEqual(Expected, 0);
assertSize(Expected, Map) when is_map(Map) ->
    ?assertEqual(Expected, maps:size(Map));
assertSize(Expected, List) when is_list(List) ->
    ?assertEqual(Expected, length(List));
assertSize(Expected, Other) ->
    case sets:is_set(Other) of
        true ->
            ?assertEqual(Expected, sets:size(Other))
    end.

assertEmpty(Data) ->
    assertSize(0, Data).

consumer(Pid, SubId, Active) ->
    #consumer{pid = Pid,
              subscription_id = SubId,
              active = Active}.

cgroup(Consumers) ->
    cgroup(-1, Consumers).

cgroup(PartitionIndex, Consumers) ->
    #group{partition_index = PartitionIndex, consumers = Consumers}.

state(Groups) ->
    state(Groups, #{}).

state(Groups, PidsGroups) ->
    #?STATE{groups = Groups, pids_groups = PidsGroups}.

register_consumer_command(Stream,
                          PartitionIndex,
                          ConsumerName,
                          ConnectionPid,
                          SubId) ->
    {register_consumer,
     <<"/">>,
     Stream,
     PartitionIndex,
     ConsumerName,
     ConnectionPid,
     SubId}.

unregister_consumer_command(Stream,
                            ConsumerName,
                            ConnectionPid,
                            SubId) ->
    {unregister_consumer,
     <<"/">>,
     Stream,
     ConsumerName,
     ConnectionPid,
     SubId}.
