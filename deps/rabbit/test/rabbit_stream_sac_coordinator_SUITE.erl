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
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_sac_coordinator_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/src/rabbit_stream_sac_coordinator.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

-define(STATE, rabbit_stream_sac_coordinator).
-define(MOD, rabbit_stream_sac_coordinator).

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
    ok = meck:new(rabbit_feature_flags),
    meck:expect(rabbit_feature_flags, is_enabled, fun (_) -> true end),
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

simple_sac_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    ConnectionPid = self(),
    GroupId = {<<"/">>, Stream, ConsumerName},
    Command0 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 0),
    State0 = state(),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers1}}} =
     State1,
     {ok, Active1}, Effects1} = ?MOD:apply(Command0, State0),
    ?assert(Active1),
    ?assertEqual([consumer(ConnectionPid, 0, active)], Consumers1),
    assertSendMessageEffect(ConnectionPid, 0, Stream, ConsumerName, true, Effects1),

    Command1 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers2}}} =
     State2,
     {ok, Active2}, Effects2} = ?MOD:apply(Command1, State1),
    ?assertNot(Active2),
    ?assertEqual([consumer(ConnectionPid, 0, active),
                  consumer(ConnectionPid, 1, waiting)],
                 Consumers2),
    assertEmpty(Effects2),

    Command2 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers3}}} =
     State3,
     {ok, Active3}, Effects3} = ?MOD:apply(Command2, State2),
    ?assertNot(Active3),
    ?assertEqual([consumer(ConnectionPid, 0, active),
                  consumer(ConnectionPid, 1, waiting),
                  consumer(ConnectionPid, 2, waiting)],
                 Consumers3),
    assertEmpty(Effects3),

    Command3 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 0),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers4}}} =
     State4,
     ok, Effects4} = ?MOD:apply(Command3, State3),
    ?assertEqual([consumer(ConnectionPid, 1, active),
                  consumer(ConnectionPid, 2, waiting)],
                 Consumers4),
    assertSendMessageEffect(ConnectionPid, 1, Stream, ConsumerName, true, Effects4),

    Command4 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers5}}} =
     State5,
     ok, Effects5} = ?MOD:apply(Command4, State4),
    ?assertEqual([consumer(ConnectionPid, 2, active)], Consumers5),
    assertSendMessageEffect(ConnectionPid, 2, Stream, ConsumerName, true, Effects5),

    Command5 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = Groups6}, ok, Effects6} = ?MOD:apply(Command5, State5),
    assertEmpty(Groups6),
    assertEmpty(Effects6),

    ok.

super_stream_partition_sac_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    ConnectionPid = self(),
    GroupId = {<<"/">>, Stream, ConsumerName},
    Command0 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 0),
    State0 = state(),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers1}}} =
     State1,
     {ok, Active1}, Effects1} = ?MOD:apply(Command0, State0),
    ?assert(Active1),
    ?assertEqual([consumer(ConnectionPid, 0, active)], Consumers1),
    assertSendMessageEffect(ConnectionPid, 0, Stream, ConsumerName, true, Effects1),

    Command1 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers2}}} =
     State2,
     {ok, Active2}, Effects2} = ?MOD:apply(Command1, State1),
    %% never active on registration
    ?assertNot(Active2),
    %% all consumers inactive, until the former active one steps down and activates the new consumer
    ?assertEqual([consumer(ConnectionPid, 0, deactivating),
                  consumer(ConnectionPid, 1, waiting)],
                 Consumers2),
    assertSendMessageSteppingDownEffect(ConnectionPid, 0, Stream, ConsumerName, Effects2),

    Command2 = activate_consumer_command(Stream, ConsumerName),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers3}}} =
     State3,
     ok, Effects3} = ?MOD:apply(Command2, State2),

    %% 1 (partition index) % 2 (consumer count) = 1 (active consumer index)
    ?assertEqual([consumer(ConnectionPid, 0, waiting),
                  consumer(ConnectionPid, 1, active)],
                 Consumers3),
    assertSendMessageEffect(ConnectionPid, 1, Stream, ConsumerName, true, Effects3),

    Command3 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers4}}} =
     State4,
     {ok, Active4}, Effects4} = ?MOD:apply(Command3, State3),
    %% never active on registration
    ?assertNot(Active4),
    %% 1 (partition index) % 3 (consumer count) = 1 (active consumer index)
    %% the active consumer stays the same
    ?assertEqual([consumer(ConnectionPid, 0, waiting),
                  consumer(ConnectionPid, 1, active),
                  consumer(ConnectionPid, 2, waiting)],
                 Consumers4),
    assertEmpty(Effects4),

    Command4 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 0),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers5}}} =
     State5,
     ok, Effects5} = ?MOD:apply(Command4, State4),
    %% 1 (partition index) % 2 (consumer count) = 1 (active consumer index)
    %% the active consumer will move from sub 1 to sub 2
    ?assertEqual([consumer(ConnectionPid, 1, deactivating),
                  consumer(ConnectionPid, 2, waiting)],
                 Consumers5),

    assertSendMessageSteppingDownEffect(ConnectionPid, 1, Stream, ConsumerName, Effects5),

    Command5 = activate_consumer_command(Stream, ConsumerName),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers6}}} =
     State6,
     ok, Effects6} = ?MOD:apply(Command5, State5),

    ?assertEqual([consumer(ConnectionPid, 1, waiting),
                  consumer(ConnectionPid, 2, active)],
                 Consumers6),
    assertSendMessageEffect(ConnectionPid, 2, Stream, ConsumerName, true, Effects6),

    Command6 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers7}}} =
     State7,
     ok, Effects7} = ?MOD:apply(Command6, State6),
    ?assertEqual([consumer(ConnectionPid, 2, active)], Consumers7),
    assertEmpty(Effects7),

    Command7 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = Groups8}, ok, Effects8} = ?MOD:apply(Command7, State7),
    assertEmpty(Groups8),
    assertEmpty(Effects8),

    ok.

ensure_monitors_test(_) ->
    GroupId = {<<"/">>, <<"stream">>, <<"app">>},
    Group = cgroup([consumer(self(), 0, true), consumer(self(), 1, false)]),
    State0 = state(#{GroupId => Group}, #{}),
    Monitors0 = #{},
    Command0 =
        register_consumer_command(<<"stream">>, -1, <<"app">>, self(), 0),
    {#?STATE{pids_groups = PidsGroups1} = State1, Monitors1, Effects1} =
        ?MOD:ensure_monitors(Command0,
                             State0,
                             Monitors0,
                             []),
    assertSize(1, PidsGroups1),
    assertSize(1, maps:get(self(), PidsGroups1)),
    ?assertEqual(#{self() => sac}, Monitors1),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects1),

    Command1 = register_consumer_command(<<"stream">>, -1, <<"app">>, self(), 1),

    {#?STATE{pids_groups = PidsGroups2} = State2, Monitors2, Effects2} =
        ?MOD:ensure_monitors(Command1,
                             State1,
                             Monitors1,
                             []),
    assertSize(1, PidsGroups2),
    assertSize(1, maps:get(self(), PidsGroups2)),
    ?assertEqual(#{self() => sac}, Monitors2),
    ?assertEqual([{monitor, process, self()}, {monitor, node, node()}],
                 Effects2),

    Group2 = cgroup([consumer(self(), 1, true)]),

    Command2 = unregister_consumer_command(<<"stream">>, <<"app">>, self(), 0),

    {#?STATE{pids_groups = PidsGroups3} = State3, Monitors3, Effects3} =
        ?MOD:ensure_monitors(Command2,
                             State2#?STATE{groups = #{GroupId => Group2}},
                             Monitors2,
                             []),
    assertSize(1, PidsGroups3),
    assertSize(1, maps:get(self(), PidsGroups3)),
    ?assertEqual(#{self() => sac}, Monitors3),
    ?assertEqual([], Effects3),

    %% trying with an unknown connection PID
    %% the function should not change anything
    UnknownConnectionPid = spawn(fun() -> ok end),
    PassthroughCommand = unregister_consumer_command(<<"stream">>,
                                                     <<"app">>,
                                                     UnknownConnectionPid,
                                                     0),

    {State3, Monitors3, Effects3} =
        ?MOD:ensure_monitors(PassthroughCommand,
                             State3,
                             Monitors3,
                             []),

    Command3 =
        unregister_consumer_command(<<"stream">>, <<"app">>, self(), 1),

    {#?STATE{pids_groups = PidsGroups4} = _State4, Monitors4, Effects4} =
        ?MOD:ensure_monitors(Command3,
                             State3#?STATE{groups = #{}},
                             Monitors3,
                             []),
    assertEmpty(PidsGroups4),
    assertEmpty(Monitors4),
    ?assertEqual([{demonitor, process, self()}], Effects4),

    ok.

handle_connection_down_sac_should_get_activated_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group = cgroup([consumer(Pid0, 0, active),
                    consumer(Pid1, 1, waiting),
                    consumer(Pid0, 2, waiting)]),
    State0 = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = State1,
     Effects1} = ?MOD:handle_connection_down(Pid0, State0),
    assertSize(1, PidsGroups1),
    assertSize(1, maps:get(Pid1, PidsGroups1)),
    assertSendMessageEffect(Pid1, 1, Stream, ConsumerName, true, Effects1),
    assertHasGroup(GroupId, cgroup([consumer(Pid1, 1, active)]), Groups1),
    {#?STATE{pids_groups = PidsGroups2, groups = Groups2},
     Effects2} = ?MOD:handle_connection_down(Pid1, State1),
    assertEmpty(PidsGroups2),
    assertEmpty(Effects2),
    assertEmpty(Groups2),

    ok.

handle_connection_down_sac_active_does_not_change_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group = cgroup([consumer(Pid1, 0, active),
                    consumer(Pid0, 1, waiting),
                    consumer(Pid0, 2, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId, cgroup([consumer(Pid1, 0, active)]), Groups),
    ok.

handle_connection_down_sac_no_more_consumers_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Group = cgroup([consumer(Pid0, 0, active),
                    consumer(Pid0, 1, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, State),
    assertEmpty(PidsGroups),
    assertEmpty(Groups),
    assertEmpty(Effects),
    ok.

handle_connection_down_sac_no_consumers_in_down_connection_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group = cgroup([consumer(Pid1, 0, active),
                    consumer(Pid1, 1, waiting)]),
    State = state(#{GroupId => Group},
                  #{Pid0 => maps:from_list([{GroupId, true}]), %% should not be there
                    Pid1 => maps:from_list([{GroupId, true}])}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, State),

    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId,
                   cgroup([consumer(Pid1, 0, active), consumer(Pid1, 1, waiting)]),
                   Groups),
    ok.

handle_connection_down_super_stream_active_stays_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group = cgroup(1, [consumer(Pid0, 0, waiting),
                       consumer(Pid0, 1, active),
                       consumer(Pid1, 2, waiting),
                       consumer(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid1, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid0, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId,
                   cgroup(1, [consumer(Pid0, 0, waiting),
                              consumer(Pid0, 1, active)]),
                   Groups),
    ok.

handle_connection_down_super_stream_active_changes_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group = cgroup(1, [consumer(Pid0, 0, waiting),
                       consumer(Pid1, 1, active),
                       consumer(Pid0, 2, waiting),
                       consumer(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} =
    ?MOD:handle_connection_down(Pid0, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertSendMessageSteppingDownEffect(Pid1, 1, Stream, ConsumerName, Effects),
    assertHasGroup(GroupId,
                   cgroup(1, [consumer(Pid1, 1, deactivating),
                              consumer(Pid1, 3, waiting)]),
                   Groups),
    ok.

handle_connection_down_super_stream_activate_in_remaining_connection_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group = cgroup(1, [consumer(Pid0, 0, waiting),
                       consumer(Pid0, 1, active),
                       consumer(Pid1, 2, waiting),
                       consumer(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertSendMessageEffect(Pid1, 3, Stream, ConsumerName, true, Effects),
    assertHasGroup(GroupId, cgroup(1, [consumer(Pid1, 2, waiting),
                                       consumer(Pid1, 3, active)]),
                   Groups),
    ok.

handle_connection_down_super_stream_no_active_removed_or_present_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    %% this is a weird case that should not happen in the wild,
    %% we test the logic in the code nevertheless.
    %% No active consumer in the group
    Group = cgroup(1, [consumer(Pid0, 0, waiting),
                       consumer(Pid0, 1, waiting),
                       consumer(Pid1, 2, waiting),
                       consumer(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId, cgroup(1, [consumer(Pid1, 2, waiting),
                                       consumer(Pid1, 3, waiting)]),
                   Groups),
    ok.

register_consumer_with_different_partition_index_should_return_error_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    ConnectionPid = self(),
    Command0 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 0),
    State0 = state(),
    {State1, {ok, true}, _} =
        rabbit_stream_sac_coordinator:apply(Command0, State0),
    Command1 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 1),
    {_, {error, partition_index_conflict}, []} =
        rabbit_stream_sac_coordinator:apply(Command1, State1).

handle_connection_down_consumers_from_dead_connection_should_be_filtered_out_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Group = cgroup(1, [consumer(Pid0, 0, waiting),
                       consumer(Pid1, 1, active),
                       consumer(Pid2, 2, waiting)]),
    State0 = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = State1,
     Effects1} =
    ?MOD:handle_connection_down(Pid0, State0),
    assertSize(2, PidsGroups1),
    assertSize(1, maps:get(Pid1, PidsGroups1)),
    assertSize(1, maps:get(Pid2, PidsGroups1)),
    assertSendMessageSteppingDownEffect(Pid1, 1, Stream, ConsumerName, Effects1),
    assertHasGroup(GroupId,
                   cgroup(1, [consumer(Pid1, 1, deactivating),
                              consumer(Pid2, 2, waiting)]),
                   Groups1),

    {#?STATE{pids_groups = PidsGroups2, groups = Groups2},
     Effects2} = ?MOD:handle_connection_down(Pid1, State1),
    assertSize(1, PidsGroups2),
    assertSize(1, maps:get(Pid2, PidsGroups2)),
    assertSendMessageEffect(Pid2, 2, Stream, ConsumerName, true, Effects2),
    assertHasGroup(GroupId,
                   cgroup(1, [consumer(Pid2, 2, active)]),
                   Groups2),

    ok.

import_state_v4_empty_test(_) ->
    OldMod = rabbit_stream_sac_coordinator_v4,
    OldState = OldMod:init_state(),
    Export = OldMod:state_to_map(OldState),
    ?assertEqual(#?STATE{groups = #{}, pids_groups = #{}},
                 ?MOD:import_state(4, Export)),
    ok.

import_state_v4_test(_) ->
    OldMod = rabbit_stream_sac_coordinator_v4,
    OldState0 = OldMod:init_state(),
    Pid0 = self(),
    Pid1  = spawn(fun() -> ok end),
    Pid2  = spawn(fun() -> ok end),
    S = <<"stream">>,
    App0 = <<"app-0">>,
    Cmd0 = register_consumer_command(S, -1, App0, Pid0, 0),
    OldState1 = apply_ensure_monitors(OldMod, Cmd0, OldState0),
    Cmd1 = register_consumer_command(S, -1, App0, Pid1, 1),
    OldState2 = apply_ensure_monitors(OldMod, Cmd1, OldState1),
    Cmd2 = register_consumer_command(S, -1, App0, Pid2, 2),
    OldState3 = apply_ensure_monitors(OldMod, Cmd2, OldState2),

    P = <<"stream-1">>,
    App1 = <<"app-1">>,
    Cmd3 = register_consumer_command(P, 1, App1, Pid0, 0),
    OldState4 = apply_ensure_monitors(OldMod, Cmd3, OldState3),
    Cmd4 = register_consumer_command(P, 1, App1, Pid1, 1),
    OldState5 = apply_ensure_monitors(OldMod, Cmd4, OldState4),
    Cmd5 = register_consumer_command(P, 1, App1, Pid2, 2),
    OldState6 = apply_ensure_monitors(OldMod, Cmd5, OldState5),
    Cmd6 = activate_consumer_command(P, App1),
    OldState7 = apply_ensure_monitors(OldMod, Cmd6, OldState6),

    Export = OldMod:state_to_map(OldState7),
    #?STATE{groups = Groups, pids_groups = PidsGroups} = ?MOD:import_state(4, Export),
    assertHasGroup({<<"/">>, S, App0},
                   cgroup(-1, [consumer(Pid0, 0, active),
                               consumer(Pid1, 1, waiting),
                               consumer(Pid2, 2, waiting)]),
                   Groups),

    assertHasGroup({<<"/">>, P, App1},
                   cgroup(1, [consumer(Pid0, 0, waiting),
                              consumer(Pid1, 1, active),
                              consumer(Pid2, 2, waiting)]),
                   Groups),
    assertSize(3, PidsGroups),
    assertSize(2, maps:get(Pid0, PidsGroups)),
    assertSize(2, maps:get(Pid1, PidsGroups)),
    assertSize(2, maps:get(Pid2, PidsGroups)),

    ok.

handle_connection_node_disconnected_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Group = cgroup(1, [consumer(Pid0, 0, waiting),
                       consumer(Pid1, 1, active),
                       consumer(Pid2, 2, waiting)]),
    State0 = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = _State1,
     [Effect1]} =
    ?MOD:handle_connection_node_disconnected(Pid1, State0),
    assertSize(2, PidsGroups1),
    assertSize(1, maps:get(Pid0, PidsGroups1)),
    assertSize(1, maps:get(Pid2, PidsGroups1)),
    ?assertEqual({timer, {sac, node_disconnected, #{connection_pid => Pid1}},
                  60_000},
                 Effect1),
    assertHasGroup(GroupId,
                   cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                              consumer(Pid1, 1, {disconnected, active}),
                              consumer(Pid2, 2, {connected, waiting})]),
                   Groups1),
    ok.

handle_node_reconnected_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    CName = <<"app">>,

    S0 = <<"s0">>,
    GId0 = {<<"/">>, S0, CName},
    Group0 = cgroup(0, [consumer(Pid0, 0, {connected, active}),
                        consumer(Pid1, 1, {disconnected, waiting}),
                        consumer(Pid2, 2, {connected, waiting})]),

    S1 = <<"s1">>,
    GId1 = {<<"/">>, S1, CName},
    Group1 = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                        consumer(Pid1, 1, {disconnected, active}),
                        consumer(Pid2, 2, {connected, waiting})]),

    S2 = <<"s2">>,
    GId2 = {<<"/">>, S2, CName},
    Group2 = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                        consumer(Pid1, 1, {disconnected, waiting}),
                        consumer(Pid2, 2, {connected, active})]),

    Groups0 = #{GId0 => Group0,
                GId1 => Group1,
                GId2 => Group2},
    %% Pid2 is missing from PIDs to groups dependency mapping
    State0 = state(Groups0,
                   #{Pid0 => #{GId0 => true, GId1 => true, GId2 => true},
                     Pid2 => #{GId0 => true, GId1 => true, GId2 => true}}),
    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = _State1,
     Effects1} =
    ?MOD:handle_node_reconnected(State0, []),

    ?assertEqual(Groups0, Groups1),
    ?assertEqual(#{Pid0 => #{GId0 => true, GId1 => true, GId2 => true},
                   Pid1 => #{GId0 => true, GId1 => true, GId2 => true},
                   Pid2 => #{GId0 => true, GId1 => true, GId2 => true}},
                 PidsGroups1),

    ?assertEqual([{mod_call,rabbit_stream_sac_coordinator,send_message,
                   [Pid1,{sac,check_connection,#{}}]},
                  {monitor, process, Pid1},
                  {monitor, node, node(Pid1)}],
                 Effects1),

    ok.

connection_reconnected_simple_disconnected_becomes_connected_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {disconnected, active}),
                    consumer(Pid1, 1, {connected, waiting}),
                    consumer(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {connected, active}),
                                consumer(Pid1, 1, {connected, waiting}),
                                consumer(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_simple_active_should_be_first_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    %% disconnected for a while, got first in consumer array
    %% because consumers arrived and left
    Group = cgroup([consumer(Pid0, 0, {disconnected, waiting}),
                    consumer(Pid1, 1, {connected, active}),
                    consumer(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, cgroup([consumer(Pid1, 1, {connected, active}),
                                consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_super_disconnected_becomes_connected_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {disconnected, waiting}),
                       consumer(Pid1, 1, {connected, waiting}),
                       consumer(Pid2, 2, {connected, active})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                                   consumer(Pid1, 1, {connected, waiting}),
                                   consumer(Pid2, 2, {connected, deactivating})]),
                   Groups1),

    assertSendMessageSteppingDownEffect(Pid2, 2, stream(), name(), Eff),
    ok.

forget_connection_simple_disconnected_becomes_forgotten_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {disconnected, active}),
                    consumer(Pid1, 1, {connected, waiting}),
                    consumer(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:forget_connection(Pid0, State0),

    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {forgotten, active}),
                                consumer(Pid1, 1, {connected, active}),
                                consumer(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertSendMessageEffect(Pid1, 1, stream(), name(), true, Eff),
    ok.

forget_connection_super_stream_disconnected_becomes_forgotten_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                       consumer(Pid1, 1, {disconnected, active}),
                       consumer(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:forget_connection(Pid1, State0),

    assertHasGroup(GId, cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                                   consumer(Pid1, 1, {forgotten, active}),
                                   consumer(Pid2, 2, {connected, active})]),
                   Groups1),

    assertSendMessageEffect(Pid2, 2, stream(), name(), true, Eff),
    ok.

register_consumer_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {connected, waiting}),
                    consumer(Pid1, 1, {disconnected, active}),
                    consumer(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = register_consumer_command(stream(), -1, name(), Pid0, 3),
    {#?STATE{groups = Groups1}, {ok, false}, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid1, 1, {disconnected, active}),
                                consumer(Pid0, 2, {connected, waiting}),
                                consumer(Pid0, 3, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

register_consumer_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                       consumer(Pid1, 1, {disconnected, active}),
                       consumer(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = register_consumer_command(stream(), -1, name(), Pid0, 3),
    {#?STATE{groups = Groups1}, {ok, false}, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                                   consumer(Pid1, 1, {disconnected, active}),
                                   consumer(Pid0, 2, {connected, waiting}),
                                   consumer(Pid0, 3, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

unregister_consumer_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {connected, waiting}),
                    consumer(Pid1, 1, {disconnected, active}),
                    consumer(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = unregister_consumer_command(stream(), name(), Pid0, 2),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid1, 1, {disconnected, active})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

unregister_consumer_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                       consumer(Pid1, 1, {disconnected, active}),
                       consumer(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = unregister_consumer_command(stream(), name(), Pid0, 0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, cgroup(1, [consumer(Pid1, 1, {disconnected, active}),
                                   consumer(Pid0, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

activate_consumer_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {connected, waiting}),
                    consumer(Pid1, 1, {disconnected, active}),
                    consumer(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid1, 1, {disconnected, active}),
                                consumer(Pid0, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

active_consumer_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                       consumer(Pid1, 1, {disconnected, active}),
                       consumer(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                                   consumer(Pid1, 1, {disconnected, active}),
                                   consumer(Pid0, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

handle_connection_down_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {connected, waiting}),
                    consumer(Pid1, 0, {disconnected, active}),
                    consumer(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} = ?MOD:handle_connection_down(Pid2, State0),
    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid1, 0, {disconnected, active})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

handle_connection_down_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                       consumer(Pid1, 0, {disconnected, active}),
                       consumer(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} = ?MOD:handle_connection_down(Pid0, State0),
    assertHasGroup(GId, cgroup(1, [consumer(Pid1, 0, {disconnected, active}),
                                   consumer(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

handle_connection_node_disconnected_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {connected, waiting}),
                    consumer(Pid1, 0, {disconnected, active}),
                    consumer(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} =
    ?MOD:handle_connection_node_disconnected(Pid2, State0),
    assertHasGroup(GId, cgroup([consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid1, 0, {disconnected, active}),
                                consumer(Pid2, 0, {disconnected, waiting})]),
                   Groups1),
    assertNodeDisconnectedTimerEffect(Pid2, Eff),
    ok.

handle_connection_node_disconnected_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {connected, waiting}),
                       consumer(Pid1, 0, {disconnected, active}),
                       consumer(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} =
    ?MOD:handle_connection_node_disconnected(Pid0, State0),
    assertHasGroup(GId, cgroup(1, [consumer(Pid0, 0, {disconnected, waiting}),
                                   consumer(Pid1, 0, {disconnected, active}),
                                   consumer(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertNodeDisconnectedTimerEffect(Pid0, Eff),
    ok.

connection_reconnected_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, 0, {disconnected, waiting}),
                    consumer(Pid1, 0, {disconnected, active}),
                    consumer(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, cgroup([consumer(Pid1, 0, {disconnected, active}),
                                consumer(Pid0, 0, {connected, waiting}),
                                consumer(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, 0, {disconnected, active}),
                       consumer(Pid1, 0, {disconnected, waiting}),
                       consumer(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = connection_reconnected_command(Pid1),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, cgroup(1, [consumer(Pid0, 0, {disconnected, active}),
                                   consumer(Pid1, 0, {connected, waiting}),
                                   consumer(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

forget_connection_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup([consumer(Pid0, {disconnected, waiting}),
                    consumer(Pid1, {connected, waiting}),
                    consumer(Pid2, {disconnected, active})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:forget_connection(Pid0, State0),

    assertHasGroup(GId, cgroup([consumer(Pid2, {disconnected, active}),
                                consumer(Pid0, {forgotten, waiting}),
                                consumer(Pid1, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

forget_connection_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = spawn(fun() -> ok end),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    GId = group_id(),
    Group = cgroup(1, [consumer(Pid0, {disconnected, waiting}),
                       consumer(Pid1, {connected, waiting}),
                       consumer(Pid2, {disconnected, active})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:forget_connection(Pid0, State0),

    assertHasGroup(GId, cgroup(1, [consumer(Pid0, {forgotten, waiting}),
                                   consumer(Pid1, {connected, waiting}),
                                   consumer(Pid2, {disconnected, active})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

purge_nodes_test(_) ->
    N0 = node(),
    {ok, N1Pid, N1} = peer:start(#{
        name => ?FUNCTION_NAME,
        connection => standard_io,
        shutdown => close
    }),

    N0P0 = spawn(N0, fun() -> ok end),
    N0P1 = spawn(N0, fun() -> ok end),
    N0P2 = spawn(N0, fun() -> ok end),
    N1P0 = spawn(N1, fun() -> ok end),
    N1P1 = spawn(N1, fun() -> ok end),
    N1P2 = spawn(N1, fun() -> ok end),

    S0 = <<"s0">>,
    S1 = <<"s1">>,
    S2 = <<"s2">>,

    GId0 = group_id(S0),
    GId1 = group_id(S1),
    GId2 = group_id(S2),

    Group0 = cgroup([consumer(N1P0, {disconnected, active}),
                     consumer(N0P1, {connected, waiting}),
                     consumer(N0P2, {connected, waiting})]),

    Group1 = cgroup(1, [consumer(N1P1, {disconnected, waiting}),
                        consumer(N1P2, {disconnected, active}),
                        consumer(N0P0, {connected, waiting})]),

    Group2 = cgroup([consumer(N0P0, {connected, active}),
                     consumer(N0P1, {connected, waiting}),
                     consumer(N0P2, {connected, waiting})]),


    State0 = state(#{GId0 => Group0, GId1 => Group1, GId2 => Group2}),
    Cmd = purge_nodes_command([N1]),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertSize(3, Groups1),
    assertHasGroup(GId0, cgroup([consumer(N0P1, {connected, active}),
                                 consumer(N0P2, {connected, waiting})]),
                   Groups1),
    assertHasGroup(GId1, cgroup(1, [consumer(N0P0, {connected, active})]),
                   Groups1),
    assertHasGroup(GId2, cgroup([consumer(N0P0, {connected, active}),
                                 consumer(N0P1, {connected, waiting}),
                                 consumer(N0P2, {connected, waiting})]),
                   Groups1),

    assertSize(2, Eff),
    assertContainsSendMessageEffect(N0P1, S0, true, Eff),
    assertContainsSendMessageEffect(N0P0, S1, true, Eff),

    _ = peer:stop(N1Pid),
    ok.

group_id() ->
    group_id(stream()).

group_id(S) ->
    {<<"/">>, S, name()}.

stream() ->
    <<"sO">>.

name() ->
    <<"app">>.

apply_ensure_monitors(Mod, Cmd, State0) ->
    {State1, _, _} = Mod:apply(Cmd, State0),
    {State2, _, _} = Mod:ensure_monitors(Cmd, State1, #{}, []),
    State2.

assertSize(Expected, []) ->
    ?assertEqual(Expected, 0);
assertSize(Expected, Map) when is_map(Map) ->
    ?assertEqual(Expected, maps:size(Map));
assertSize(Expected, List) when is_list(List) ->
    ?assertEqual(Expected, length(List)).

assertEmpty(Data) ->
    assertSize(0, Data).

assertHasGroup(GroupId, Group, Groups) ->
    G = maps:get(GroupId, Groups),
    ?assertEqual(Group, G).

consumer(Pid, Status) ->
    consumer(Pid, 0, Status).

consumer(Pid, SubId, {Connectivity, Status}) ->
    #consumer{pid = Pid,
              subscription_id = SubId,
              owner = <<"owning connection label">>,
              status = {Connectivity, Status}};
consumer(Pid, SubId, Status) ->
    consumer(Pid, SubId, {connected, Status}).

cgroup(Consumers) ->
    cgroup(-1, Consumers).

cgroup(PartitionIndex, Consumers) ->
    #group{partition_index = PartitionIndex, consumers = Consumers}.

state() ->
    state(#{}).

state(Groups) ->
    state(Groups, ?MOD:compute_pid_group_dependencies(Groups)).

state(Groups, PidsGroups) ->
    #?STATE{groups = Groups, pids_groups = PidsGroups}.

register_consumer_command(Stream,
                          PartitionIndex,
                          ConsumerName,
                          ConnectionPid,
                          SubId) ->
    #command_register_consumer{vhost = <<"/">>,
                               stream = Stream,
                               partition_index = PartitionIndex,
                               consumer_name = ConsumerName,
                               connection_pid = ConnectionPid,
                               owner = <<"owning connection label">>,
                               subscription_id = SubId}.

unregister_consumer_command(Stream,
                            ConsumerName,
                            ConnectionPid,
                            SubId) ->
    #command_unregister_consumer{vhost = <<"/">>,
                                 stream = Stream,
                                 consumer_name = ConsumerName,
                                 connection_pid = ConnectionPid,
                                 subscription_id = SubId}.

activate_consumer_command(Stream, ConsumerName) ->
    #command_activate_consumer{vhost = <<"/">>,
                               stream = Stream,
                               consumer_name = ConsumerName}.

connection_reconnected_command(Pid) ->
    #command_connection_reconnected{pid = Pid}.

purge_nodes_command(Nodes) ->
    #command_purge_nodes{nodes = Nodes}.


assertContainsSendMessageEffect(Pid, Stream, Active, Effects) ->
    assertContainsSendMessageEffect(Pid, 0, Stream, name(), Active, Effects).

assertContainsSendMessageEffect(Pid, SubId, Stream, ConsumerName, Active,
                                Effects) ->
    Contains = lists:any(fun(Eff) ->
                                 Eff =:= {mod_call,
                                          rabbit_stream_sac_coordinator,
                                          send_message,
                                          [Pid,
                                           {sac,
                                            #{subscription_id => SubId,
                                              stream => Stream,
                                              consumer_name => ConsumerName,
                                              active => Active}}]}
              end, Effects),
    ?assert(Contains).


assertSendMessageEffect(Pid, SubId, Stream, ConsumerName, Active, [Effect]) ->
    ?assertEqual({mod_call,
                  rabbit_stream_sac_coordinator,
                  send_message,
                  [Pid,
                   {sac,
                    #{subscription_id => SubId,
                      stream => Stream,
                      consumer_name => ConsumerName,
                      active => Active}
                    }]},
                 Effect).

assertSendMessageSteppingDownEffect(Pid, SubId, Stream, ConsumerName, [Effect]) ->
    ?assertEqual({mod_call,
                  rabbit_stream_sac_coordinator,
                  send_message,
                  [Pid,
                   {sac,
                    #{subscription_id => SubId,
                      stream => Stream,
                      consumer_name => ConsumerName,
                      active => false,
                      stepping_down => true}}]},
                 Effect).

assertNodeDisconnectedTimerEffect(Pid, [Effect]) ->
    ?assertMatch({timer,
                  {sac, node_disconnected, #{connection_pid := Pid}},
                  _},
                 Effect).
