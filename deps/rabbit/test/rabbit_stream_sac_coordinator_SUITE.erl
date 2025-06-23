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

check_conf_test(_) ->
    K = disconnected_timeout,
    Def = 60_000,
    ?assertMatch({new, #{K := Def}},
                 ?MOD:check_conf_change(state_with_conf(#{}))),
    ?assertMatch({new, #{K := Def}},
                 ?MOD:check_conf_change(state_with_conf(#{K => 42}))),
    ?assertMatch(unchanged,
                 ?MOD:check_conf_change(state_with_conf(#{K => Def}))),
    ?assertMatch(unchanged,
                 ?MOD:check_conf_change(#{K => Def})),
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
    assertCsrsEqual([csr(ConnectionPid, 0, active)], Consumers1),
    assertSendMessageActivateEffect(ConnectionPid, 0, Stream, ConsumerName, true, Effects1),

    Command1 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers2}}} =
     State2,
     {ok, Active2}, Effects2} = ?MOD:apply(Command1, State1),
    ?assertNot(Active2),
    assertCsrsEqual([csr(ConnectionPid, 0, active),
                     csr(ConnectionPid, 1, waiting)],
                    Consumers2),
    assertEmpty(Effects2),

    Command2 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers3}}} =
     State3,
     {ok, Active3}, Effects3} = ?MOD:apply(Command2, State2),
    ?assertNot(Active3),
    assertCsrsEqual([csr(ConnectionPid, 0, active),
                     csr(ConnectionPid, 1, waiting),
                     csr(ConnectionPid, 2, waiting)],
                    Consumers3),
    assertEmpty(Effects3),

    Command3 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 0),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers4}}} =
     State4,
     ok, Effects4} = ?MOD:apply(Command3, State3),
    assertCsrsEqual([csr(ConnectionPid, 1, active),
                     csr(ConnectionPid, 2, waiting)],
                    Consumers4),
    assertSendMessageActivateEffect(ConnectionPid, 1, Stream, ConsumerName, true, Effects4),

    Command4 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers5}}} =
     State5,
     ok, Effects5} = ?MOD:apply(Command4, State4),
    assertCsrsEqual([csr(ConnectionPid, 2, active)], Consumers5),
    assertSendMessageActivateEffect(ConnectionPid, 2, Stream, ConsumerName, true, Effects5),

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
    assertCsrsEqual([csr(ConnectionPid, 0, active)], Consumers1),
    assertSendMessageActivateEffect(ConnectionPid, 0, Stream, ConsumerName, true, Effects1),

    Command1 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers2}}} =
     State2,
     {ok, Active2}, Effects2} = ?MOD:apply(Command1, State1),
    %% never active on registration
    ?assertNot(Active2),
    %% all consumers inactive, until the former active one steps down and activates the new consumer
    assertCsrsEqual([csr(ConnectionPid, 0, deactivating),
                     csr(ConnectionPid, 1, waiting)],
                    Consumers2),
    assertSendMessageSteppingDownEffect(ConnectionPid, 0, Stream, ConsumerName, Effects2),

    Command2 = activate_consumer_command(Stream, ConsumerName),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers3}}} =
     State3,
     ok, Effects3} = ?MOD:apply(Command2, State2),

    %% 1 (partition index) % 2 (consumer count) = 1 (active consumer index)
    assertCsrsEqual([csr(ConnectionPid, 0, waiting),
                     csr(ConnectionPid, 1, active)],
                    Consumers3),
    assertSendMessageActivateEffect(ConnectionPid, 1, Stream, ConsumerName, true, Effects3),

    Command3 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers4}}} =
     State4,
     {ok, Active4}, Effects4} = ?MOD:apply(Command3, State3),
    %% never active on registration
    ?assertNot(Active4),
    %% 1 (partition index) % 3 (consumer count) = 1 (active consumer index)
    %% the active consumer stays the same
    assertCsrsEqual([csr(ConnectionPid, 0, waiting),
                     csr(ConnectionPid, 1, active),
                     csr(ConnectionPid, 2, waiting)],
                    Consumers4),
    assertEmpty(Effects4),

    Command4 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 0),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers5}}} =
     State5,
     ok, Effects5} = ?MOD:apply(Command4, State4),
    %% 1 (partition index) % 2 (consumer count) = 1 (active consumer index)
    %% the active consumer will move from sub 1 to sub 2
    assertCsrsEqual([csr(ConnectionPid, 1, deactivating),
                     csr(ConnectionPid, 2, waiting)],
                    Consumers5),

    assertSendMessageSteppingDownEffect(ConnectionPid, 1, Stream, ConsumerName, Effects5),

    Command5 = activate_consumer_command(Stream, ConsumerName),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers6}}} =
     State6,
     ok, Effects6} = ?MOD:apply(Command5, State5),

    assertCsrsEqual([csr(ConnectionPid, 1, waiting),
                     csr(ConnectionPid, 2, active)],
                    Consumers6),
    assertSendMessageActivateEffect(ConnectionPid, 2, Stream, ConsumerName, true, Effects6),

    Command6 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers7}}} =
     State7,
     ok, Effects7} = ?MOD:apply(Command6, State6),
    assertCsrsEqual([csr(ConnectionPid, 2, active)], Consumers7),
    assertEmpty(Effects7),

    Command7 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = Groups8}, ok, Effects8} = ?MOD:apply(Command7, State7),
    assertEmpty(Groups8),
    assertEmpty(Effects8),

    ok.

ensure_monitors_test(_) ->
    GroupId = {<<"/">>, <<"stream">>, <<"app">>},
    Group = grp([csr(self(), 0, true), csr(self(), 1, false)]),
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

    Group2 = grp([csr(self(), 1, true)]),

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
    UnknownConnectionPid = new_process(),
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
    Pid1 = new_process(),
    Group = grp([csr(Pid0, 0, active),
                 csr(Pid1, 1, waiting),
                 csr(Pid0, 2, waiting)]),
    State0 = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = State1,
     Effects1} = ?MOD:handle_connection_down(Pid0, normal, State0),
    assertSize(1, PidsGroups1),
    assertSize(1, maps:get(Pid1, PidsGroups1)),
    assertSendMessageActivateEffect(Pid1, 1, Stream, ConsumerName, true, Effects1),
    assertHasGroup(GroupId, grp([csr(Pid1, 1, active)]), Groups1),
    {#?STATE{pids_groups = PidsGroups2, groups = Groups2},
     Effects2} = ?MOD:handle_connection_down(Pid1, normal, State1),
    assertEmpty(PidsGroups2),
    assertEmpty(Effects2),
    assertEmpty(Groups2),

    ok.

handle_connection_down_sac_active_does_not_change_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = new_process(),
    Group = grp([csr(Pid1, 0, active),
                 csr(Pid0, 1, waiting),
                 csr(Pid0, 2, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, normal, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId, grp([csr(Pid1, 0, active)]), Groups),
    ok.

handle_connection_down_sac_no_more_consumers_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Group = grp([csr(Pid0, 0, active),
                 csr(Pid0, 1, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, normal, State),
    assertEmpty(PidsGroups),
    assertEmpty(Groups),
    assertEmpty(Effects),
    ok.

handle_connection_down_sac_no_consumers_in_down_connection_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = new_process(),
    Group = grp([csr(Pid1, 0, active),
                 csr(Pid1, 1, waiting)]),
    State = state(#{GroupId => Group},
                  #{Pid0 => maps:from_list([{GroupId, true}]), %% should not be there
                    Pid1 => maps:from_list([{GroupId, true}])}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, normal, State),

    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId,
                   grp([csr(Pid1, 0, active), csr(Pid1, 1, waiting)]),
                   Groups),
    ok.

handle_connection_down_super_stream_active_stays_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = new_process(),
    Group = grp(1, [csr(Pid0, 0, waiting),
                    csr(Pid0, 1, active),
                    csr(Pid1, 2, waiting),
                    csr(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid1, normal, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid0, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId,
                   grp(1, [csr(Pid0, 0, waiting),
                           csr(Pid0, 1, active)]),
                   Groups),
    ok.

handle_connection_down_super_stream_active_changes_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = new_process(),
    Group = grp(1, [csr(Pid0, 0, waiting),
                    csr(Pid1, 1, active),
                    csr(Pid0, 2, waiting),
                    csr(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} =
    ?MOD:handle_connection_down(Pid0, normal, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertSendMessageSteppingDownEffect(Pid1, 1, Stream, ConsumerName, Effects),
    assertHasGroup(GroupId,
                   grp(1, [csr(Pid1, 1, deactivating),
                           csr(Pid1, 3, waiting)]),
                   Groups),
    ok.

handle_connection_down_super_stream_activate_in_remaining_connection_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = new_process(),
    Group = grp(1, [csr(Pid0, 0, waiting),
                    csr(Pid0, 1, active),
                    csr(Pid1, 2, waiting),
                    csr(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, normal, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertSendMessageActivateEffect(Pid1, 3, Stream, ConsumerName, true, Effects),
    assertHasGroup(GroupId, grp(1, [csr(Pid1, 2, waiting),
                                    csr(Pid1, 3, active)]),
                   Groups),
    ok.

handle_connection_down_super_stream_no_active_removed_or_present_test(_) ->
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = new_process(),
    %% this is a weird case that should not happen in the wild,
    %% we test the logic in the code nevertheless.
    %% No active consumer in the group
    Group = grp(1, [csr(Pid0, 0, waiting),
                    csr(Pid0, 1, waiting),
                    csr(Pid1, 2, waiting),
                    csr(Pid1, 3, waiting)]),
    State = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups, groups = Groups},
     Effects} = ?MOD:handle_connection_down(Pid0, normal, State),
    assertSize(1, PidsGroups),
    assertSize(1, maps:get(Pid1, PidsGroups)),
    assertEmpty(Effects),
    assertHasGroup(GroupId, grp(1, [csr(Pid1, 2, waiting),
                                    csr(Pid1, 3, waiting)]),
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
    Pid1 = new_process(),
    Pid2 = new_process(),
    Group = grp(1, [csr(Pid0, 0, waiting),
                    csr(Pid1, 1, active),
                    csr(Pid2, 2, waiting)]),
    State0 = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = State1,
     Effects1} =
    ?MOD:handle_connection_down(Pid0, normal, State0),
    assertSize(2, PidsGroups1),
    assertSize(1, maps:get(Pid1, PidsGroups1)),
    assertSize(1, maps:get(Pid2, PidsGroups1)),
    assertSendMessageSteppingDownEffect(Pid1, 1, Stream, ConsumerName, Effects1),
    assertHasGroup(GroupId,
                   grp(1, [csr(Pid1, 1, deactivating),
                           csr(Pid2, 2, waiting)]),
                   Groups1),

    {#?STATE{pids_groups = PidsGroups2, groups = Groups2},
     Effects2} = ?MOD:handle_connection_down(Pid1, normal, State1),
    assertSize(1, PidsGroups2),
    assertSize(1, maps:get(Pid2, PidsGroups2)),
    assertSendMessageActivateEffect(Pid2, 2, Stream, ConsumerName, true, Effects2),
    assertHasGroup(GroupId,
                   grp(1, [csr(Pid2, 2, active)]),
                   Groups2),

    ok.

import_state_v4_empty_test(_) ->
    OldMod = rabbit_stream_sac_coordinator_v4,
    OldState = OldMod:init_state(),
    Export = OldMod:state_to_map(OldState),
    ?assertEqual(#?STATE{groups = #{}, pids_groups = #{},
                         conf = #{disconnected_timeout => 60_000}},
                 ?MOD:import_state(4, Export)),
    ok.

import_state_v4_test(_) ->
    OldMod = rabbit_stream_sac_coordinator_v4,
    OldState0 = OldMod:init_state(),
    Pid0 = self(),
    Pid1  = new_process(),
    Pid2  = new_process(),
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
    %% a duplicate consumer sneaks in
    %% this should not happen in real life, but it tests the dedup
    %% logic in the import function
    Cmd6 = register_consumer_command(P, 1, App1, Pid0, 0),
    OldState7 = apply_ensure_monitors(OldMod, Cmd6, OldState6),
    Cmd7 = activate_consumer_command(P, App1),
    OldState8 = apply_ensure_monitors(OldMod, Cmd7, OldState7),

    Export = OldMod:state_to_map(OldState8),
    #?STATE{groups = Groups, pids_groups = PidsGroups} = ?MOD:import_state(4, Export),
    assertHasGroup({<<"/">>, S, App0},
                   grp(-1, [csr(Pid0, 0, active),
                            csr(Pid1, 1, waiting),
                            csr(Pid2, 2, waiting)]),
                   Groups),

    assertHasGroup({<<"/">>, P, App1},
                   grp(1, [csr(Pid0, 0, waiting),
                           csr(Pid1, 1, active),
                           csr(Pid2, 2, waiting)]),
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
    Pid1 = new_process(),
    Pid2 = new_process(),
    Group = grp(1, [csr(Pid0, 0, waiting),
                    csr(Pid1, 1, active),
                    csr(Pid2, 2, waiting)]),
    State0 = state(#{GroupId => Group}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = _State1,
     [Effect1]} =
    ?MOD:handle_connection_down(Pid1, noconnection, State0),
    assertSize(2, PidsGroups1),
    assertSize(1, maps:get(Pid0, PidsGroups1)),
    assertSize(1, maps:get(Pid2, PidsGroups1)),
    ?assertEqual({timer, {sac, node_disconnected, #{connection_pid => Pid1}},
                  60_000},
                 Effect1),
    assertHasGroup(GroupId,
                   grp(1, [csr(Pid0, 0, {connected, waiting}),
                           csr(Pid1, 1, {disconnected, active}),
                           csr(Pid2, 2, {connected, waiting})]),
                   Groups1),
    ok.

handle_node_reconnected_test(_) ->
    N0 = node(),
    {N1Pid, N1} = start_node(?FUNCTION_NAME),
    N0Pid0 = new_process(N0),
    N0Pid1 = new_process(N0),
    N1Pid0 = new_process(N1),

    S0 = <<"s0">>,
    S1 = <<"s1">>,
    S2 = <<"s2">>,

    GId0 = group_id(S0),
    GId1 = group_id(S1),
    GId2 = group_id(S2),


    Group0 = grp(0, [csr(N0Pid0, 0, {connected, active}),
                     csr(N1Pid0, 1, {disconnected, waiting}),
                     csr(N0Pid1, 2, {connected, waiting})]),

    Group1 = grp(1, [csr(N0Pid0, 0, {connected, waiting}),
                     csr(N1Pid0, 1, {disconnected, active}),
                     csr(N0Pid1, 2, {connected, waiting})]),

    Group2 = grp(1, [csr(N0Pid0, 0, {connected, waiting}),
                     csr(N1Pid0, 1, {disconnected, waiting}),
                     csr(N0Pid1, 2, {connected, active})]),

    Groups0 = #{GId0 => Group0,
                GId1 => Group1,
                GId2 => Group2},
    %% Pid2 is missing from PIDs to groups dependency mapping
    State0 = state(Groups0,
                   #{N0Pid0 => #{GId0 => true, GId1 => true, GId2 => true},
                     N0Pid1 => #{GId0 => true, GId1 => true, GId2 => true}}),
    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = _State1,
     Effects1} =
    ?MOD:handle_node_reconnected(N1, State0, []),

    ?assertEqual(Groups0, Groups1),
    ?assertEqual(#{N0Pid0 => #{GId0 => true, GId1 => true, GId2 => true},
                   N1Pid0 => #{GId0 => true, GId1 => true, GId2 => true},
                   N0Pid1 => #{GId0 => true, GId1 => true, GId2 => true}},
                 PidsGroups1),

    assertSize(2, Effects1),
    assertContainsCheckConnectionEffect(N1Pid0, Effects1),
    assertContainsMonitorProcessEffect(N1Pid0, Effects1),

    stop_node(N1Pid),
    ok.

connection_reconnected_simple_disconnected_becomes_connected_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {disconnected, active}),
                 csr(Pid1, 1, {connected, waiting}),
                 csr(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, active}),
                             csr(Pid1, 1, {connected, waiting}),
                             csr(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_simple_active_should_be_first_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    %% disconnected for a while, got first in consumer array
    %% because consumers arrived and left
    Group = grp([csr(Pid0, 0, {disconnected, waiting}),
                 csr(Pid1, 1, {connected, active}),
                 csr(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp([csr(Pid1, 1, {connected, active}),
                             csr(Pid0, 0, {connected, waiting}),
                             csr(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_super_disconnected_becomes_connected_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {disconnected, waiting}),
                    csr(Pid1, 1, {connected, waiting}),
                    csr(Pid2, 2, {connected, active})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {connected, waiting}),
                                csr(Pid1, 1, {connected, waiting}),
                                csr(Pid2, 2, {connected, deactivating})]),
                   Groups1),

    assertSendMessageSteppingDownEffect(Pid2, 2, stream(), name(), Eff),
    ok.

presume_conn_down_simple_disconnected_becomes_presumed_down_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {disconnected, active}),
                 csr(Pid1, 1, {connected, waiting}),
                 csr(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:presume_connection_down(Pid0, State0),

    assertHasGroup(GId, grp([csr(Pid0, 0, {presumed_down, active}),
                             csr(Pid1, 1, {connected, active}),
                             csr(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertSendMessageActivateEffect(Pid1, 1, stream(), name(), true, Eff),
    ok.

presume_conn_down_super_stream_disconnected_becomes_presumed_down_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 1, {disconnected, active}),
                    csr(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:presume_connection_down(Pid1, State0),

    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {connected, waiting}),
                                csr(Pid1, 1, {presumed_down, active}),
                                csr(Pid2, 2, {connected, active})]),
                   Groups1),

    assertSendMessageActivateEffect(Pid2, 2, stream(), name(), true, Eff),
    ok.

presume_conn_down_simple_connected_does_not_become_presumed_down_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {connected, active}),
                 csr(Pid1, 1, {connected, waiting}),
                 csr(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:presume_connection_down(Pid1, State0),

    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, active}),
                             csr(Pid1, 1, {connected, waiting}),
                             csr(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

presume_conn_down_super_stream_connected_does_not_become_presumed_down_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 1, {connected, active}),
                    csr(Pid2, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:presume_connection_down(Pid1, State0),

    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {connected, waiting}),
                                csr(Pid1, 1, {connected, active}),
                                csr(Pid2, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.


register_consumer_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {connected, waiting}),
                 csr(Pid1, 1, {disconnected, active}),
                 csr(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = register_consumer_command(stream(), -1, name(), Pid0, 3),
    {#?STATE{groups = Groups1}, {ok, false}, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, waiting}),
                             csr(Pid1, 1, {disconnected, active}),
                             csr(Pid0, 2, {connected, waiting}),
                             csr(Pid0, 3, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

register_consumer_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 1, {disconnected, active}),
                    csr(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = register_consumer_command(stream(), 1, name(), Pid0, 3),
    {#?STATE{groups = Groups1}, {ok, false}, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {connected, waiting}),
                                csr(Pid1, 1, {disconnected, active}),
                                csr(Pid0, 2, {connected, waiting}),
                                csr(Pid0, 3, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

unregister_active_consumer_should_not_select_disconnected_consumer(_) ->
    P = self(),
    GId = group_id(),
    Group = grp([csr(P, 0, {connected, active}),
                 csr(P, 1, {disconnected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = unregister_consumer_command(stream(), name(), P, 0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp([csr(P, 1, {disconnected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

unregister_consumer_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {connected, waiting}),
                 csr(Pid1, 1, {disconnected, active}),
                 csr(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = unregister_consumer_command(stream(), name(), Pid0, 2),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, waiting}),
                             csr(Pid1, 1, {disconnected, active})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

unregister_consumer_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 1, {disconnected, active}),
                    csr(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = unregister_consumer_command(stream(), name(), Pid0, 0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp(1, [csr(Pid1, 1, {disconnected, active}),
                                csr(Pid0, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

activate_consumer_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {connected, waiting}),
                 csr(Pid1, 1, {disconnected, active}),
                 csr(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, waiting}),
                             csr(Pid1, 1, {disconnected, active}),
                             csr(Pid0, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

active_consumer_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 1, {disconnected, active}),
                    csr(Pid0, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {connected, waiting}),
                                csr(Pid1, 1, {disconnected, active}),
                                csr(Pid0, 2, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

activate_consumer_simple_unblock_all_waiting_test(_) ->
    P = self(),
    GId = group_id(),
    Group = grp([csr(P, 0, {connected, waiting}),
                 csr(P, 1, {connected, waiting}),
                 csr(P, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp([csr(P, 0, {connected, active}),
                             csr(P, 1, {connected, waiting}),
                             csr(P, 2, {connected, waiting})]),
                   Groups1),
    assertContainsActivateMessage(P, 0, Eff),
    ok.

activate_consumer_simple_unblock_ignore_disconnected_test(_) ->
    P = self(),
    GId = group_id(),
    Group = grp([csr(P, 0, {disconnected, waiting}),
                 csr(P, 1, {connected, waiting}),
                 csr(P, 2, {connected, waiting}),
                 csr(P, 3, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp([csr(P, 0, {disconnected, waiting}),
                             csr(P, 1, {connected, active}),
                             csr(P, 2, {connected, waiting}),
                             csr(P, 3, {connected, waiting})]),
                   Groups1),
    assertContainsActivateMessage(P, 1, Eff),
    ok.

activate_consumer_super_stream_unblock_all_waiting_test(_) ->
    P = self(),
    GId = group_id(),
    Group = grp(1, [csr(P, 0, {connected, waiting}),
                    csr(P, 1, {connected, waiting}),
                    csr(P, 2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp(1, [csr(P, 0, {connected, waiting}),
                                csr(P, 1, {connected, active}),
                                csr(P, 2, {connected, waiting})]),
                   Groups1),
    assertContainsActivateMessage(P, 1, Eff),
    ok.

activate_consumer_super_stream_unblock_ignore_disconnected_test(_) ->
    P = self(),
    GId = group_id(),
    Group = grp(1, [csr(P, 0, {disconnected, waiting}),
                    csr(P, 1, {connected, waiting}),
                    csr(P, 2, {connected, waiting}),
                    csr(P, 3, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),
    assertHasGroup(GId, grp(1, [csr(P, 0, {disconnected, waiting}),
                                csr(P, 1, {connected, waiting}),
                                csr(P, 2, {connected, active}),
                                csr(P, 3, {connected, waiting})]),
                   Groups1),
    assertContainsActivateMessage(P, 2, Eff),
    ok.

handle_connection_down_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {connected, waiting}),
                 csr(Pid1, 0, {disconnected, active}),
                 csr(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} = ?MOD:handle_connection_down(Pid2, normal,
                                                                   State0),
    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, waiting}),
                             csr(Pid1, 0, {disconnected, active})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

handle_connection_down_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 0, {disconnected, active}),
                    csr(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} = ?MOD:handle_connection_down(Pid0, normal,
                                                                   State0),
    assertHasGroup(GId, grp(1, [csr(Pid1, 0, {disconnected, active}),
                                csr(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

handle_connection_node_disconnected_simple_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {connected, waiting}),
                 csr(Pid1, 0, {disconnected, active}),
                 csr(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} =
    ?MOD:handle_connection_down(Pid2, noconnection, State0),
    assertHasGroup(GId, grp([csr(Pid0, 0, {connected, waiting}),
                             csr(Pid1, 0, {disconnected, active}),
                             csr(Pid2, 0, {disconnected, waiting})]),
                   Groups1),
    assertNodeDisconnectedTimerEffect(Pid2, Eff),
    ok.

handle_connection_node_disconnected_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {connected, waiting}),
                    csr(Pid1, 0, {disconnected, active}),
                    csr(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    {#?STATE{groups = Groups1}, Eff} =
    ?MOD:handle_connection_down(Pid0, noconnection, State0),
    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {disconnected, waiting}),
                                csr(Pid1, 0, {disconnected, active}),
                                csr(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertNodeDisconnectedTimerEffect(Pid0, Eff),
    ok.

connection_reconnected_simple_disconn_active_blocks_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, 0, {disconnected, waiting}),
                 csr(Pid1, 0, {disconnected, active}),
                 csr(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = connection_reconnected_command(Pid0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp([csr(Pid1, 0, {disconnected, active}),
                             csr(Pid0, 0, {connected, waiting}),
                             csr(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_simple_forg_act_disconn_active_blocks_rebalancing_test(_) ->
    P0 = new_process(),
    P1 = new_process(),
    P2 = new_process(),
    GId = group_id(),
    Group = grp([csr(P0, 0, {presumed_down, active}),
                 csr(P1, 0, {disconnected, active}),
                 csr(P2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = connection_reconnected_command(P0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp([csr(P0, 0, {connected, waiting}),
                             csr(P1, 0, {disconnected, active}),
                             csr(P2, 0, {connected, waiting})]),
                   Groups1),
    assertSize(1, Eff),
    assertContainsSendMessageSteppingDownEffect(P0, Eff),
    ok.

connection_reconnected_simple_forg_act_should_trigger_rebalancing_test(_) ->
    P0 = new_process(),
    P1 = new_process(),
    P2 = new_process(),
    GId = group_id(),
    Group = grp([csr(P0, {presumed_down, active}),
                 csr(P1, {connected, active}),
                 csr(P2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    S0 = state(Groups0),
    Cmd0 = connection_reconnected_command(P0),
    {#?STATE{groups = Groups1} = S1, ok, Eff1} = ?MOD:apply(Cmd0, S0),

    assertHasGroup(GId, grp([csr(P0, {connected, waiting}),
                             csr(P1, {connected, waiting}),
                             csr(P2, {connected, waiting})]),
                   Groups1),
    assertSize(2, Eff1),
    assertContainsSendMessageSteppingDownEffect(P0, 0, stream(), name(), Eff1),
    assertContainsSendMessageSteppingDownEffect(P1, 0, stream(), name(), Eff1),

    %% activation from the first consumer stepping down
    Cmd1 = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups2} = S2, ok, Eff2} = ?MOD:apply(Cmd1, S1),
    assertHasGroup(GId, grp([csr(P0, {connected, active}),
                             csr(P1, {connected, waiting}),
                             csr(P2, {connected, waiting})]),
                   Groups2),
    assertSize(1, Eff2),
    assertContainsActivateMessage(P0, Eff2),

    %% activation from the second consumer stepping down
    %% this is expected, but should not change the state
    Cmd2 = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups3}, ok, Eff3} = ?MOD:apply(Cmd2, S2),
    assertHasGroup(GId, grp([csr(P0, {connected, active}),
                             csr(P1, {connected, waiting}),
                             csr(P2, {connected, waiting})]),
                   Groups3),
    assertEmpty(Eff3),

    ok.

connection_reconnected_super_stream_disconn_active_blocks_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, 0, {disconnected, active}),
                    csr(Pid1, 0, {disconnected, waiting}),
                    csr(Pid2, 0, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = connection_reconnected_command(Pid1),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp(1, [csr(Pid0, 0, {disconnected, active}),
                                csr(Pid1, 0, {connected, waiting}),
                                csr(Pid2, 0, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

connection_reconnected_super_stream_forg_act_disconn_active_blocks_rebalancing_test(_) ->
    P0 = new_process(),
    P1 = new_process(),
    P2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(P0, {presumed_down, active}),
                    csr(P1, {disconnected, active}),
                    csr(P2, {connected, waiting})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),
    Cmd = connection_reconnected_command(P0),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertHasGroup(GId, grp(1, [csr(P0, {connected, waiting}),
                                csr(P1, {disconnected, active}),
                                csr(P2, {connected, waiting})]),
                   Groups1),
    assertSize(1, Eff),
    assertContainsSendMessageSteppingDownEffect(P0, Eff),
    ok.

connection_reconnected_super_stream_forg_act_should_trigger_rebalancing_test(_) ->
    P0 = new_process(),
    P1 = new_process(),
    P2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(P0, {presumed_down, active}),
                    csr(P1, {connected, waiting}),
                    csr(P2, {connected, active})]),

    Groups0 = #{GId => Group},
    S0 = state(Groups0),
    Cmd0 = connection_reconnected_command(P0),
    {#?STATE{groups = Groups1} = S1, ok, Eff1} = ?MOD:apply(Cmd0, S0),

    assertHasGroup(GId, grp(1, [csr(P0, {connected, waiting}),
                                csr(P1, {connected, waiting}),
                                csr(P2, {connected, waiting})]),
                   Groups1),
    assertSize(2, Eff1),
    assertContainsSendMessageSteppingDownEffect(P0, 0, stream(), name(), Eff1),
    assertContainsSendMessageSteppingDownEffect(P2, 0, stream(), name(), Eff1),

    %% activation from the first consumer stepping down
    Cmd1 = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups2} = S2, ok, Eff2} = ?MOD:apply(Cmd1, S1),
    assertHasGroup(GId, grp(1, [csr(P0, {connected, waiting}),
                                csr(P1, {connected, active}),
                                csr(P2, {connected, waiting})]),
                   Groups2),
    assertSize(1, Eff2),
    assertContainsActivateMessage(P1, Eff2),

    %% activation from the second consumer stepping down
    %% this is expected, but should not change the state
    Cmd2 = activate_consumer_command(stream(), name()),
    {#?STATE{groups = Groups3}, ok, Eff3} = ?MOD:apply(Cmd2, S2),
    assertHasGroup(GId, grp(1, [csr(P0, {connected, waiting}),
                                csr(P1, {connected, active}),
                                csr(P2, {connected, waiting})]),
                   Groups3),
    assertEmpty(Eff3),

    ok.

presume_conn_down_simple_disconn_active_blocks_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp([csr(Pid0, {disconnected, waiting}),
                 csr(Pid1, {connected, waiting}),
                 csr(Pid2, {disconnected, active})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:presume_connection_down(Pid0, State0),

    assertHasGroup(GId, grp([csr(Pid2, {disconnected, active}),
                             csr(Pid0, {presumed_down, waiting}),
                             csr(Pid1, {connected, waiting})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

presume_conn_down_super_stream_disconn_active_block_rebalancing_test(_) ->
    Pid0 = new_process(),
    Pid1 = new_process(),
    Pid2 = new_process(),
    GId = group_id(),
    Group = grp(1, [csr(Pid0, {disconnected, waiting}),
                    csr(Pid1, {connected, waiting}),
                    csr(Pid2, {disconnected, active})]),

    Groups0 = #{GId => Group},
    State0 = state(Groups0),

    {#?STATE{groups = Groups1}, Eff} = ?MOD:presume_connection_down(Pid0, State0),

    assertHasGroup(GId, grp(1, [csr(Pid0, {presumed_down, waiting}),
                                csr(Pid1, {connected, waiting}),
                                csr(Pid2, {disconnected, active})]),
                   Groups1),
    assertEmpty(Eff),
    ok.

purge_nodes_test(_) ->
    N0 = node(),
    {N1Pid, N1} = start_node(?FUNCTION_NAME),

    N0P0 = new_process(N0),
    N0P1 = new_process(N0),
    N0P2 = new_process(N0),
    N1P0 = new_process(N1),
    N1P1 = new_process(N1),
    N1P2 = new_process(N1),

    S0 = <<"s0">>,
    S1 = <<"s1">>,
    S2 = <<"s2">>,

    GId0 = group_id(S0),
    GId1 = group_id(S1),
    GId2 = group_id(S2),

    Group0 = grp([csr(N1P0, {disconnected, active}),
                  csr(N0P1, {connected, waiting}),
                  csr(N0P2, {connected, waiting})]),

    Group1 = grp(1, [csr(N1P1, {disconnected, waiting}),
                     csr(N1P2, {disconnected, active}),
                     csr(N0P0, {connected, waiting})]),

    Group2 = grp([csr(N0P0, {connected, active}),
                  csr(N0P1, {connected, waiting}),
                  csr(N0P2, {connected, waiting})]),

    State0 = state(#{GId0 => Group0, GId1 => Group1, GId2 => Group2}),
    Cmd = purge_nodes_command([N1]),
    {#?STATE{groups = Groups1}, ok, Eff} = ?MOD:apply(Cmd, State0),

    assertSize(3, Groups1),
    assertHasGroup(GId0, grp([csr(N0P1, {connected, active}),
                              csr(N0P2, {connected, waiting})]),
                   Groups1),
    assertHasGroup(GId1, grp(1, [csr(N0P0, {connected, active})]),
                   Groups1),
    assertHasGroup(GId2, grp([csr(N0P0, {connected, active}),
                              csr(N0P1, {connected, waiting}),
                              csr(N0P2, {connected, waiting})]),
                   Groups1),

    assertSize(2, Eff),
    assertContainsSendMessageEffect(N0P1, S0, true, Eff),
    assertContainsSendMessageEffect(N0P0, S1, true, Eff),

    stop_node(N1Pid),
    ok.

node_disconnected_and_reconnected_test(_) ->
    N0 = node(),
    {N1Pid, N1} = start_node(?FUNCTION_NAME),

    N0P0 = new_process(N0),
    N0P1 = new_process(N0),
    N0P2 = new_process(N0),
    N1P0 = new_process(N1),
    N1P1 = new_process(N1),
    N1P2 = new_process(N1),

    N0Pids = [N0P0, N0P1, N0P2],
    N1Pids = [N1P0, N1P1, N1P2],

    S0 = <<"s0">>,
    S1 = <<"s1">>,
    S2 = <<"s2">>,

    GId0 = group_id(S0),
    GId1 = group_id(S1),
    GId2 = group_id(S2),

    GIds = [GId0, GId1, GId2],

    G0 = grp([csr(N0P0, {connected, active}),
              csr(N1P0, {connected, waiting}),
              csr(N0P1, {connected, waiting})]),

    G1 = grp(1, [csr(N1P1, {connected, waiting}),
                 csr(N0P2, {connected, active}),
                 csr(N1P2, {connected, waiting})]),

    G2 = grp([csr(N0P0, {connected, active}),
              csr(N1P1, {connected, waiting}),
              csr(N0P2, {connected, waiting})]),

    State0 = state(#{GId0 => G0, GId1 => G1, GId2 => G2}),

    {State1, Eff1} = ?MOD:handle_connection_down(N1P0, noconnection, State0),
    {State2, Eff2} = ?MOD:handle_connection_down(N1P1, noconnection, State1),
    {State3, Eff3} = ?MOD:handle_connection_down(N1P2, noconnection, State2),

    assertNodeDisconnectedTimerEffect(N1P0, Eff1),
    assertNodeDisconnectedTimerEffect(N1P1, Eff2),
    assertNodeDisconnectedTimerEffect(N1P2, Eff3),

    assertHasGroup(GId0,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P0, {disconnected, waiting}),
                        csr(N0P1, {connected, waiting})]),
                   State3#?STATE.groups),

    assertHasGroup(GId1,
                   grp(1, [csr(N1P1, {disconnected, waiting}),
                           csr(N0P2, {connected, active}),
                           csr(N1P2, {disconnected, waiting})]),
                   State3#?STATE.groups),

    assertHasGroup(GId2,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P1, {disconnected, waiting}),
                        csr(N0P2, {connected, waiting})]),
                   State3#?STATE.groups),

    PidsGroups3 = State3#?STATE.pids_groups,
    assertSize(3, PidsGroups3),
    [ ?assert(maps:is_key(Pid, PidsGroups3)) || Pid <- N0Pids],
    [ ?assertNot(maps:is_key(Pid, PidsGroups3)) || Pid <- N1Pids],

    {State4, Eff4} = ?MOD:handle_node_reconnected(N1, State3, []),
    %% groups should not change
    [?assertEqual(maps:get(GId, State3#?STATE.groups),
                  maps:get(GId, State4#?STATE.groups))
      || GId <- GIds],

    %% all connections should be checked and monitored
    [begin
         assertContainsCheckConnectionEffect(Pid, Eff4),
         assertContainsMonitorProcessEffect(Pid, Eff4)
    end || Pid <- N1Pids],

    Cmd4 = connection_reconnected_command(N1P0),
    {#?STATE{groups = Groups5} = State5, ok, Eff5} = ?MOD:apply(Cmd4, State4),

    assertHasGroup(GId0,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P0, {connected, waiting}),
                        csr(N0P1, {connected, waiting})]),
                   Groups5),

    assertHasGroup(GId1,
                   grp(1, [csr(N1P1, {disconnected, waiting}),
                           csr(N0P2, {connected, active}),
                           csr(N1P2, {disconnected, waiting})]),
                   Groups5),

    assertHasGroup(GId2,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P1, {disconnected, waiting}),
                        csr(N0P2, {connected, waiting})]),
                   Groups5),

    assertEmpty(Eff5),

    Cmd5 = connection_reconnected_command(N1P1),
    {#?STATE{groups = Groups6} = State6, ok, Eff6} = ?MOD:apply(Cmd5, State5),

    assertHasGroup(GId0,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P0, {connected, waiting}),
                        csr(N0P1, {connected, waiting})]),
                   Groups6),

    assertHasGroup(GId1,
                   grp(1, [csr(N1P1, {connected, waiting}),
                           csr(N0P2, {connected, active}),
                           csr(N1P2, {disconnected, waiting})]),
                   Groups6),

    assertHasGroup(GId2,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P1, {connected, waiting}),
                        csr(N0P2, {connected, waiting})]),
                   Groups6),

    assertEmpty(Eff6),

    %% last connection does not come back for some reason
    {#?STATE{groups = Groups7}, Eff7} = ?MOD:presume_connection_down(N1P2, State6),

    assertHasGroup(GId0,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P0, {connected, waiting}),
                        csr(N0P1, {connected, waiting})]),
                   Groups7),

    assertHasGroup(GId1,
                   grp(1, [csr(N1P1, {connected, waiting}),
                           csr(N0P2, {connected, active}),
                           csr(N1P2, {presumed_down, waiting})]),
                   Groups7),

    assertHasGroup(GId2,
                   grp([csr(N0P0, {connected, active}),
                        csr(N1P1, {connected, waiting}),
                        csr(N0P2, {connected, waiting})]),
                   Groups7),

    assertEmpty(Eff7),

    stop_node(N1Pid),
    ok.

node_disconnected_reconnected_connection_down_test(_) ->
    N0 = node(),
    {N1Pid, N1} = start_node(list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "1")),
    {N2Pid, N2} = start_node(list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "2")),

    P0 = new_process(N0),
    P1 = new_process(N1),
    P2 = new_process(N2),

    GId = group_id(),

    G0 = grp(1, [csr(P0, {connected, waiting}),
                 csr(P1, {connected, active}),
                 csr(P2, {connected, waiting})]),
    S0 = state(#{GId => G0}),

    {#?STATE{groups = G1} = S1, Eff1} =
        ?MOD:handle_connection_down(P1, noconnection, S0),

    assertHasGroup(GId,
                   grp(1, [csr(P0, {connected, waiting}),
                           csr(P1, {disconnected, active}),
                           csr(P2, {connected, waiting})]),
                   G1),

    assertNodeDisconnectedTimerEffect(P1, Eff1),

    {#?STATE{groups = G2} = S2, Eff2} =
        ?MOD:handle_node_reconnected(N1, S1, []),

    assertHasGroup(GId,
                   grp(1, [csr(P0, {connected, waiting}),
                           csr(P1, {disconnected, active}),
                           csr(P2, {connected, waiting})]),
                   G2),

    assertContainsCheckConnectionEffect(P1, Eff2),

    {#?STATE{groups = G3}, Eff3} = ?MOD:handle_connection_down(P1, normal, S2),

    assertHasGroup(GId,
                   grp(1, [csr(P0, {connected, waiting}),
                           csr(P2, {connected, active})]),
                   G3),

    assertContainsSendMessageEffect(P2, stream(), true, Eff3),

    stop_node(N1Pid),
    stop_node(N2Pid),
    ok.

list_nodes_test(_) ->
    N0 = node(),
    {N1Pid, N1} = start_node(list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "1")),
    {N2Pid, N2} = start_node(list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "2")),

    P0 = new_process(N0),
    P1 = new_process(N1),
    P2 = new_process(N2),

    Id0 = group_id(<<"sO">>),
    Id1 = group_id(<<"s1">>),
    Id2 = group_id(<<"s2">>),

    ?assertEqual(lists:sort([N0, N1, N2]),
                 list_nodes(#{Id0 => grp([csr(P0), csr(P0), csr(P0)]),
                              Id1 => grp([csr(P1), csr(P1), csr(P1)]),
                              Id2 => grp([csr(P2), csr(P2), csr(P2)])})),
    ?assertEqual(lists:sort([N0, N2]),
                 list_nodes(#{Id0 => grp([csr(P0), csr(P0), csr(P0)]),
                              Id2 => grp([csr(P2), csr(P2), csr(P2)])})),
    ?assertEqual(lists:sort([N2]),
                 list_nodes(#{Id2 => grp([csr(P2), csr(P2), csr(P2)])})),
    ?assertEqual(lists:sort([N1, N2]),
                 list_nodes(#{Id0 => grp([csr(P1), csr(P2), csr(P2)]),
                              Id1 => grp([csr(P1), csr(P1), csr(P2)]),
                              Id2 => grp([csr(P2), csr(P2), csr(P2)])})),
    ?assertEqual(lists:sort([N0, N1, N2]),
                 list_nodes(#{Id0 => grp([csr(P0), csr(P1), csr(P2)])})),
    assertEmpty(list_nodes(#{})),

    stop_node(N1Pid),
    stop_node(N2Pid),
    ok.

state_enter_test(_) ->
    N0 = node(),
    {N1Pid, N1} = start_node(list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "1")),
    {N2Pid, N2} = start_node(list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "2")),

    P0 = new_process(N0),
    P1 = new_process(N1),
    P2 = new_process(N2),

    Id0 = group_id(<<"sO">>),
    Id1 = group_id(<<"s1">>),
    Id2 = group_id(<<"s2">>),

    assertEmpty(?MOD:state_enter(follower, #{})),

    ?assertEqual(mon_node_eff([N0, N1, N2]),
                 state_enter_leader(#{Id0 => grp([csr(P0), csr(P0), csr(P0)]),
                                      Id1 => grp([csr(P1), csr(P1), csr(P1)]),
                                      Id2 => grp([csr(P2), csr(P2), csr(P2)])})),

    ?assertEqual(mon_node_eff([N0, N1]),
                 state_enter_leader(#{Id0 => grp([csr(P0), csr(P0), csr(P0)]),
                                      Id1 => grp([csr(P1), csr(P1), csr(P1)]),
                                      Id2 => grp([csr(P0), csr(P1), csr(P1)])})),

    ?assertEqual(lists:sort(mon_node_eff([N0, N1]) ++ [timer_eff(P1)]),
                 state_enter_leader(#{Id0 => grp([csr(P0), csr(P1, {disconnected, waiting})]),
                                      Id2 => grp([csr(P0)])})),

    ?assertEqual(lists:sort(mon_node_eff([N0, N1, N2]) ++ timer_eff([P1, P2])),
                 state_enter_leader(#{Id0 => grp([csr(P0), csr(P1, {disconnected, waiting})]),
                                      Id1 => grp([csr(P0), csr(P2, {disconnected, waiting})]),
                                      Id2 => grp([csr(P0), csr(P1, {disconnected, waiting})])})),

    stop_node(N1Pid),
    stop_node(N2Pid),
    ok.

mon_node_eff(Nodes) when is_list(Nodes) ->
    lists:sort([mon_node_eff(N) || N <- Nodes]);
mon_node_eff(N) ->
    {monitor, node, N}.

timer_eff(Pids) when is_list(Pids) ->
    lists:sort([timer_eff(Pid) || Pid <- Pids]);
timer_eff(Pid) ->
    {timer, {sac, node_disconnected,
             #{connection_pid => Pid}}, 10_000}.

state_enter_leader(MapState) ->
    lists:sort(?MOD:state_enter(leader, state(MapState))).

list_nodes(MapState) ->
    lists:sort(?MOD:list_nodes(state(MapState))).

start_node(Name) ->
    {ok, NodePid, Node} = peer:start(#{
        name => Name,
        connection => standard_io,
        shutdown => close
    }),
    {NodePid, Node}.

stop_node(NodePid) ->
    _ = peer:stop(NodePid).

new_process() ->
    new_process(node()).

new_process(Node) ->
    spawn(Node, fun() -> ok end).

group_id() ->
    group_id(stream()).

group_id(S) ->
    {<<"/">>, S, name()}.

stream() ->
    <<"sO">>.

name() ->
    <<"app">>.

sub_id() ->
    0.

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

assertHasGroup(GroupId,
               #group{partition_index = ExpectedPI, consumers = ExpectedCs},
               Groups) ->
    #{GroupId := #group{partition_index = CurrentPI, consumers = CurrentCs}} = Groups,
    ?assertEqual(ExpectedPI, CurrentPI),
    assertCsrsEqual(ExpectedCs, CurrentCs).

assertCsrsEqual([Expected], [Current]) ->
    assertCsrEqual(Expected, Current);
assertCsrsEqual(ExpectedCs, CurrentCs) ->
    assertSize(length(ExpectedCs), CurrentCs),
    lists:foreach(fun(N) ->
                          Expected = lists:nth(N, ExpectedCs),
                          Current = lists:nth(N, CurrentCs),
                          assertCsrEqual(Expected, Current)
                  end, lists:seq(1, length(ExpectedCs))).

assertCsrEqual(Expected, Current) ->
    ?assertEqual(Expected#consumer{ts = 0}, Current#consumer{ts = 0}).

csr(Pid) ->
    csr(Pid, {connected, waiting}).

csr(Pid, Status) ->
    csr(Pid, sub_id(), Status).

csr(Pid, SubId, {Connectivity, Status}) ->
    #consumer{pid = Pid,
              subscription_id = SubId,
              owner = <<"owning connection label">>,
              status = {Connectivity, Status},
              ts = erlang:system_time(millisecond)};
csr(Pid, SubId, Status) ->
    csr(Pid, SubId, {connected, Status}).

grp(Consumers) ->
    grp(-1, Consumers).

grp(PartitionIndex, Consumers) ->
    #group{partition_index = PartitionIndex, consumers = Consumers}.

state() ->
    state(#{}).

state(Groups) ->
    state(Groups, ?MOD:compute_pid_group_dependencies(Groups)).

state(Groups, PidsGroups) ->
    #?STATE{groups = Groups, pids_groups = PidsGroups}.

state_with_conf(Conf) ->
    #?STATE{conf = Conf}.

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

assertContainsCheckConnectionEffect(Pid, Effects) ->
    assertContainsSendMessageEffect(Pid, {sac, check_connection, #{}}, Effects).

assertContainsSendMessageEffect(Pid, Stream, Active, Effects) ->
    assertContainsSendMessageEffect(Pid, 0, Stream, name(), Active, Effects).

assertContainsActivateMessage(Pid, SubId, Effects) ->
    assertContainsSendMessageEffect(Pid, SubId, stream(), name(),
                                    true, Effects).

assertContainsActivateMessage(Pid, Effects) ->
    assertContainsSendMessageEffect(Pid, sub_id(), stream(), name(),
                                    true, Effects).

assertContainsSendMessageEffect(Pid, SubId, Stream, ConsumerName, Active,
                                Effects) ->
    assertContainsSendMessageEffect(Pid, {sac,
                                          #{subscription_id => SubId,
                                            stream => Stream,
                                            consumer_name => ConsumerName,
                                            active => Active}},
                                    Effects).

assertContainsSendMessageSteppingDownEffect(Pid, Effects) ->
    assertContainsSendMessageSteppingDownEffect(Pid, sub_id(), stream(),
                                                name(), Effects).

assertContainsSendMessageSteppingDownEffect(Pid, SubId, Stream, ConsumerName,
                                            Effects) ->
    assertContainsSendMessageEffect(Pid, {sac,
                                          #{subscription_id => SubId,
                                            stream => Stream,
                                            consumer_name => ConsumerName,
                                            active => false,
                                            stepping_down => true}}, Effects).

assertContainsSendMessageEffect(Pid, Msg, Effects) ->
    assertContainsEffect({mod_call,
                          rabbit_stream_sac_coordinator,
                          send_message,
                          [Pid, Msg]}, Effects).

assertContainsMonitorProcessEffect(Pid, Effects) ->
    assertContainsEffect({monitor, process, Pid}, Effects).

assertContainsEffect(Effect, Effects) ->
    Contains = lists:any(fun(Eff) -> Eff =:= Effect end, Effects),
    ?assert(Contains, "List does not contain the expected effect").

assertSendMessageActivateEffect(Pid, SubId, Stream, ConsumerName, Active, Effects) ->
    assertSendMessageEffect(Pid, {sac,
                                  #{subscription_id => SubId,
                                    stream => Stream,
                                    consumer_name => ConsumerName,
                                    active => Active}
                                 }, Effects).

assertSendMessageSteppingDownEffect(Pid, SubId, Stream, ConsumerName, Effects) ->
    assertSendMessageEffect(Pid, {sac,
                                  #{subscription_id => SubId,
                                    stream => Stream,
                                    consumer_name => ConsumerName,
                                    active => false,
                                    stepping_down => true}}, Effects).

assertSendMessageEffect(Pid, Msg, [Effect]) ->
    ?assertEqual({mod_call,
                  rabbit_stream_sac_coordinator,
                  send_message,
                  [Pid, Msg]},
                 Effect).

assertNodeDisconnectedTimerEffect(Pid, [Effect]) ->
    ?assertMatch({timer,
                  {sac, node_disconnected, #{connection_pid := Pid}},
                  _},
                 Effect).
