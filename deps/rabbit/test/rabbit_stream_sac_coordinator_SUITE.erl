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
%% Copyright (c) 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
     {ok, Active1}, Effects1} =
        rabbit_stream_sac_coordinator:apply(Command0, State0),
    ?assert(Active1),
    ?assertEqual([consumer(ConnectionPid, 0, true)], Consumers1),
    assertSendMessageEffect(ConnectionPid, 0, Stream, ConsumerName, true, Effects1),

    Command1 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers2}}} =
         State2,
     {ok, Active2}, Effects2} =
        rabbit_stream_sac_coordinator:apply(Command1, State1),
    ?assertNot(Active2),
    ?assertEqual([consumer(ConnectionPid, 0, true),
                  consumer(ConnectionPid, 1, false)],
                 Consumers2),
    assertEmpty(Effects2),

    Command2 =
        register_consumer_command(Stream, -1, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers3}}} =
         State3,
     {ok, Active3}, Effects3} =
        rabbit_stream_sac_coordinator:apply(Command2, State2),
    ?assertNot(Active3),
    ?assertEqual([consumer(ConnectionPid, 0, true),
                  consumer(ConnectionPid, 1, false),
                  consumer(ConnectionPid, 2, false)],
                 Consumers3),
    assertEmpty(Effects3),

    Command3 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 0),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers4}}} =
         State4,
     ok, Effects4} =
        rabbit_stream_sac_coordinator:apply(Command3, State3),
    ?assertEqual([consumer(ConnectionPid, 1, true),
                  consumer(ConnectionPid, 2, false)],
                 Consumers4),
    assertSendMessageEffect(ConnectionPid, 1, Stream, ConsumerName, true, Effects4),

    Command4 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers5}}} =
         State5,
     ok, Effects5} =
        rabbit_stream_sac_coordinator:apply(Command4, State4),
    ?assertEqual([consumer(ConnectionPid, 2, true)], Consumers5),
    assertSendMessageEffect(ConnectionPid, 2, Stream, ConsumerName, true, Effects5),

    Command5 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = Groups6}, ok, Effects6} =
        rabbit_stream_sac_coordinator:apply(Command5, State5),
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
     {ok, Active1}, Effects1} =
        rabbit_stream_sac_coordinator:apply(Command0, State0),
    ?assert(Active1),
    ?assertEqual([consumer(ConnectionPid, 0, true)], Consumers1),
    assertSendMessageEffect(ConnectionPid, 0, Stream, ConsumerName, true, Effects1),

    Command1 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers2}}} =
         State2,
     {ok, Active2}, Effects2} =
        rabbit_stream_sac_coordinator:apply(Command1, State1),
    %% never active on registration
    ?assertNot(Active2),
    %% all consumers inactive, until the former active one steps down and activates the new consumer
    ?assertEqual([consumer(ConnectionPid, 0, false),
                  consumer(ConnectionPid, 1, false)],
                 Consumers2),
    assertSendMessageSteppingDownEffect(ConnectionPid, 0, Stream, ConsumerName, Effects2),

    Command2 = activate_consumer_command(Stream, ConsumerName),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers3}}} =
         State3,
     ok, Effects3} =
        rabbit_stream_sac_coordinator:apply(Command2, State2),

    %% 1 (partition index) % 2 (consumer count) = 1 (active consumer index)
    ?assertEqual([consumer(ConnectionPid, 0, false),
                  consumer(ConnectionPid, 1, true)],
                 Consumers3),
    assertSendMessageEffect(ConnectionPid, 1, Stream, ConsumerName, true, Effects3),

    Command3 =
        register_consumer_command(Stream, 1, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers4}}} =
         State4,
     {ok, Active4}, Effects4} =
        rabbit_stream_sac_coordinator:apply(Command3, State3),
    %% never active on registration
    ?assertNot(Active4),
    %% 1 (partition index) % 3 (consumer count) = 1 (active consumer index)
    %% the active consumer stays the same
    ?assertEqual([consumer(ConnectionPid, 0, false),
                  consumer(ConnectionPid, 1, true),
                  consumer(ConnectionPid, 2, false)],
                 Consumers4),
    assertEmpty(Effects4),

    Command4 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 0),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers5}}} =
         State5,
     ok, Effects5} =
        rabbit_stream_sac_coordinator:apply(Command4, State4),
    %% 1 (partition index) % 2 (consumer count) = 1 (active consumer index)
    %% the active consumer will move from sub 1 to sub 2
    ?assertEqual([consumer(ConnectionPid, 1, false),
                  consumer(ConnectionPid, 2, false)],
                 Consumers5),

    assertSendMessageSteppingDownEffect(ConnectionPid, 1, Stream, ConsumerName, Effects5),

    Command5 = activate_consumer_command(Stream, ConsumerName),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers6}}} =
         State6,
     ok, Effects6} =
        rabbit_stream_sac_coordinator:apply(Command5, State5),

    ?assertEqual([consumer(ConnectionPid, 1, false),
                  consumer(ConnectionPid, 2, true)],
                 Consumers6),
    assertSendMessageEffect(ConnectionPid, 2, Stream, ConsumerName, true, Effects6),

    Command6 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 1),
    {#?STATE{groups = #{GroupId := #group{consumers = Consumers7}}} =
         State7,
     ok, Effects7} =
        rabbit_stream_sac_coordinator:apply(Command6, State6),
    ?assertEqual([consumer(ConnectionPid, 2, true)], Consumers7),
    assertEmpty(Effects7),

    Command7 =
        unregister_consumer_command(Stream, ConsumerName, ConnectionPid, 2),
    {#?STATE{groups = Groups8}, ok, Effects8} =
        rabbit_stream_sac_coordinator:apply(Command7, State7),
    assertEmpty(Groups8),
    assertEmpty(Effects8),

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
    ?assertEqual(#{self() => sac}, Monitors1),
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
    ?assertEqual(#{self() => sac}, Monitors2),
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
    ?assertEqual(#{self() => sac}, Monitors3),
    ?assertEqual([], Effects3),

    %% trying with an unknown connection PID
    %% the function should not change anything
    UnknownConnectionPid = spawn(fun() -> ok end),
    PassthroughCommand =
        unregister_consumer_command(<<"stream">>,
                                    <<"app">>,
                                    UnknownConnectionPid,
                                    0),

    {State3, Monitors3, Effects3} =
        rabbit_stream_sac_coordinator:ensure_monitors(PassthroughCommand,
                                                      State3,
                                                      Monitors3,
                                                      []),

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
    Stream = <<"stream">>,
    ConsumerName = <<"app">>,
    GroupId = {<<"/">>, Stream, ConsumerName},
    Pid0 = self(),
    Pid1 = spawn(fun() -> ok end),
    Group =
        cgroup([consumer(Pid0, 0, true), consumer(Pid1, 1, false),
                consumer(Pid0, 2, false)]),
    State0 =
        state(#{GroupId => Group},
              #{Pid0 => maps:from_list([{GroupId, true}]),
                Pid1 => maps:from_list([{GroupId, true}])}),

    {#?STATE{pids_groups = PidsGroups1, groups = Groups1} = State1,
     Effects1} =
        rabbit_stream_sac_coordinator:handle_connection_down(Pid0, State0),
    assertSize(1, PidsGroups1),
    assertSize(1, maps:get(Pid1, PidsGroups1)),
    assertSendMessageEffect(Pid1, 1, Stream, ConsumerName, true, Effects1),
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
    ?assertEqual(Expected, length(List)).

assertEmpty(Data) ->
    assertSize(0, Data).

consumer(Pid, SubId, Active) ->
    #consumer{pid = Pid,
              subscription_id = SubId,
              owner = <<"owning connection label">>,
              active = Active}.

cgroup(Consumers) ->
    cgroup(-1, Consumers).

cgroup(PartitionIndex, Consumers) ->
    #group{partition_index = PartitionIndex, consumers = Consumers}.

state() ->
    state(#{}).

state(Groups) ->
    state(Groups, #{}).

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
