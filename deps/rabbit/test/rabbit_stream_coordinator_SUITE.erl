-module(rabbit_stream_coordinator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/src/rabbit_stream_coordinator.hrl").

-define(STATE, rabbit_stream_coordinator).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     listeners,
     machine_version_upgrade_to_2,
     machine_version_upgrade_to_3,
     machine_version_upgrade_to_7,
     sac_v7_down_handler_should_not_use_monitors_map,
     sac_v7_ensure_monitors_should_not_use_monitors_map,
     sac_pre_v7_down_handler_should_use_monitors_map,
     sac_pre_v7_ensure_monitors_should_use_monitors_map,
     new_stream,
     new_stream_idempotent,
     leader_down,
     leader_down_scenario_1,
     replica_down,
     add_replica,
     restart_stream,
     delete_stream,
     delete_stream_idempotent,
     delete_replica_leader,
     delete_replica,
     delete_two_replicas,
     delete_replica_2,
     leader_start_failed,
     member_started_stale_epoch,
     replica_disconnected_nodeup,
     nodeup_resumes_only_affected_streams,
     restart_stream_preserves_deleted,
     stranded_no_writer_reelection,
     action_failed_short_parks_and_reconciles,
     action_failed_no_backoff_retries_immediately,
     reconcile_drops_superseded_parked_entry,
     state_enter_rearms_retry_timer,
     action_throttling,
     action_throttling_drops_deleted_stream,
     aux_upgrade_from_prior_version,
     overview
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config)
  when TestCase =:= sac_v7_down_handler_should_not_use_monitors_map;
       TestCase =:= sac_v7_ensure_monitors_should_not_use_monitors_map;
       TestCase =:= sac_pre_v7_down_handler_should_use_monitors_map;
       TestCase =:= sac_pre_v7_ensure_monitors_should_use_monitors_map ->
    ok = meck:new(rabbit_stream_sac_coordinator, [no_link]),
    Config;
init_per_testcase(TestCase, Config)
  when TestCase =:= action_throttling;
       TestCase =:= action_throttling_drops_deleted_stream;
       TestCase =:= aux_upgrade_from_prior_version ->
    ok = meck:new(ra_aux, [passthrough, no_link]),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, _Config)
  when TestCase =:= sac_v7_down_handler_should_not_use_monitors_map;
       TestCase =:= sac_v7_ensure_monitors_should_not_use_monitors_map;
       TestCase =:= sac_pre_v7_down_handler_should_use_monitors_map;
       TestCase =:= sac_pre_v7_ensure_monitors_should_use_monitors_map ->
    meck:unload(rabbit_stream_sac_coordinator),
    ok;
end_per_testcase(TestCase, _Config)
  when TestCase =:= action_throttling;
       TestCase =:= action_throttling_drops_deleted_stream;
       TestCase =:= aux_upgrade_from_prior_version ->
    meck:unload(ra_aux),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

update_stream(M, C, S) ->
    rabbit_stream_coordinator:update_stream(M, C, S).

evaluate_stream(M, S, A) ->
    rabbit_stream_coordinator:evaluate_stream(M, S, A).

apply_cmd(M, C, S) ->
    rabbit_stream_coordinator:apply(M, C, S).

%% Machine versions up to 7 are served by the frozen rabbit_stream_coordinator_v7
%% module, so pre-v8 behaviour is exercised against it directly.
apply_cmd_v7(M, C, S) ->
    rabbit_stream_coordinator_v7:apply(M, C, S).

update_stream_v7(M, C, S) ->
    rabbit_stream_coordinator_v7:update_stream(M, C, S).

evaluate_stream_v7(M, S, A) ->
    rabbit_stream_coordinator_v7:evaluate_stream(M, S, A).

register_listener(Args, S) ->
    apply_cmd_v7(meta(#{index => 42, machine_version => 2}),
                 {register_listener, Args}, S).

eval_listeners(Stream) ->
    rabbit_stream_coordinator:eval_listeners(2, Stream, []).

down(Pid, S) ->
    apply_cmd_v7(meta(#{index => 42, machine_version => 2}),
                 {down, Pid, reason}, S).


listeners(_) ->
    S = <<"stream">>,
    Q = #resource{kind = queue, name = S, virtual_host = <<"/">>},
    ListPid = spawn(fun() -> ok end),
    N1 = r@n1,
    State0 = #?STATE{streams = #{}, monitors = #{}},
    StreamId = "stream1",
    ?assertMatch({_, stream_not_found, []},
                 register_listener(#{pid => ListPid,
                                     node => N1,
                                     stream_id => S,
                                     type => leader}, State0)
                ),

    LeaderPid0 = spawn(fun() -> ok end),
    Leader0 = #member{role = {writer, 1},
                      state = {running, 1, LeaderPid0}},
    Conf = #{name => StreamId},
    State1 = State0#?STATE{streams = #{StreamId => #stream{id = StreamId,
                                                           conf = Conf,
                                                           nodes = [N1],
                                                           listeners = #{},
                                                           members = #{N1 => Leader0},
                                                           queue_ref = Q}}},

    {State2, ok, Effs2} = register_listener(#{pid => ListPid,
                                              node => N1,
                                              stream_id => StreamId,
                                              type => leader}, State1),
    Stream2 = maps:get(StreamId, State2#?STATE.streams),
    ?assertEqual(
       #{{ListPid, leader} => LeaderPid0},
       Stream2#stream.listeners
      ),
    ?assertEqual(
       [{monitor, process, ListPid},
        {send_msg, ListPid,
         {queue_event, Q,
          {stream_leader_change, LeaderPid0}},
         cast}],
       Effs2
      ),
    ?assertEqual(
       #{ListPid => {#{StreamId => ok}, listener}},
       State2#?STATE.monitors
      ),

    {State3, ok, Effs3} = register_listener(#{pid => ListPid,
                                              node => N1,
                                              stream_id => StreamId,
                                              type => local_member}, State2),
    Stream3 = maps:get(StreamId, State3#?STATE.streams),
    ?assertEqual(
       #{{ListPid, leader} => LeaderPid0,
         {ListPid, member} => {N1, LeaderPid0}},
       Stream3#stream.listeners
      ),
    ?assertEqual(
       [{monitor, process, ListPid},
        {send_msg, ListPid,
         {queue_event, Q,
          {stream_local_member_change, LeaderPid0}},
         cast}],
       Effs3
      ),
    ?assertEqual(
       #{ListPid => {#{StreamId => ok}, listener}},
       State3#?STATE.monitors
      ),

    %% nothing should change after this evaluation
    {Stream3, []} = eval_listeners(Stream3),

    %% simulating a leader restart
    LeaderPid1 = spawn(fun() -> ok end),
    Leader1 = Leader0#member{state = {running, 2, LeaderPid1}},

    Stream4 = Stream3#stream{members = #{N1 => Leader1}},

    {Stream5, Effs5} = eval_listeners(Stream4),
    ?assertEqual(
       #{{ListPid, leader} => LeaderPid1,
         {ListPid, member} => {N1, LeaderPid1}},
       Stream5#stream.listeners
      ),
    ?assertEqual(
       [{send_msg, ListPid,
         {queue_event, Q,
          {stream_local_member_change, LeaderPid1}},
         cast},
        {send_msg, ListPid,
         {queue_event, Q,
          {stream_leader_change, LeaderPid1}},
         cast}],
       Effs5
      ),

    State5 = State3#?STATE{streams = #{StreamId => Stream5}},

    {State6, ok, []} = down(ListPid, State5),

    Stream6 = maps:get(StreamId, State6#?STATE.streams),
    ?assertEqual(
       #{},
       Stream6#stream.listeners
      ),

    ok.

machine_version_upgrade_to_2(_) ->
    machine_version_to_2(0),
    machine_version_to_2(1),
    ok.

machine_version_to_2(From) ->
    S = <<"stream">>,
    LeaderPid = spawn(fun() -> ok end),
    ListPid = spawn(fun() -> ok end), %% simulate a dead listener (not cleaned up)
    DeadListPid = spawn(fun() -> ok end),
    State0 = #?STATE{streams = #{S =>
                                 #stream{listeners = #{ListPid => LeaderPid,
                                                       DeadListPid => LeaderPid}}},
                     monitors = #{ListPid => {S, listener}}},

    {State1, ok, Effects} = apply_cmd(#{index => 42}, {machine_version, From, 2}, State0),

    Stream1 = maps:get(S, State1#?STATE.streams),
    ?assertEqual(
       #{{ListPid, leader} => LeaderPid,
         {DeadListPid, leader} => LeaderPid}, %% should be cleaned up on DOWN event
       Stream1#stream.listeners
      ),
    ?assertEqual(
       #{ListPid => {#{S => ok}, listener},
         DeadListPid => {#{S => ok}, listener}},
       State1#?STATE.monitors
      ),
    ?assertEqual(
       [{monitor, process, DeadListPid}, %% will trigger an immediate DOWN event
        {monitor, process, ListPid}],
       Effects
      ),
    ok.

machine_version_upgrade_to_3(_) ->
    machine_version_to_3(0),
    machine_version_to_3(1),
    machine_version_to_3(2),
    ok.

machine_version_to_3(From) ->
    State0 = #?STATE{},
    #?STATE{single_active_consumer = Sac0} = State0,

    ?assert(Sac0 == undefined),

    {#?STATE{single_active_consumer = Sac1}, ok, Effects} = apply_cmd(#{index => 42}, {machine_version, From, 3}, State0),

    ?assertNot(Sac1 == undefined),
    ?assertEqual(Effects, []),
    ok.

machine_version_upgrade_to_7(_) ->
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> ok end),
    Pid3 = spawn(fun() -> ok end),
    S = <<"stream">>,
    Monitors0 = #{Pid1 => sac,
                  Pid2 => {S, member},
                  Pid3 => sac},
    State0 = #?STATE{monitors = Monitors0},

    {State1, ok, Effects} = apply_cmd(#{index => 42}, {machine_version, 6, 7}, State0),

    ?assertEqual(#{Pid2 => {S, member}}, State1#?STATE.monitors),
    ?assertEqual([], Effects),
    ok.

sac_v7_down_handler_should_not_use_monitors_map(_) ->
    ConnectionPid = spawn(fun() -> ok end),
    SacState0 = fake_sac_state,
    SacState1 = updated_sac_state,
    meck:expect(rabbit_stream_sac_coordinator, handle_connection_down,
                fun(_Meta, Pid, normal, State) when Pid =:= ConnectionPid,
                                                    State =:= SacState0 ->
                        {SacState1, []}
                end),

    OtherPid = spawn(fun() -> ok end),
    Monitors0 = #{OtherPid => {<<"other">>, member}},
    State0 = #?STATE{single_active_consumer = SacState0,
                     monitors = Monitors0},

    {State1, ok, _Effects} = apply_cmd(meta(#{index => 42, machine_version => 7}),
                                       {down, ConnectionPid, normal}, State0),

    ?assert(meck:called(rabbit_stream_sac_coordinator, handle_connection_down,
                        ['_', ConnectionPid, normal, SacState0])),
    ?assertEqual(SacState1, State1#?STATE.single_active_consumer),
    ?assertEqual(Monitors0, State1#?STATE.monitors),
    ok.

sac_v7_ensure_monitors_should_not_use_monitors_map(_) ->
    ConnectionPid = self(),
    SacCmd = fake_sac_cmd,
    SacState0 = fake_sac_state,
    SacState1 = updated_sac_state,
    meck:expect(rabbit_stream_sac_coordinator, apply,
                fun(_Meta, Cmd, State) when Cmd =:= SacCmd,
                                            State =:= SacState0 ->
                        {SacState1, {ok, true}, []}
                end),
    meck:expect(rabbit_stream_sac_coordinator, ensure_monitors,
                fun(_Meta, Cmd, State, Monitors, Effects) when Cmd =:= SacCmd,
                                                               State =:= SacState1 ->
                        {State, Monitors#{ConnectionPid => sac}, Effects}
                end),

    State0 = #?STATE{single_active_consumer = SacState0,
                     monitors = #{}},

    {State1, {ok, true}, _Effects} = apply_cmd(meta(#{index => 42, machine_version => 7}),
                                               {sac, SacCmd}, State0),

    ?assertEqual(#{}, State1#?STATE.monitors),
    ?assertEqual(SacState1, State1#?STATE.single_active_consumer),
    ok.

sac_pre_v7_down_handler_should_use_monitors_map(_) ->
    ConnectionPid = spawn(fun() -> ok end),
    SacState0 = fake_sac_state,
    SacState1 = updated_sac_state,
    meck:expect(rabbit_stream_sac_coordinator, handle_connection_down,
                fun(_Meta, Pid, normal, State) when Pid =:= ConnectionPid,
                                                    State =:= SacState0 ->
                        {SacState1, []}
                end),

    OtherPid = spawn(fun() -> ok end),
    Monitors0 = #{ConnectionPid => sac,
                  OtherPid => {<<"other">>, member}},
    State0 = #?STATE{single_active_consumer = SacState0,
                     monitors = Monitors0},

    {State1, ok, _Effects} = apply_cmd_v7(meta(#{index => 42, machine_version => 6}),
                                          {down, ConnectionPid, normal}, State0),

    ?assert(meck:called(rabbit_stream_sac_coordinator, handle_connection_down,
                        ['_', ConnectionPid, normal, SacState0])),
    ?assertEqual(SacState1, State1#?STATE.single_active_consumer),
    ?assertEqual(#{OtherPid => {<<"other">>, member}}, State1#?STATE.monitors),
    ok.

sac_pre_v7_ensure_monitors_should_use_monitors_map(_) ->
    ConnectionPid = self(),
    SacCmd = fake_sac_cmd,
    SacState0 = fake_sac_state,
    SacState1 = updated_sac_state,
    %% the frozen v7 module calls the SAC module through its pre-version-aware
    %% arity-2/4 entry points
    meck:expect(rabbit_stream_sac_coordinator, apply,
                fun(Cmd, State) when Cmd =:= SacCmd,
                                     State =:= SacState0 ->
                        {SacState1, {ok, true}, []}
                end),
    meck:expect(rabbit_stream_sac_coordinator, ensure_monitors,
                fun(Cmd, State, Monitors, Effects) when Cmd =:= SacCmd,
                                                        State =:= SacState1 ->
                        {State, Monitors#{ConnectionPid => sac}, Effects}
                end),

    State0 = #?STATE{single_active_consumer = SacState0,
                     monitors = #{}},

    {State1, {ok, true}, _Effects} = apply_cmd_v7(meta(#{index => 42, machine_version => 6}),
                                                  {sac, SacCmd}, State0),

    ?assertEqual(#{ConnectionPid => sac}, State1#?STATE.monitors),
    ?assertEqual(SacState1, State1#?STATE.single_active_consumer),
    ok.

new_stream(_) ->
    [N1, N2, N3] = Nodes = [r@n1, r@n2, r@n3],
    StreamId = atom_to_list(?FUNCTION_NAME),
    Name = list_to_binary(StreamId),
    TypeState = #{name => StreamId,
                  nodes => Nodes},
    Q = new_q(Name, TypeState),
    From = {self(), make_ref()},
    Meta = #{system_time => ?LINE,
             from => From},
    S0 = update_stream(Meta, {new_stream, StreamId,
                              #{leader_node => N1,
                                queue => Q}}, undefined),
    E = 1,
    %% ready means a new leader has been chosen
    %% and the epoch incremented
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {ready, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {ready, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {ready, E}}}},
                 S0),

    %% we expect the next action to be starting the writer
    Idx1 = ?LINE,
    Meta1 = meta(Idx1),
    {S1, Actions} = evaluate_stream(Meta1, S0, []),
    ?assertMatch([{aux, {start_writer, StreamId,
                         #{node := N1, epoch := E, index := _},
                         #{epoch := E,
                           leader_node := N1,
                           replica_nodes := [N2, N3]}}}],
                 Actions),
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = {starting, Idx1},
                                                   state = {ready, E}}}},

                 S1),

    E1LeaderPid = fake_pid(N1),
    Idx2 = ?LINE,
    Meta2 = meta(Idx2),
    S2 = update_stream(Meta2, {member_started, StreamId,
                              #{epoch => E,
                                index => Idx1,
                                pid => E1LeaderPid}}, S1),
    ?assertMatch(#stream{nodes = Nodes,
                         epoch = E,
                         members = #{N1 :=
                                     #member{role = {writer, E},
                                             current = undefined,
                                             state = {running, E, E1LeaderPid}}}},
                         S2),
    Idx3 = ?LINE,
    {S3, Actions2} = evaluate_stream(meta(Idx3), S2, []),
    ?assertMatch([{aux, {start_replica, StreamId, #{node := N2},
                         #{epoch := E,
                           leader_pid := E1LeaderPid,
                           leader_node := N1}}},
                  {aux, {start_replica, StreamId, #{node := N3},
                         #{epoch := E,
                           leader_pid := E1LeaderPid,
                           leader_node := N1}}},
                  {aux, {update_mnesia, _, _, _}},
                  %% we reply to the caller once the leader has started
                  {reply, From, {wrap_reply, {ok, E1LeaderPid}}}
                 ], lists:sort(Actions2)),

    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, E1LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = {starting, Idx3},
                                                   state = {ready, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = {starting, Idx3},
                                                   state = {ready, E}}}},
                 S3),
    R1Pid = fake_pid(N2),
    S4 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, index => Idx3, pid => R1Pid}}, S3),
    {S5, []} = evaluate_stream(meta(?LINE), S4, []),
    R2Pid = fake_pid(N3),
    S6 = update_stream(Meta, {member_started, StreamId,
                              #{epoch => E, index => Idx3, pid => R2Pid}}, S5),
    {S7, []} = evaluate_stream(meta(?LINE), S6, []),
    %% actions should have start_replica requests
    ?assertMatch(#stream{nodes = Nodes,
                         members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, E1LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, R1Pid}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, R2Pid}}}},
                 S7),

    ok.

new_stream_idempotent(_) ->
    S0 = rabbit_stream_coordinator:init(#{machine_version => 7}),
    StreamId = atom_to_list(?FUNCTION_NAME),

    TypeState = #{name => StreamId,
                  retention => [],
                  nodes => [node()]},
    Q = new_q(list_to_binary(StreamId), TypeState),
    NewStream = {new_stream, StreamId, #{leader_node => node(),
                                         retention => [],
                                         queue => Q}},
    From = {self(), make_ref()},
    StartIdx = ?LINE,
    Meta = (meta(StartIdx))#{from => From},
    {S1, '$ra_no_reply', _} = apply_cmd(Meta, NewStream, S0),
    {S1, '$ra_no_reply', []} = apply_cmd(Meta#{index := ?LINE}, NewStream, S1),
    Pid = self(),
    {S2, _, _} = apply_cmd(meta(?LINE), {member_started, StreamId,
                                         #{epoch => 1,
                                           index => StartIdx,
                                           pid => Pid}}, S1),
    {_, {ok, Pid}, []} = apply_cmd(Meta#{index := ?LINE}, NewStream, S2),

    ok.

leader_down(_) ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, LeaderPid, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   target = stopped,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    Idx2 = ?LINE,
    {S2, Actions} = evaluate_stream(meta(Idx2), S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch(
       [{aux, {stop, StreamId,
               #{node := N1, epoch := E, index := Idx2},
               #{epoch := E}}},
        {aux, {stop, StreamId,
               #{node := N2, epoch := E, index := Idx2},
               #{epoch := E}}},
        {aux, {stop, StreamId,
               #{node := N3, epoch := E, index := Idx2},
               #{epoch := E}}}], lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = {stopping, Idx2},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = {stopping, Idx2},
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = {stopping, Idx2},
                                                   state = {running, E, Replica2}}}},
                 S2),

    %% idempotency check
    {S2, []} = evaluate_stream(meta(?LINE), S2, []),
    N2Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId,
                                     #{node => N2,
                                       index => Idx2,
                                       epoch => E,
                                       tail => N2Tail}}, S2),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {stopped, E, N2Tail}}}},
                 S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    N3Tail = {E, 102},
    #{index := Idx4} = Meta4 = meta(?LINE + 1),
    S4 = update_stream(Meta4, {member_stopped, StreamId,
                               #{node => N3,
                                 index => Idx2,
                                 epoch => E,
                                 tail => N3Tail}}, S3),
    E2 = E + 1,
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, Idx2},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S4),
    {S5, Actions4} = evaluate_stream(Meta4, S4, []),
    %% new leader has been selected so should be started
    ?assertMatch([{aux, {start_writer, StreamId, #{node := N3},
                         #{leader_node := N3}}}],
                 lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),

    E2LeaderPid = fake_pid(n3),
    #{index := Idx6} = Meta6 = meta(?LINE),
    S6 = update_stream(Meta6, {member_started, StreamId,
                               #{epoch => E2,
                                 index => Idx4,
                                 pid => E2LeaderPid}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, Idx2},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S6),
    {S7, Actions6} = evaluate_stream(Meta6, S6, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId,
                         #{node := N2},
                         #{leader_pid := E2LeaderPid}}},
                  {aux, {update_mnesia, _, _, _}}
                 ],
                 lists:sort(Actions6)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = {starting, Idx6},
                                                   state = {ready, E2}},
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S7),
    E2RepllicaN2Pid = fake_pid(n2),
    S8 = update_stream(meta(?LINE), {member_started, StreamId,
                                     #{epoch => E2,
                                       index => Idx6,
                                       pid => E2RepllicaN2Pid}}, S7),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2RepllicaN2Pid}}}},
                 S8),
    %% nothing to do
    {S8, []} = evaluate_stream(meta(?LINE), S8, []),

    #{index := Idx9} = Meta9 = meta(?LINE),
    S9 = update_stream(Meta9, {action_failed, StreamId,
                               #{action => stopping,
                                 index => Idx2,
                                 node => N1,
                                 epoch => E}}, S8),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {down, E}}}},
                 S9),

    {S10, Actions9} = evaluate_stream(Meta9, S9, []),
    %% retries action
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E2}, _}}],
                 lists:sort(Actions9)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, Idx9},
                                                   state = {down, E}}}},
                 S10),

    %% now finally succeed in stopping the old writer
    N1Tail = {1, 107},
    S11 = update_stream(meta(?LINE),
                        {member_stopped, StreamId, #{node => N1,
                                                     index => Idx9,
                                                     epoch => E2,
                                                     tail => N1Tail}}, S10),
    %% skip straight to ready as cluster is already operative
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S11),

    {S12, Actions11} = evaluate_stream(meta(?LINE), S11, []),
    ?assertMatch([{aux, {start_replica, StreamId, #{node := N1},
                         #{leader_pid := E2LeaderPid}}}],
                 lists:sort(Actions11)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {starting, _},
                                                   state = {ready, E2}}}},
                 S12),
    ok.

replica_down(_) ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, Replica1, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {running, E, LeaderPid}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(meta(?LINE), S1, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId, #{node := N2},
                         #{leader_pid := LeaderPid}}}
                 ],
                 lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = {starting, _},
                                                   state = {down, E}}
                                     }},
                 S2),
    ok.

leader_start_failed(_) ->

    %% after a leader is selected we need to handle the case where the leader
    %% start fails
    %% this can happen if a node hosting the leader disconnects then connects
    %% then disconnects again (rabbit seems to do this sometimes).
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    Idx2 = ?LINE,
    S1 = update_stream(meta(Idx2), {down, LeaderPid, boom}, S0),
    {S2, _Actions} = evaluate_stream(meta(Idx2), S1, []),
    %% leader was down but a temporary reconnection allowed the stop to complete
    S3 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N1,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {1, 2}}}, S2),

    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    Meta4 = meta(?LINE),
    S4 = update_stream(Meta4,
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {1, 1}}}, S3),
    E2 = E+1,
    {S5, Actions4} = evaluate_stream(Meta4, S4, []),
    ?assertMatch([{aux, {start_writer, StreamId, _,
                         #{epoch := E2,
                           leader_node := N1}}}],
                 lists:sort(Actions4)),
    #{index := Idx4} = Meta4,
    S6 = update_stream(meta(?LINE),
                       {action_failed, StreamId, #{node => N1,
                                                   index => Idx4,
                                                   action => starting,
                                                   epoch => E2}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   target = stopped,
                                                   state = {ready, E2}},
                                     N2 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, E, _}}}},
                 S6),
    % E3 = E2+1,
    Idx7 = ?LINE,
    {S7, Actions6} = evaluate_stream(meta(Idx7), S6, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E2}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E2}, _}}
                 ], lists:sort(Actions6)),
    %% late stop from prior epoch - need to run stop again to make sure
    Meta8 = meta(?LINE),
    S8 = update_stream(Meta8,
                       {member_stopped, StreamId, #{node => N3,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {1, 1}}}, S7),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E2},
                                                   current = {stopping, _},
                                                   target = stopped,
                                                   state = {ready, E2}},
                                     N2 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = {stopping, _},
                                                   state = {ready, E2}},
                                     N3 := #member{role = {replica, E2},
                                                   target = stopped,
                                                   current = undefined,
                                                   state = {stopped, E, _}}}},
                 S8),
    {_S9, Actions8} = evaluate_stream(Meta8, S8, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N3, epoch := E2}, _}}
                 ], lists:sort(Actions8)),

    ok.

leader_down_scenario_1(_) ->
    %% leader ended up in a stopped state in epoch 2 but on ereplica was
    %% in ready, 2 and the other down 1

    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    Idx1 = ?LINE,
    S1 = update_stream(meta(Idx1), {down, LeaderPid, boom}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = undefined,
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {running, E, Replica2}}}},
                 S1),
    {S2, Actions} = evaluate_stream(meta(Idx1), S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E2}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E2}, _}},
                  {aux, {stop, StreamId, #{node := N3, epoch := E2}, _}}],
                 lists:sort(Actions)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, E},
                                                   current = {stopping, Idx1},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E},
                                                   current = {stopping, Idx1},
                                                   state = {running, E, Replica1}},
                                     N3 := #member{role = {replica, E},
                                                   current = {stopping, Idx1},
                                                   state = {running, E, Replica2}}}},
                 S2),

    %% idempotency check
    {S2, []} = evaluate_stream(meta(?LINE), S2, []),
    N2Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => N2Tail}}, S2),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E},
                                                   current = undefined,
                                                   state = {stopped, E, N2Tail}}}},
                 S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    N3Tail = {E, 102},
    Meta4 = meta(?LINE),
    S4 = update_stream(Meta4, {member_stopped, StreamId, #{node => N3,
                                                           index => Idx1,
                                                           epoch => E,
                                                           tail => N3Tail}}, S3),
    E2 = E + 1,
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S4),
    {S5, Actions4} = evaluate_stream(Meta4, S4, []),
    %% new leader has been selected so should be started
    ?assertMatch([{aux, {start_writer, StreamId, _Args, #{leader_node := N3}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),

    E2LeaderPid = fake_pid(n3),
    Meta6 = meta(?LINE),
    S6 = update_stream(Meta6, {member_started, StreamId,
                               Meta4#{epoch => E2, pid => E2LeaderPid}}, S5),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     %% N3 has the higher offset so should
                                     %% be selected as writer of E2
                                     N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {running, E2, E2LeaderPid}}}},
                 S6),
    {S6b, Actions6} = evaluate_stream(Meta6, S6, []),
    ?assertMatch([
                  {aux, {start_replica, StreamId, #{node := N2}, _}},
                  {aux, {update_mnesia, _, _, _}}
                 ],
                 lists:sort(Actions6)),

    #{index := Idx7} = Meta7 = meta(?LINE),
    S7 = update_stream(Meta7, {down, E2LeaderPid, boom}, S6b),
    {S8, Actions7} = evaluate_stream(Meta7, S7, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N3, epoch := E2}, _}}],
                 lists:sort(Actions7)),
    ?assertMatch(#stream{members = #{N1 := #member{role = {replica, E2},
                                                   current = {stopping, _},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E2},
                                                   current = {starting, _},
                                                   state = {ready, E2}},
                                     N3 := #member{role = {writer, E2},
                                                   current = {stopping, Idx7},
                                                   state = {down, E2}}}},
                 S8),
    %% writer is stopped before the ready replica has been started
    S9 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 index => Idx7,
                                                                 epoch => E2,
                                                                 tail => N3Tail}},
                       S8),
    ?assertMatch(#stream{members = #{N3 := #member{role = {writer, E2},
                                                   current = undefined,
                                                   state = {stopped, E2, N3Tail}}}},
                 S9),
    {S10, []} = evaluate_stream(meta(?LINE), S9, []),
    #{index := Idx12} = Meta12 = meta(?LINE),
    S11 = update_stream(Meta12, {action_failed, StreamId,
                                      Meta6#{action => starting,
                                             node => N2,
                                             epoch => E2}},
                        S10),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}}},
                 S11),
    {S12, Actions11} = evaluate_stream(Meta12, S11, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N2, epoch := E2}, _}}],
                 lists:sort(Actions11)),
    ?assertMatch(#stream{members = #{N2 := #member{role = {replica, E2},
                                                   current = {stopping, Idx12},
                                                   state = {ready, E2}}}},
                 S12),
    S13 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                  index => Idx12,
                                                                  epoch => E2,
                                                                  tail => N2Tail}},
                        S12),
    E3 = E2 + 1,
    ?assertMatch(#stream{members = #{
                                     N1 := #member{role = {replica, E3},
                                                   current = {stopping, Idx1},
                                                   state = {down, E}},
                                     N2 := #member{role = {replica, E3},
                                                   current = undefined,
                                                   state = {ready, E3}},
                                     N3 := #member{role = {writer, E3},
                                                   current = undefined,
                                                   state = {ready, E3}}
                                    }},
                 S13),
    ok.


restart_stream(_) ->
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {restart_stream, StreamId,
                               #{preferred_leader_node => N2}}, S0),
    ?assertMatch(#stream{target = running,
                         members = #{N3 := #member{target = stopped,
                                                   preferred = false,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   preferred = true,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N1 := #member{target = stopped,
                                                   preferred = false,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(Meta1, S1, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N1}, _}},
                  {aux, {stop, StreamId, #{node := N2}, _}},
                  {aux, {stop, StreamId, #{node := N3}, _}}
                 ],
                 lists:sort(Actions1)),

    ?assertMatch(#stream{target = running,
                         members = #{N3 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = _},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = _},
                                     N1 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = _}
                                    }},
                 S2),
    ok.

delete_stream(_) ->
    %% leader ended up in a stopped state in epoch 2 but one replica was
    %% in ready, 2 and the other down 1

    % E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_stream, StreamId, #{}}, S0),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _},
                                     N2 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _},
                                     N1 := #member{target = deleted,
                                                   current = undefined,
                                                   state = _}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(meta(?LINE), S1, []),
    %% expect all members to be stopping now
    %% replicas will receive downs however as will typically exit if leader does
    %% this is ok
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N1}, _}},
                  {aux, {delete_member, StreamId, #{node := N2}, _}},
                  {aux, {delete_member, StreamId, #{node := N3}, _}}
                  % {reply, From, {wrap_reply, {ok, 0}}}
                 ],
                 lists:sort(Actions1)),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = _},
                                     N2 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = _},
                                     N1 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = _}
                                    }},
                 S2),
    S3 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N1}},
                       S2),
    ?assertMatch(#stream{target = deleted,
                         members = #{N2 := _, N3 := _} = M3}
                   when not is_map_key(N1, M3), S3),
    {S4, []} = evaluate_stream(meta(?LINE), S3, []),
    ?assertMatch(#stream{target = deleted,
                         members = #{N2 := _, N3 := _} = M3}
                   when not is_map_key(N1, M3), S4),
    S5 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N2}},
                       S4),
    ?assertMatch(#stream{target = deleted,
                         members = #{N3 := _} = M5}
                   when not is_map_key(N2, M5), S5),
    {S6, []} = evaluate_stream(meta(?LINE), S5, []),
    S7 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N3}},
                       S6),
    ?assertEqual(undefined, S7),
    ok.

delete_stream_idempotent(_) ->
    S0 = rabbit_stream_coordinator:init(#{machine_version => 5}),
    StreamId = atom_to_list(?FUNCTION_NAME),

    TypeState = #{name => StreamId,
                  retention => [],
                  nodes => [node()]},
    Q = new_q(list_to_binary(StreamId), TypeState),
    Cmd0 = {new_stream, StreamId, #{leader_node => node(),
                                    retention => [],
                                    queue => Q}},
    {S1, _, _} = apply_cmd(meta(?LINE), Cmd0, S0),

    Cmd1 = {delete_stream, StreamId, #{}},
    {S2, ok, []} = apply_cmd(meta(?LINE), Cmd1, S1),

    Cmd2 = {member_deleted, StreamId, #{node => node()}},
    {S3, '$ra_no_reply', []} = apply_cmd(meta(?LINE), Cmd2, S2),

    {S3, ok, []} = apply_cmd(meta(?LINE), Cmd1, S3),

    ok.

add_replica(_) ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {add_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {down, 0}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(Meta1, S1, []),
    ?assertMatch([{aux, {stop, StreamId, #{node := N1, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N3, epoch := E}, _}}],
                 lists:sort(Actions1)),
    Idx1 = maps:get(index, Meta1),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {down, 0}}
                                    }},
                 S2),
    N1Tail = {E, 101},
    S3 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N1,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => N1Tail}},
                        S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {stopped, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {running, _, _}},
                                     N3 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {down, 0}}
                                    }}, S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    N2Tail = {E, 100},
    S4 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N2,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => N2Tail}},
                        S3),
    E2 = E + 1,
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{target = stopped,
                                                   current = {stopping, Idx1},
                                                   state = {down, 0}}
                                    }}, S4),
    Idx3 = ?LINE,
    {S3, []} = evaluate_stream(meta(Idx3), S3, []),
    {S5, Actions4} = evaluate_stream(meta(Idx3), S4, []),
    ?assertMatch([{aux, {start_writer, StreamId, #{index := Idx3},
                         #{leader_node := N1}}}],
                  lists:sort(Actions4)),
    ?assertMatch(#stream{epoch = E2}, S5),
    S6 = update_stream(meta(?LINE), {member_stopped, StreamId, #{node => N3,
                                                                 index => Idx1,
                                                                 epoch => E,
                                                                 tail => empty}},
                        S5),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2, N3],
                         members = #{N1 := #member{target = running,
                                                   current = {starting, Idx3},
                                                   role = {writer, _},
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N3 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}}
                                    }}, S6),
    ok.

delete_replica(_) ->
    %% TOOD: replica and leader needs to be tested
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1, Replica2]),
    From = {self(), make_ref()},
    Idx1 = ?LINE,
    Meta1 = (meta(Idx1))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(Meta1, S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N3}, _}},
                  {aux, {stop, StreamId, #{node := N1, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}}],
                 lists:sort(Actions1)),
    S3 = update_stream(meta(?LINE), {member_deleted, StreamId, #{node => N3}},
                       S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, _, _}}
                                    } = Members}
                   when not is_map_key(N3, Members), S3),
    {S3, []} = evaluate_stream(meta(?LINE), S3, []),
    S4 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N1,
                                                    index => Idx1,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S3),
    {S4, []} = evaluate_stream(meta(?LINE), S4, []),
    S5 = update_stream(meta(?LINE),
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx1,
                                                    epoch => E,
                                                    tail => {E, 101}}},
                       S4),
    {S6, Actions5} = evaluate_stream(meta(?LINE), S5, []),
    E2 = E + 1,
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {ready, E2}},
                                     N2 := #member{target = running,
                                                   role = {writer, E2},
                                                   current = {starting, _},
                                                   state = {ready, E2}}
                                    }}, S6),
    ?assertMatch([{aux, {start_writer, StreamId, _Args, #{nodes := [N1, N2]}}}
                  ], lists:sort(Actions5)),
    {S4, []} = evaluate_stream(meta(?LINE), S4, []),
    ok.

delete_two_replicas(_) ->
    %% There was a race condition on the rabbit_stream_queue_SUITE testcases delete_replica
    %% and delete_last_replica. A replica can sometimes restart after deletion as it transitions
    %% again to running state. This test reproduces it. See `rabbit_stream_coordinator.erl`
    %% line 1039, the processing of `member_stopped` command. The new function `update_target`
    %% ensures this transition never happens.
    %% This test reproduces the trace that leads to that error.
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1, Replica2]),
    From = {self(), make_ref()},
    Idx1 = ?LINE,
    Meta1 = (meta(Idx1))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N3 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    {S2, Actions1} = evaluate_stream(Meta1, S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N3}, _}},
                  {aux, {stop, StreamId, #{node := N1, epoch := E}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}}],
                 lists:sort(Actions1)),

    Idx2 = ?LINE,
    Meta2 = (meta(Idx2))#{from => From},
    S3 = update_stream(Meta2, {delete_replica, StreamId, #{node => N2}}, S2),
    ?assertMatch(#stream{target = running,
                         nodes = [N1],
                         members = #{N1 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = {running, _, _}},
                                     N2 := #member{target = deleted,
                                                   current = {stopping, _},
                                                   state = {running, _, _}},
                                     N3 := #member{target = deleted,
                                                   current = {deleting, _},
                                                   state = {running, _, _}}
                                    }},
                 S3),
    {S4, []} = evaluate_stream(Meta2, S3, []),


    Idx3 = ?LINE,
    S5 = update_stream(meta(Idx3),
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx1,
                                                    epoch => E,
                                                    tail => {E, 101}}},
                       S4),
    %% A deleted member can never transition to another target.
    ?assertMatch(#stream{members = #{N2 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {stopped, _, _}}}},
                 S5),
    ok.

delete_replica_2(_) ->
    %% replica is deleted before it has been fully started
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    N3 = node(Replica2),
    %% set replicas back to starting state
    #stream{id = StreamId,
            members = Members00} = S00 = started_stream(StreamId, LeaderPid,
                                                        [Replica1, Replica2]),
    Members = maps:map(fun (_, #member{role = {replica, _}} = M) ->
                               M#member{state = {ready, 1},
                                        current = {starting, 1}};
                           (_, M) ->
                               M
                       end, Members00),
    S0 = S00#stream{members = Members},
    From = {self(), make_ref()},
    Idx1 = ?LINE,
    Meta1 = (meta(Idx1))#{from => From},
    %% DELETE REPLICA
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N3}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N1, N2],
                         members = #{N1 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = {starting, _},
                                                   state = {ready, _}},
                                     N3 := #member{target = deleted,
                                                   current = {starting, _},
                                                   state = {ready, _}}
                                    }},
                 S1),
    Idx2 = ?LINE,
    {S2, Actions1} = evaluate_stream(meta(Idx2), S1, []),
    ?assertMatch([
                  % {aux, {delete_member, StreamId, #{node := N3}, _}},
                  {aux, {stop, StreamId, #{node := N1, epoch := E}, _}}],
                 lists:sort(Actions1)),
    %% LEADER DOWN
    Meta3 = #{index := _Idx3} = meta(?LINE),
    S3 = update_stream(Meta3, {down, LeaderPid, normal}, S2),
    ?assertMatch(#stream{target = running,
                         members = #{N1 := #member{target = stopped,
                                                   current = {stopping, _},
                                                   state = {down, _}},
                                     N2 := #member{target = stopped,
                                                   current = {starting, _},
                                                   state = {ready, _}},
                                     N3 := #member{target = deleted,
                                                   current = {starting, _},
                                                   state = {ready, _}}
                                    }},
                 S3),
    {S4, Actions4} = evaluate_stream(meta(?LINE), S3, []),
    ?assertMatch([], Actions4),
    %% LEADER STOPPED
    Idx4 = ?LINE,
    S5 = update_stream(meta(Idx4),
                       {member_stopped, StreamId, #{node => N1,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S4),
    ?assertMatch(#stream{members = #{N1 := #member{target = running,
                                                   current = undefined,
                                                   state = {stopped, _, _}}}},
                 S5),
    {S6, Actions6} = evaluate_stream(meta(?LINE), S5, []),
    ?assertMatch([], Actions6),
    %% DELETED REPLICA START FAIL
    Meta7 = meta(?LINE),
    S7 = update_stream(Meta7, {action_failed, StreamId,
                               #{action => starting,
                                 index => 1,
                                 node => N3,
                                 epoch => E}}, S6),
    {S8, Actions8} = evaluate_stream(Meta7, S7, []),
    ?assertMatch([{aux, {delete_member, _, #{node := N3}, _}}], Actions8),
    %% OTHER REPLICA START FAIL
    Meta9 = meta(?LINE),
    S9 = update_stream(Meta9, {action_failed, StreamId,
                               #{action => starting,
                                 index => 1,
                                 node => N2,
                                 epoch => E}}, S8),
    {_S10, Actions10} = evaluate_stream(Meta9, S9, []),
    ?assertMatch([{aux, {stop, _, _, _}} ], Actions10),
    ok.

delete_replica_leader(_) ->
    %% TOOD: replica and leader needs to be tested
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, _Replica2] = [fake_pid(n2), fake_pid(n3)],
    N1 = node(LeaderPid),
    N2 = node(Replica1),
    %% this is to be added
    % N3 = node(Replica2),

    S0 = started_stream(StreamId, LeaderPid, [Replica1]),
    From = {self(), make_ref()},
    Meta1 = (meta(?LINE))#{from => From},
    S1 = update_stream(Meta1, {delete_replica, StreamId, #{node => N1}}, S0),
    ?assertMatch(#stream{target = running,
                         nodes = [N2],
                         members = #{N1 := #member{target = deleted,
                                                   current = undefined,
                                                   state = {running, _, _}},
                                     N2 := #member{target = stopped,
                                                   current = undefined,
                                                   state = {running, _, _}}
                                    }},
                 S1),
    Idx2 = ?LINE,
    {S2, Actions1} = evaluate_stream(meta(Idx2), S1, []),
    ?assertMatch([{aux, {delete_member, StreamId, #{node := N1}, _}},
                  {aux, {stop, StreamId, #{node := N2, epoch := E}, _}}],
                 lists:sort(Actions1)),
    S3 = S2,
    Idx4 = ?LINE,
    S4 = update_stream(meta(Idx4),
                       {member_stopped, StreamId, #{node => N2,
                                                    index => Idx2,
                                                    epoch => E,
                                                    tail => {E, 100}}},
                       S3),
    E2 = E+1,
    ?assertMatch(#stream{target = running,
                         nodes = [N2],
                         members = #{N1 := #member{target = deleted,
                                                   current = {deleting, Idx2},
                                                   state = {running, _, _}},
                                     N2 := #member{target = running,
                                                   role = {writer, E2},
                                                   current = undefined,
                                                   state = {ready, E2}}
                                    }},
                 S4),
    ok.

member_started_stale_epoch(_) ->
    %% A replica start action was issued in an old epoch and, while it was in
    %% flight, a re-election advanced the stream epoch. The stale member_started
    %% (carrying the old epoch) must clear the 'starting' marker (v8+) so the
    %% member becomes actionable again instead of being stuck with
    %% current = {starting, _} until the next coordinator leader change.
    E1 = 1,
    E2 = 2,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    Replica = fake_pid(n3),
    N1 = node(LeaderPid),
    N3 = node(Replica),
    OldIdx = 99,
    S0 = started_stream(StreamId, LeaderPid, [Replica]),
    %% craft the stranded state: the stream has advanced to epoch 2 with a
    %% running leader, but N3 still carries an in-flight start issued in epoch 1
    %% (target = stopped, as it would be after the leader-down that triggered
    %% the re-election)
    #stream{members = Members0} = S0,
    Members = Members0#{N1 => #member{role = {writer, E2},
                                      state = {running, E2, LeaderPid},
                                      current = undefined},
                        N3 => #member{role = {replica, E2},
                                      state = {ready, E1},
                                      target = stopped,
                                      current = {starting, OldIdx}}},
    S1 = S0#stream{epoch = E2, members = Members},
    StaleStarted = {member_started, StreamId,
                    #{epoch => E1, index => OldIdx, pid => Replica}},

    %% v8: the stale 'starting' marker is cleared
    S2 = update_stream(meta(#{index => ?LINE, machine_version => 8}),
                       StaleStarted, S1),
    ?assertMatch(#stream{members = #{N3 := #member{current = undefined,
                                                   state = {ready, E1}}}},
                 S2),
    %% and the member is now driven to stop, so it can later be restarted in
    %% the current epoch
    {S2Ev, Actions} = evaluate_stream(meta(?LINE), S2, []),
    ?assert(lists:any(fun ({aux, {stop, _, #{node := N}, _}}) -> N == N3;
                          (_) -> false
                      end, Actions)),
    ?assertMatch(#stream{members = #{N3 := #member{current = {stopping, _}}}},
                 S2Ev),

    %% pre-v8: the stale member_started is ignored and the member stays stuck
    S3 = update_stream_v7(meta(#{index => ?LINE, machine_version => 7}),
                          StaleStarted, S1),
    ?assertMatch(#stream{members = #{N3 := #member{current = {starting, OldIdx},
                                                   state = {ready, E1}}}},
                 S3),
    {_, ActionsV7} = evaluate_stream_v7(meta(#{index => ?LINE,
                                               machine_version => 7}), S3, []),
    ?assertNot(lists:any(fun ({aux, {stop, _, #{node := N}, _}}) -> N == N3;
                             (_) -> false
                         end, ActionsV7)),
    ok.

replica_disconnected_nodeup(_) ->
    %% A replica whose node loses its connection (noconnection) is marked
    %% 'disconnected' rather than 'down'. When the node reconnects (v8+) the
    %% member is resumed as running instead of remaining 'disconnected' until
    %% the next stop/restart cycle.
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    Replica = fake_pid(n3),
    N3 = node(Replica),
    S0 = started_stream(StreamId, LeaderPid, [Replica]),
    S1 = update_stream(meta(?LINE), {down, Replica, noconnection}, S0),
    ?assertMatch(#stream{members = #{N3 := #member{state = {disconnected, E, Replica}}}},
                 S1),

    %% v8: the reconnected node's replica is resumed as running
    S2 = update_stream(meta(#{index => ?LINE, machine_version => 8}),
                       {nodeup, N3}, S1),
    ?assertMatch(#stream{members = #{N3 := #member{state = {running, E, Replica}}}},
                 S2),

    %% pre-v8: the replica stays disconnected
    S3 = update_stream_v7(meta(#{index => ?LINE, machine_version => 7}),
                          {nodeup, N3}, S1),
    ?assertMatch(#stream{members = #{N3 := #member{state = {disconnected, E, Replica}}}},
                 S3),
    ok.

nodeup_resumes_only_affected_streams(_) ->
    %% The v8 apply({nodeup, Node}) clause only re-evaluates streams with a
    %% member on Node that was parked for the nodeup (a disconnected replica or
    %% a member sleeping on 'nodeup'). Streams with no member on Node, or whose
    %% on-Node member is already running, must be left untouched and produce no
    %% effects.
    E = 1,
    Leader = fake_pid(n1),
    Replica = fake_pid(n3),
    N3 = node(Replica),

    %% affected stream: replica on N3 disconnected, waiting for the node back
    Affected = "affected",
    A0 = started_stream(Affected, Leader, [Replica]),
    A1 = update_stream(meta(?LINE), {down, Replica, noconnection}, A0),
    ?assertMatch(#stream{members = #{N3 := #member{state = {disconnected, E, Replica}}}},
                 A1),

    %% unaffected stream: members only on n1 and n2, nothing on N3
    Unaffected = "unaffected",
    U0 = started_stream(Unaffected, fake_pid(n1), [fake_pid(n2)]),

    State0 = (rabbit_stream_coordinator:init(#{machine_version => 8}))#?STATE{
               streams = #{Affected => A1, Unaffected => U0}},

    {State1, ok, Effects} =
        apply_cmd(meta(#{index => ?LINE, machine_version => 8}),
                  {nodeup, N3}, State0),

    #?STATE{streams = Streams1, monitors = Monitors1} = State1,

    %% the disconnected replica is resumed as running
    ?assertMatch(#{Affected := #stream{members =
                                       #{N3 := #member{state = {running, E, Replica}}}}},
                 Streams1),
    %% its process monitor is (re-)issued and recorded
    ?assertEqual({Affected, member}, maps:get(Replica, Monitors1)),
    ?assert(lists:member({monitor, process, Replica}, Effects)),

    %% the unaffected stream is byte-identical to its input
    ?assertEqual(U0, maps:get(Unaffected, Streams1)),

    %% the only effect is the single monitor re-issue for the resumed replica
    ?assertEqual([{monitor, process, Replica}], Effects),
    ok.

restart_stream_preserves_deleted(_) ->
    %% restart_stream must never flip a member that is being deleted back to
    %% 'stopped': doing so resurrects the old writer, leaving two members with a
    %% writer role, which crashes find_leader/1 with a case_clause.
    StreamId = atom_to_list(?FUNCTION_NAME),
    OldWriter = fake_pid(n1),
    NewWriter = fake_pid(n2),
    N1 = node(OldWriter),
    N2 = node(NewWriter),
    %% N1 is the old writer lingering with target = deleted, N2 is the freshly
    %% elected writer in the next epoch
    S0 = (started_stream(StreamId, NewWriter, []))#stream{
           epoch = 2,
           nodes = [N1, N2],
           members = #{N1 => #member{role = {writer, 1},
                                     target = deleted,
                                     state = {running, 1, OldWriter},
                                     current = undefined},
                       N2 => #member{role = {writer, 2},
                                     target = running,
                                     state = {running, 2, NewWriter},
                                     current = undefined}}},
    From = {self(), make_ref()},

    %% v8: the deleted member is preserved, exactly one writer remains
    S1 = update_stream((meta(#{index => ?LINE, machine_version => 8}))#{from => From},
                       {restart_stream, StreamId, #{}}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{target = deleted},
                                     N2 := #member{target = stopped}}},
                 S1),
    %% evaluate_stream (which calls find_leader/1) must not crash
    ?assertMatch({#stream{}, _}, evaluate_stream(meta(?LINE), S1, [])),

    %% pre-v8: the deleted member is resurrected to 'stopped', producing two
    %% writers, and evaluate_stream then crashes in find_leader/1
    S1V7 = update_stream_v7((meta(#{index => ?LINE, machine_version => 7}))#{from => From},
                            {restart_stream, StreamId, #{}}, S0),
    ?assertMatch(#stream{members = #{N1 := #member{role = {writer, 1},
                                                   target = stopped},
                                     N2 := #member{role = {writer, 2},
                                                   target = stopped}}},
                 S1V7),
    ?assertError({case_clause, _}, evaluate_stream_v7(meta(?LINE), S1V7, [])),
    ok.

stranded_no_writer_reelection(_) ->
    %% A stream that has settled with no viable writer (e.g. after sequential
    %% member removal during a multi-node loss) and no pending action must be
    %% recovered by the level-triggered backstop re-election (v8+), even though
    %% no further event triggers the edge-triggered election.
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    P1 = fake_pid(n1),
    P2 = fake_pid(n2),
    P3 = fake_pid(n3),
    N1 = node(P1),
    N2 = node(P2),
    N3 = node(P3),
    Stopped = fun (Role, Off) ->
                      #member{role = Role,
                              target = stopped,
                              state = {stopped, E, {E, Off}},
                              current = undefined}
              end,
    S0 = (started_stream(StreamId, P1, [P2, P3]))#stream{
           epoch = E,
           nodes = [N1, N2, N3],
           members = #{N1 => Stopped({writer, E}, 100),
                       N2 => Stopped({replica, E}, 100),
                       N3 => Stopped({replica, E}, 90)}},

    %% v8: the backstop elects a new leader in the next epoch and starts it
    {S1, Actions1} = evaluate_stream(meta(#{index => ?LINE,
                                            machine_version => 8}), S0, []),
    ?assertMatch(#stream{epoch = 2}, S1),
    ?assert(lists:any(fun ({aux, {start_writer, _, _, _}}) -> true;
                          (_) -> false
                      end, Actions1)),
    #stream{members = Mem1} = S1,
    Writers = maps:filter(fun (_, #member{role = {writer, _}}) -> true;
                              (_, _) -> false
                          end, Mem1),
    ?assertEqual(1, map_size(Writers)),

    %% pre-v8: no backstop, the stream stays stranded with no writer
    {S2, Actions2} = evaluate_stream_v7(meta(#{index => ?LINE,
                                               machine_version => 7}), S0, []),
    ?assertMatch(#stream{epoch = E}, S2),
    ?assertEqual([], [A || {aux, {start_writer, _, _, _}} = A <- Actions2]),
    ok.

%% The leader must never run more than max_concurrency action workers at once;
%% excess actions are queued and started as running ones complete.
action_throttling(_) ->
    MachineState = (rabbit_stream_coordinator:init(#{machine_version => 8}))#?STATE{
                     streams = #{"s" => #stream{}}},
    meck:expect(ra_aux, machine_state, fun(_) -> MachineState end),
    RaAux = fake_ra_aux,
    NoOp = fun() -> ok end,
    Args = #{node => n1, epoch => 1, index => 1},
    Run = fun(Aux) ->
                  rabbit_stream_coordinator:run_action(starting, "s", Args, NoOp,
                                                       Aux, RaAux)
          end,
    Complete = fun(Aux) ->
                       [Pid | _] = rabbit_stream_coordinator:aux_running_pids(Aux),
                       rabbit_stream_coordinator:handle_aux(
                         leader, undefined, {down, Pid, normal}, Aux, RaAux)
               end,

    %% issue four actions with a limit of two
    Aux0 = rabbit_stream_coordinator:make_aux(2),
    {no_reply, Aux1, _, E1} = Run(Aux0),
    {no_reply, Aux2, _, E2} = Run(Aux1),
    {no_reply, Aux3, _, E3} = Run(Aux2),
    {no_reply, Aux4, _, E4} = Run(Aux3),

    %% only two started (each emitting a monitor effect), two are queued
    ?assertEqual(2, rabbit_stream_coordinator:aux_running_count(Aux4)),
    ?assertEqual(2, rabbit_stream_coordinator:aux_pending_count(Aux4)),
    ?assertMatch([{monitor, process, aux, _}], E1),
    ?assertMatch([{monitor, process, aux, _}], E2),
    ?assertEqual([], E3),
    ?assertEqual([], E4),

    %% each completion starts exactly one queued action until the queue drains
    {no_reply, Aux5, _, E5} = Complete(Aux4),
    ?assertEqual(2, rabbit_stream_coordinator:aux_running_count(Aux5)),
    ?assertEqual(1, rabbit_stream_coordinator:aux_pending_count(Aux5)),
    ?assertMatch([{monitor, process, aux, _}], E5),

    {no_reply, Aux6, _, _} = Complete(Aux5),
    ?assertEqual(2, rabbit_stream_coordinator:aux_running_count(Aux6)),
    ?assertEqual(0, rabbit_stream_coordinator:aux_pending_count(Aux6)),

    %% with the queue empty, a completion just frees the slot
    {no_reply, Aux7, _, E7} = Complete(Aux6),
    ?assertEqual(1, rabbit_stream_coordinator:aux_running_count(Aux7)),
    ?assertEqual(0, rabbit_stream_coordinator:aux_pending_count(Aux7)),
    ?assertEqual([], E7),
    ok.

%% A queued action whose stream was deleted while it waited is dropped rather
%% than started (starting it would orphan an osiris member).
action_throttling_drops_deleted_stream(_) ->
    MachineState = (rabbit_stream_coordinator:init(#{machine_version => 8}))#?STATE{
                     streams = #{"live" => #stream{}}},
    meck:expect(ra_aux, machine_state, fun(_) -> MachineState end),
    RaAux = fake_ra_aux,
    NoOp = fun() -> ok end,
    Args = #{node => n1, epoch => 1, index => 1},
    Run = fun(Stream, Aux) ->
                  rabbit_stream_coordinator:run_action(starting, Stream, Args,
                                                       NoOp, Aux, RaAux)
          end,

    %% limit of one: fill the slot, then queue one for a deleted stream and one
    %% for the live stream
    Aux0 = rabbit_stream_coordinator:make_aux(1),
    {no_reply, Aux1, _, _} = Run("live", Aux0),
    {no_reply, Aux2, _, []} = Run("gone", Aux1),
    {no_reply, Aux3, _, []} = Run("live", Aux2),
    ?assertEqual(1, rabbit_stream_coordinator:aux_running_count(Aux3)),
    ?assertEqual(2, rabbit_stream_coordinator:aux_pending_count(Aux3)),

    %% freeing the slot drops the "gone" action and starts the "live" one
    [Pid | _] = rabbit_stream_coordinator:aux_running_pids(Aux3),
    {no_reply, Aux4, _, E4} = rabbit_stream_coordinator:handle_aux(
                                leader, undefined, {down, Pid, normal}, Aux3, RaAux),
    ?assertEqual(1, rabbit_stream_coordinator:aux_running_count(Aux4)),
    ?assertEqual(0, rabbit_stream_coordinator:aux_pending_count(Aux4)),
    ?assertMatch([{monitor, process, aux, _}], E4),
    ok.

%% The aux state is transient and is not re-initialised by Ra when the machine
%% version changes in place, so handle_aux must upgrade an aux record built by
%% a pre-v8 version (record tag 'aux', {aux, Actions, Resizer}) rather than
%% silently dropping the command.
aux_upgrade_from_prior_version(_) ->
    MachineState = (rabbit_stream_coordinator:init(#{machine_version => 8}))#?STATE{
                     streams = #{"s" => #stream{}}},
    meck:expect(ra_aux, machine_state, fun(_) -> MachineState end),
    RaAux = fake_ra_aux,
    Args = #{node => n1, epoch => 1, index => 1},
    P1 = spawn(fun() -> ok end),
    P2 = spawn(fun() -> ok end),
    %% a pre-v8 aux record with two in-flight actions and no resizer
    V7Aux = {aux,
             #{P1 => {"s", starting, Args},
               P2 => {"s", starting, Args}},
             undefined},
    %% a completion for P1 arriving on the old-shaped aux is handled without
    %% crashing: the record is upgraded, P1 removed and P2 preserved
    {no_reply, Aux1, _, _} = rabbit_stream_coordinator:handle_aux(
                               leader, undefined, {down, P1, normal}, V7Aux, RaAux),
    ?assertEqual(1, rabbit_stream_coordinator:aux_running_count(Aux1)),
    ?assertEqual(0, rabbit_stream_coordinator:aux_pending_count(Aux1)),
    ok.

overview(_Config) ->
    S0 = rabbit_stream_coordinator:init(#{machine_version => 5}),
    O0 = rabbit_stream_coordinator:overview(S0),
    ?assertMatch(#{num_monitors := 0,
                   num_streams := 0,
                   single_active_consumer := #{groups := _,
                                               num_groups := 0},
                   streams := #{}}, O0),

    StreamId = <<"bananas">>,
    TypeState = #{name => StreamId,
                  retention => [],
                  nodes => [node()]},
    Q = new_q(<<"bananas">>, TypeState),
    Cmd = {new_stream, StreamId, #{leader_node => node(),
                                   retention => [],
                                   queue => Q}},
    {S1, _, _} = apply_cmd(meta(#{index => 1,
                                  machine_version => 3,
                                  system_time => 203984982374}), Cmd, S0),

    ?assertMatch(#{num_monitors := 0,
                   num_streams := 1,
                   single_active_consumer := #{groups := _,
                                               num_groups := 0},
                   streams := #{StreamId := _}},
                 rabbit_stream_coordinator:overview(S1)),

    ok.

%% A failed member action with a backoff class parks the member (v8+) instead
%% of retrying immediately, and the retry_reconcile timer re-drives it once due.
action_failed_short_parks_and_reconciles(_) ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N2 = node(Replica1),
    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, Replica1, boom}, S0),
    StartIdx = ?LINE,
    {S2, [{aux, {start_replica, StreamId, #{node := N2}, _}}]} =
        evaluate_stream(meta(StartIdx), S1, []),
    ?assertMatch(#stream{members = #{N2 := #member{current = {starting, StartIdx}}}}, S2),
    State1 = coordinator_state_with(StreamId, S2),
    %% the replica start fails with a short backoff
    FailIdx = ?LINE,
    FailTime = FailIdx * 2,
    RetryAt = FailTime + ?ACTION_RETRY_SHORT_MS,
    {State2, ok, Effs} =
        apply_cmd(meta(FailIdx),
                  {action_failed, StreamId, #{node => N2,
                                              index => StartIdx,
                                              epoch => E,
                                              action => starting,
                                              backoff => short}},
                  State1),
    ?assertMatch(#{StreamId := #stream{members = #{N2 := #member{current = {sleeping, RetryAt}}}}},
                 streams(State2)),
    %% parked, so no immediate re-drive, but a retry timer is armed
    ?assertEqual(false, has_aux_start_replica(Effs, N2)),
    ?assertEqual({true, ?ACTION_RETRY_SHORT_MS}, find_retry_timer(Effs)),
    %% a reconcile before the retry is due changes nothing
    {State3, ok, EffsEarly} =
        apply_cmd(meta(#{index => ?LINE, system_time => RetryAt - 1}),
                  {timeout, retry_reconcile}, State2),
    ?assertMatch(#{StreamId := #stream{members = #{N2 := #member{current = {sleeping, RetryAt}}}}},
                 streams(State3)),
    ?assertEqual(false, has_aux_start_replica(EffsEarly, N2)),
    ?assertEqual({true, 1}, find_retry_timer(EffsEarly)),
    %% once due, the member is re-driven and the timer is not re-armed
    ReconcileIdx = ?LINE,
    {State4, ok, EffsDue} =
        apply_cmd(meta(#{index => ReconcileIdx, system_time => RetryAt}),
                  {timeout, retry_reconcile}, State3),
    ?assertMatch(#{StreamId := #stream{members = #{N2 := #member{current = {starting, ReconcileIdx}}}}},
                 streams(State4)),
    ?assertEqual(true, has_aux_start_replica(EffsDue, N2)),
    ?assertEqual({false, undefined}, find_retry_timer(EffsDue)),
    ?assertEqual(0, parked_size(State4)),
    ok.

%% A 'none' backoff (e.g. writer start failures) must retry immediately without
%% parking, preserving fast re-election behaviour.
action_failed_no_backoff_retries_immediately(_) ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N2 = node(Replica1),
    S0 = started_stream(StreamId, LeaderPid, ReplicaPids),
    S1 = update_stream(meta(?LINE), {down, Replica1, boom}, S0),
    StartIdx = ?LINE,
    {S2, _} = evaluate_stream(meta(StartIdx), S1, []),
    State1 = coordinator_state_with(StreamId, S2),
    {State2, ok, Effs} =
        apply_cmd(meta(?LINE),
                  {action_failed, StreamId, #{node => N2,
                                              index => StartIdx,
                                              epoch => E,
                                              action => starting,
                                              backoff => none}},
                  State1),
    ?assertMatch(#{StreamId := #stream{members = #{N2 := #member{current = {starting, _}}}}},
                 streams(State2)),
    ?assertEqual(true, has_aux_start_replica(Effs, N2)),
    ?assertEqual({false, undefined}, find_retry_timer(Effs)),
    ?assertEqual(0, parked_size(State2)),
    ok.

%% member.current is authoritative: a parked entry whose member is no longer
%% sleeping at exactly that retry time is stale and dropped without re-driving.
reconcile_drops_superseded_parked_entry(_) ->
    E = 1,
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    [Replica1, Replica2] = ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    N2 = node(Replica1),
    Stream = started_stream(StreamId, LeaderPid, ReplicaPids),
    RetryAt = 1000,
    Parked = gb_trees:insert({RetryAt, StreamId, N2}, [], gb_trees:empty()),
    Base = rabbit_stream_coordinator:init(#{machine_version => 8}),
    State0 = Base#rabbit_stream_coordinator{streams = #{StreamId => Stream},
                                            parked = Parked},
    {State1, ok, Effs} =
        apply_cmd(meta(#{index => ?LINE, system_time => RetryAt}),
                  {timeout, retry_reconcile}, State0),
    ?assertEqual(0, parked_size(State1)),
    ?assertMatch(#{StreamId := #stream{members = #{N2 := #member{current = undefined,
                                                                 state = {running, E, Replica1}}}}},
                 streams(State1)),
    ?assertEqual(false, has_aux_start_replica(Effs, N2)),
    ?assertEqual({false, undefined}, find_retry_timer(Effs)),
    ok.

%% Timers are lost on leader change but the parked actions survive in the
%% replicated state, so state_enter(leader) re-arms the reconcile.
state_enter_rearms_retry_timer(_) ->
    StreamId = atom_to_list(?FUNCTION_NAME),
    LeaderPid = fake_pid(n1),
    ReplicaPids = [fake_pid(n2), fake_pid(n3)],
    Stream = started_stream(StreamId, LeaderPid, ReplicaPids),
    Base = rabbit_stream_coordinator:init(#{machine_version => 8}),
    Empty = Base#rabbit_stream_coordinator{streams = #{StreamId => Stream}},
    ?assertEqual({false, undefined},
                 find_retry_timer(rabbit_stream_coordinator:state_enter(leader, Empty))),
    %% a retry due in the past (RetryAt = 0) re-arms with a zero delay so the
    %% overdue retry fires as soon as this node becomes leader
    Parked = gb_trees:insert({0, StreamId, node(fake_pid(n2))},
                             [], gb_trees:empty()),
    State = Empty#rabbit_stream_coordinator{parked = Parked},
    ?assertEqual({true, 0},
                 find_retry_timer(rabbit_stream_coordinator:state_enter(leader, State))),
    ok.

coordinator_state_with(StreamId, #stream{conf = Conf, members = Members} = Stream) ->
    %% the full apply pipeline runs eval_retention, which requires a retention
    %% in the stream conf and map-valued member confs
    Conf1 = Conf#{retention => []},
    Members1 = maps:map(fun (_, M) -> M#member{conf = Conf1} end, Members),
    Stream1 = Stream#stream{conf = Conf1, members = Members1},
    Base = rabbit_stream_coordinator:init(#{machine_version => 8}),
    Base#rabbit_stream_coordinator{streams = #{StreamId => Stream1}}.

streams(#rabbit_stream_coordinator{streams = Streams}) ->
    Streams.

parked_size(#rabbit_stream_coordinator{parked = undefined}) ->
    0;
parked_size(#rabbit_stream_coordinator{parked = Parked}) ->
    gb_trees:size(Parked).

has_aux_start_replica(Effs, Node) ->
    lists:any(fun ({aux, {start_replica, _, #{node := N}, _}}) ->
                      N =:= Node;
                  (_) ->
                      false
              end, Effs).

find_retry_timer(Effs) ->
    case [D || {timer, retry_reconcile, D} <- Effs] of
        [D | _] ->
            {true, D};
        [] ->
            {false, undefined}
    end.

meta(N) when is_integer(N) ->
    meta(#{index => N});
meta(#{index := N} = M) when is_map(M) ->
    maps:merge(#{term => 1,
                 machine_version => rabbit_stream_coordinator:version(),
                 system_time => N * 2}, M).

started_stream(StreamId, LeaderPid, ReplicaPids) ->
    E = 1,
    Nodes = [node(LeaderPid) | [node(P) || P <- ReplicaPids]],
    Conf = #{name => StreamId,
             nodes => Nodes},

    VHost = <<"/">>,
    QName = #resource{kind = queue,
                      name = list_to_binary(StreamId),
                      virtual_host = VHost},
    Members0 = #{node(LeaderPid) => #member{role = {writer, E},
                                            state = {running, E, LeaderPid},
                                            current = undefined}},
    Members = lists:foldl(fun (R, Acc) ->
                                  N = node(R),
                                  Acc#{N => #member{role = {replica, E},
                                                    state = {running, E, R},
                                                    current = undefined}}
                          end, Members0, ReplicaPids),


    #stream{id = StreamId,
            epoch = 1,
            nodes = Nodes,
            queue_ref = QName,
            conf = Conf,
            mnesia = {updated, 1},
            members = Members}.

new_q(Name, TypeState) ->
    VHost = <<"/">>,
    QName = #resource{kind = queue,
                      name = Name,
                      virtual_host = VHost},
    amqqueue:set_type_state(
      amqqueue:new_with_version(amqqueue_v2,
                                QName,
                                none,
                                true,
                                false,
                                none,
                                [],
                                VHost,
                                #{},
                                rabbit_stream_queue), TypeState).

fake_pid(Node) ->
    NodeBin = atom_to_binary(Node),
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    %% drop the local node data from a local pid
    <<Pre:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(NodeBin),
    %% get the encoding type of the pid
    <<_:8, Type:8/unsigned, _/binary>> = Pre,
    %% replace it with the incoming node binary
    Final = <<131, Type, 100, S:16/unsigned, NodeBin/binary, LocalPidData/binary>>,
    binary_to_term(Final).

%% Utility
