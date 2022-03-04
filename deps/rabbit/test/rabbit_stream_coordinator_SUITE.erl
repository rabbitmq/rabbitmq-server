-module(rabbit_stream_coordinator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
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
     new_stream,
     leader_down,
     leader_down_scenario_1,
     replica_down,
     add_replica,
     delete_stream,
     delete_replica_leader,
     delete_replica,
     delete_two_replicas,
     delete_replica_2,
     leader_start_failed
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

init_per_testcase(_TestCase, Config) ->
    Config.

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

register_listener(Args, S) ->
    apply_cmd(#{index => 42, machine_version => 2}, {register_listener, Args}, S).

eval_listeners(Stream) ->
    rabbit_stream_coordinator:eval_listeners(2, Stream, []).

down(Pid, S) ->
    apply_cmd(#{index => 42, machine_version => 2}, {down, Pid, reason}, S).


listeners(_) ->
    S = <<"stream">>,
    Q = #resource{kind = queue, name = S, virtual_host = <<"/">>},
    ListPid = spawn(fun() -> ok end),
    N1 = r@n1,
    State0 = #?STATE{streams = #{}, monitors = #{}},
    ?assertMatch(
       {_, stream_not_found, []},
       register_listener(#{pid => ListPid,
                           node => N1,
                           stream_id => S,
                           type => leader}, State0)
      ),

    LeaderPid0 = spawn(fun() -> ok end),
    Leader0 = #member{
                role = {writer, 1},
                state = {running, 1, LeaderPid0},
                node = N1
               },
    State1 = State0#?STATE{
                       streams = #{S => #stream{
                                           listeners = #{},
                                           members = #{N1 => Leader0},
                                           queue_ref = Q}
                      }},

    {State2, ok, Effs2} = register_listener(#{pid => ListPid,
                                              node => N1,
                                              stream_id => S,
                                              type => leader}, State1),
    Stream2 = maps:get(S, State2#?STATE.streams),
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
       #{ListPid => {#{S => ok}, listener}},
       State2#?STATE.monitors
      ),

    {State3, ok, Effs3} = register_listener(#{pid => ListPid,
                                              node => N1,
                                              stream_id => S,
                                              type => local_member}, State2),
    Stream3 = maps:get(S, State3#?STATE.streams),
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
       #{ListPid => {#{S => ok}, listener}},
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

    State5 = State3#?STATE{streams = #{S => Stream5}},

    {State6, ok, []} = down(ListPid, State5),

    Stream6 = maps:get(S, State6#?STATE.streams),
    ?assertEqual(
       #{},
       Stream6#stream.listeners
      ),

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
    #{index := Idx4} = Meta4 = meta(?LINE),
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
    %% idempotency test
    _ = update_stream(Meta1, {delete_stream, StreamId, #{}}, S7),
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

meta(N) when is_integer(N) ->
    #{index => N,
      machine_version => 1,
      system_time => N + 1000}.

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
                                            node = node(LeaderPid),
                                            state = {running, E, LeaderPid},
                                            current = undefined}},
    Members = lists:foldl(fun (R, Acc) ->
                                  N = node(R),
                                  Acc#{N => #member{role = {replica, E},
                                                    node = N,
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
