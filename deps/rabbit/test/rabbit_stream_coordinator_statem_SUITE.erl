%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_stream_coordinator_statem_SUITE).

-behaviour(proper_statem).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/src/rabbit_stream_coordinator.hrl").

-define(NUM_TESTS, 2000).
-define(NODE_POOL, [n1, n2, n3]).
-define(EXTRA_NODES, [nx, ny]).

%% An async reply the coordinator is waiting for, parsed from an {aux, _}
%% effect.
-record(pending, {kind     :: started | stopped | deleted | mnesia | retention,
                  node     :: atom(),
                  epoch    :: integer(),
                  ok_cmd   :: term(),
                  fail_cmd :: term()}).

%% proper_statem model: just enough to generate sensible commands.
-record(st, {setup = false :: boolean(),
             nodes = []    :: [atom()]}).

all() ->
    [prop_writer_always_recovers].

%% -------------------------------------------------------------------
%% Suite setup/teardown (no broker required).
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok.

%% -------------------------------------------------------------------
%% Property.
%% -------------------------------------------------------------------

%% Liveness: after ANY sequence of member failures (writer included),
%% async-reply deliveries (in any order, including stale ones) and membership
%% changes (delete_replica, add_replica, restart_stream), the most cooperative
%% possible environment (every outstanding action eventually succeeds and
%% every node reconnects) can always bring the stream back to a running writer.
prop_writer_always_recovers(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_writer_always_recovers_body/0, [], ?NUM_TESTS).

prop_writer_always_recovers_body() ->
    ?FORALL(Commands, commands(?MODULE),
            begin
                _ = erase(world),
                {History, _State, Result} = run_commands(?MODULE, Commands),
                {Converged, Drained} =
                    case world() of
                        undefined -> {true, undefined};
                        World     -> drain(World)
                    end,
                Recovered = Converged andalso
                    (Drained =:= undefined orelse writer_running(Drained)),
                _ = erase(world),
                ?WHENFAIL(
                   begin
                       ct:pal("Command sequence left the writer unrecoverable.~n"
                              "Commands:~n~s~n"
                              "Final (cooperatively drained) state:~n~s",
                              [format_commands(Commands), format_world(Drained)]),
                       io:format("History length: ~p~n", [length(History)])
                   end,
                   Result =:= ok andalso Recovered)
            end).

%% -------------------------------------------------------------------
%% proper_statem callbacks.
%% -------------------------------------------------------------------

initial_state() ->
    #st{}.

command(#st{setup = false}) ->
    {call, ?MODULE, cmd_setup, [?NODE_POOL]};
command(#st{setup = true}) ->
    Pool = ?NODE_POOL ++ ?EXTRA_NODES,
    frequency(
      [{30, {call, ?MODULE, cmd_deliver, [elements(Pool), elements([ok, fail])]}},
       {15, {call, ?MODULE, cmd_crash, [elements(Pool),
                                        elements([normal, noconnection])]}},
       {10, {call, ?MODULE, cmd_nodeup, [elements(Pool)]}},
       {8,  {call, ?MODULE, cmd_delete_replica, [elements(Pool)]}},
       {8,  {call, ?MODULE, cmd_add_replica, [elements(?EXTRA_NODES)]}},
       {6,  {call, ?MODULE, cmd_restart_stream, []}}]).

precondition(#st{setup = Setup}, {call, _, cmd_setup, _}) ->
    not Setup;
precondition(#st{setup = Setup}, {call, _, _, _}) ->
    Setup.

%% The commands are self-correcting no-ops when they do not apply (the
%% coordinator ignores irrelevant/stale commands), so postconditions only guard
%% against unexpected crashes; the liveness check runs once at the end.
postcondition(_State, {call, _, _, _}, _Result) ->
    true.

next_state(St, _Res, {call, _, cmd_setup, [Nodes]}) ->
    St#st{setup = true, nodes = Nodes};
next_state(#st{nodes = Ns} = St, _Res, {call, _, cmd_add_replica, [N]}) ->
    St#st{nodes = lists:usort([N | Ns])};
next_state(#st{nodes = Ns} = St, _Res, {call, _, cmd_delete_replica, [N]}) ->
    St#st{nodes = Ns -- [N]};
next_state(St, _Res, {call, _, _, _}) ->
    St.

%% -------------------------------------------------------------------
%% Commands: operate on the model world held in the process dictionary.
%% -------------------------------------------------------------------

cmd_setup(Nodes) ->
    put(world, initial(Nodes)),
    ok.

cmd_deliver(Node, Outcome) ->
    W = world(),
    case find_pending(Node, in_flight(W)) of
        {ok, P} -> put(world, deliver(W, P, Outcome));
        none    -> ok
    end,
    ok.

cmd_crash(Node, Reason) ->
    W = world(),
    case running_pid(Node, coord(W)) of
        {ok, Pid} ->
            R = case Reason of noconnection -> noconnection; _ -> boom end,
            put(world, apply_cmd(W, {down, Pid, R}));
        none -> ok
    end,
    ok.

cmd_nodeup(Node) ->
    put(world, apply_cmd(world(), {nodeup, Node})),
    ok.

cmd_delete_replica(Node) ->
    put(world, apply_cmd(world(), {delete_replica, "s1", #{node => Node}})),
    ok.

cmd_add_replica(Node) ->
    put(world, apply_cmd(world(), {add_replica, "s1", #{node => Node}})),
    ok.

cmd_restart_stream() ->
    put(world, apply_cmd(world(), {restart_stream, "s1", #{}})),
    ok.

%% -------------------------------------------------------------------
%% The model world: {Idx, Coord, InFlight}.
%% -------------------------------------------------------------------

world() -> get(world).
coord({_Idx, Coord, _InFlight}) -> Coord.
in_flight({_Idx, _Coord, InFlight}) -> InFlight.

meta(N) ->
    #{index => N, term => 1, machine_version => rabbit_stream_coordinator:version(), system_time => N * 2}.

%% Build a pid whose node/1 is an arbitrary atom, without distribution.
fake_pid(Node) ->
    NodeBin = atom_to_binary(Node),
    ThisNodeSize = size(term_to_binary(node())) + 1,
    Pid = spawn(fun () -> ok end),
    <<Pre:ThisNodeSize/binary, LocalPidData/binary>> = term_to_binary(Pid),
    S = size(NodeBin),
    <<_:8, Type:8/unsigned, _/binary>> = Pre,
    Final = <<131, Type, 100, S:16/unsigned, NodeBin/binary, LocalPidData/binary>>,
    binary_to_term(Final).

%% A freshly running stream on the given nodes (first is the writer).
started_stream(StreamId, Nodes) ->
    E = 1,
    [LeaderPid | ReplicaPids] = [fake_pid(N) || N <- Nodes],
    Conf = #{name => StreamId, nodes => Nodes, retention => []},
    QName = #resource{kind = queue, name = list_to_binary(StreamId),
                      virtual_host = <<"/">>},
    Members0 = #{hd(Nodes) => #member{role = {writer, E},
                                      state = {running, E, LeaderPid},
                                      current = undefined, conf = Conf,
                                      target = running}},
    Members = lists:foldl(
                fun ({N, R}, Acc) ->
                        Acc#{N => #member{role = {replica, E},
                                          state = {running, E, R},
                                          current = undefined, conf = Conf,
                                          target = running}}
                end, Members0, lists:zip(tl(Nodes), ReplicaPids)),
    #stream{id = StreamId, epoch = E, nodes = Nodes, queue_ref = QName,
            conf = Conf, mnesia = {updated, E}, members = Members,
            target = running}.

initial(Nodes) ->
    Stream = started_stream("s1", Nodes),
    Base = rabbit_stream_coordinator:init(#{machine_version => rabbit_stream_coordinator:version()}),
    Monitors = maps:fold(
                 fun (_N, #member{state = {running, _, Pid}}, Acc) ->
                         Acc#{Pid => {Stream#stream.id, member}};
                     (_N, _M, Acc) -> Acc
                 end, #{}, Stream#stream.members),
    Coord = Base#rabbit_stream_coordinator{
              streams  = #{Stream#stream.id => Stream},
              monitors = Monitors},
    {1, Coord, []}.

stream_of(#rabbit_stream_coordinator{streams = Streams}) ->
    maps:get("s1", Streams, undefined).

%% Apply a command through the real coordinator and harvest new aux effects.
apply_cmd({Idx, Coord, InFlight}, Cmd) ->
    {Coord1, _Reply, Effects} =
        rabbit_stream_coordinator:apply(meta(Idx), Cmd, Coord),
    New = [aux_to_pending(Aux, Coord1)
           || {aux, Aux} <- Effects, is_tuple(Aux)],
    {Idx + 1, Coord1, InFlight ++ [P || P <- New, P =/= skip]}.

aux_to_pending({start_writer, Id, Args, _Conf}, _C) -> start_pending(Id, Args);
aux_to_pending({start_replica, Id, Args, _Conf}, _C) -> start_pending(Id, Args);
aux_to_pending({stop, Id, #{node := N, epoch := E} = Args, _Conf}, _C) ->
    #pending{kind = stopped, node = N, epoch = E,
             ok_cmd = {member_stopped, Id, Args#{tail => {E, 100}}},
             fail_cmd = {action_failed, Id, Args#{action => stopping}}};
aux_to_pending({delete_member, Id, #{node := N, epoch := E} = Args, _Conf}, _C) ->
    #pending{kind = deleted, node = N, epoch = E,
             ok_cmd = {member_deleted, Id, #{node => N}},
             fail_cmd = {action_failed, Id, Args#{action => deleting}}};
aux_to_pending({delete_member, Id, Node, _Conf}, C) when is_atom(Node) ->
    St = stream_of(C),
    {Idx, E} = case St of
                   #stream{epoch = SE,
                           members = #{Node := #member{current = {deleting, I}}}} ->
                       {I, SE};
                   #stream{epoch = SE} -> {0, SE}
               end,
    #pending{kind = deleted, node = Node, epoch = E,
             ok_cmd = {member_deleted, Id, #{node => Node}},
             fail_cmd = {action_failed, Id, #{node => Node, index => Idx,
                                              action => deleting, epoch => E}}};
aux_to_pending({update_mnesia, Id, #{epoch := E, node := N}, _Conf}, _C) ->
    #pending{kind = mnesia, node = N, epoch = E,
             ok_cmd = {mnesia_updated, Id, #{epoch => E}},
             fail_cmd = {action_failed, Id, #{action => updating_mnesia,
                                              index => 0, node => N, epoch => E}}};
aux_to_pending({update_retention, Id, #{node := N, epoch := E} = Args, _Conf}, _C) ->
    #pending{kind = retention, node = N, epoch = E,
             ok_cmd = {retention_updated, Id, Args},
             fail_cmd = {action_failed, Id, Args#{action => update_retention}}};
aux_to_pending(_Other, _C) ->
    skip.

start_pending(Id, #{node := N, epoch := E} = Args) ->
    #pending{kind = started, node = N, epoch = E,
             ok_cmd = {member_started, Id, Args#{pid => fake_pid(N)}},
             fail_cmd = {action_failed, Id, Args#{action => starting}}}.

deliver({Idx, Coord, InFlight}, P, Outcome) ->
    Cmd = case Outcome of ok -> P#pending.ok_cmd; fail -> P#pending.fail_cmd end,
    apply_cmd({Idx, Coord, InFlight -- [P]}, Cmd).

find_pending(Node, InFlight) ->
    case [P || #pending{node = N} = P <- InFlight, N == Node] of
        [P | _] -> {ok, P};
        []      -> none
    end.

running_pid(Node, Coord) ->
    case stream_of(Coord) of
        #stream{members = #{Node := #member{state = {running, _, Pid}}}} ->
            {ok, Pid};
        #stream{members = #{Node := #member{state = {disconnected, _, Pid}}}} ->
            {ok, Pid};
        _ ->
            none
    end.

writer_running({_Idx, Coord, _}) ->
    case stream_of(Coord) of
        undefined -> true;
        #stream{members = Members} ->
            lists:any(fun (#member{role = {writer, _}, state = {running, _, _}}) ->
                              true;
                          (_) -> false
                      end, maps:values(Members))
    end.

%% Cooperative drain: deliver every in-flight reply as success and reconnect
%% every disconnected node (each at most once), to a fixpoint. If even this
%% cannot restore a running writer, the state is a genuine liveness dead-end.
drain(World) -> drain(World, 1000, #{}).
drain(World, 0, _Done) -> {false, World};
drain({_Idx, Coord, InFlight} = World, Fuel, Done) ->
    case InFlight of
        [P | _] ->
            drain(deliver(World, P, ok), Fuel - 1, Done);
        [] ->
            case [N || {ok, N} <- [disconnected_node(stream_of(Coord))],
                       not maps:is_key(N, Done)] of
                [N | _] ->
                    drain(apply_cmd(World, {nodeup, N}), Fuel - 1, Done#{N => true});
                [] ->
                    {true, World}
            end
    end.

disconnected_node(undefined) -> none;
disconnected_node(#stream{members = Members}) ->
    case [N || N := #member{state = {disconnected, _, _}} <- Members] of
        [N | _] -> {ok, N};
        []      -> none
    end.

%% -------------------------------------------------------------------
%% Pretty-printing for counterexamples.
%% -------------------------------------------------------------------

format_commands(Commands) ->
    [io_lib:format("  ~p~n", [Call])
     || {set, _Var, {call, _M, F, A}} <- Commands,
        Call <- [list_to_tuple([F | A])]].

format_world(undefined) ->
    "  <no stream set up>\n";
format_world({Idx, Coord, InFlight}) ->
    Head = io_lib:format("  idx=~p in_flight=~p~n",
                         [Idx, [{P#pending.kind, P#pending.node} || P <- InFlight]]),
    Body = case stream_of(Coord) of
               undefined -> "  stream deleted\n";
               #stream{epoch = E, target = T, members = Members} ->
                   [io_lib:format("  epoch=~p target=~p~n", [E, T]) |
                    [io_lib:format("    ~p: role=~p state=~p current=~p target=~p~n",
                                   [N, R, strip(S), Cur, MT])
                     || {N, #member{role = R, state = S, current = Cur,
                                    target = MT}} <- lists:sort(maps:to_list(Members))]]
           end,
    [Head | Body].

strip({running, E, _}) -> {running, E, pid};
strip({disconnected, E, _}) -> {disconnected, E, pid};
strip({stopped, E, _}) -> {stopped, E, tail};
strip(S) -> S.
