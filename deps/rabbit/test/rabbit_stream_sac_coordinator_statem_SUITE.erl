%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_stream_sac_coordinator_statem_SUITE).

-behaviour(proper_statem).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/src/rabbit_stream_sac_coordinator.hrl").

-define(NUM_TESTS, 2000).
-define(MOD, rabbit_stream_sac_coordinator).
-define(VH, <<"/">>).
-define(STREAM, <<"s">>).
-define(NAME, <<"app">>).
-define(GID, {?VH, ?STREAM, ?NAME}).
-define(CONNS, [c1, c2, c3, c4]).
-define(PARTITION_INDEXES, [-1, 0, 1, 2]).

%% proper_statem model: tracks each connection's lifecycle so that only
%% protocol-realistic transitions are generated (a connection goes
%% connected -> disconnected -> presumed_down and back via reconnect; it never
%% receives a second down event, which an Erlang monitor would not produce).
-record(st, {setup = false :: boolean(),
             conns = #{}    :: #{atom() => connected | disconnected | presumed_down}}).

all() ->
    [prop_single_active_consumer_converges].

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

%% Convergence property: after ANY sequence of consumer
%% registrations, unregistrations, connection failures (normal/noconnection),
%% disconnect timeouts and reconnections, the most cooperative environment
%% (every disconnected/presumed-down consumer reconnects and every
%% `deactivating' step-down is answered with an activate_consumer) can always
%% bring a non-empty group back to exactly one {connected, active} consumer.
prop_single_active_consumer_converges(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_single_active_consumer_converges_body/0, [], ?NUM_TESTS).

prop_single_active_consumer_converges_body() ->
    ?FORALL(Commands, commands(?MODULE),
            begin
                _ = erase_state(),
                {History, _State, Result} = run_commands(?MODULE, Commands),
                World = world(),
                {Converged, Drained} =
                    case World of
                        undefined -> {true, undefined};
                        _         -> drain(World)
                    end,
                Ok = Converged andalso
                    (Drained =:= undefined orelse converged(Drained)),
                _ = erase_state(),
                %% Result =/= ok signals a safety violation (a postcondition
                %% failed mid-run); Ok =:= false signals a liveness one.
                ?WHENFAIL(
                   begin
                       ct:pal("Commands:~n~s~n"
                              "Group at end of run:~n~s"
                              "After cooperative drain:~n~s",
                              [format_commands(Commands),
                               format_group(World), format_group(Drained)]),
                       io:format("History length: ~p~n", [length(History)])
                   end,
                   Result =:= ok andalso Ok)
            end).

%% -------------------------------------------------------------------
%% proper_statem callbacks.
%% -------------------------------------------------------------------

initial_state() ->
    #st{}.

command(#st{setup = false}) ->
    {call, ?MODULE, cmd_setup, [elements(?PARTITION_INDEXES)]};
command(#st{setup = true, conns = Cs}) ->
    Status = fun (C) -> maps:get(C, Cs, absent) end,
    Absent = [C || C <- ?CONNS, Status(C) =:= absent],
    Connected = [C || C <- ?CONNS, Status(C) =:= connected],
    Disconnected = [C || C <- ?CONNS, Status(C) =:= disconnected],
    Down = [C || C <- ?CONNS, lists:member(Status(C), [disconnected, presumed_down])],
    %% only offer a command when it has an applicable connection; activate and
    %% evaluate are always available so the frequency list is never empty
    frequency(
      [{20, {call, ?MODULE, cmd_register, [elements(Absent)]}} || Absent =/= []] ++
      [{10, {call, ?MODULE, cmd_unregister, [elements(Connected)]}} || Connected =/= []] ++
      [{10, {call, ?MODULE, cmd_down, [elements(Connected),
                                       elements([normal, noconnection])]}} || Connected =/= []] ++
      [{8,  {call, ?MODULE, cmd_presume, [elements(Disconnected)]}} || Disconnected =/= []] ++
      [{8,  {call, ?MODULE, cmd_reconnect, [elements(Down)]}} || Down =/= []] ++
      [{8,  {call, ?MODULE, cmd_activate, []}},
       {6,  {call, ?MODULE, cmd_evaluate, []}}]).

precondition(#st{setup = Setup}, {call, _, cmd_setup, _}) ->
    not Setup;
precondition(#st{setup = false}, _) ->
    false;
precondition(#st{conns = Cs}, {call, _, cmd_register, [C]}) ->
    maps:get(C, Cs, absent) =:= absent;
precondition(#st{conns = Cs}, {call, _, cmd_unregister, [C]}) ->
    maps:get(C, Cs, absent) =:= connected;
precondition(#st{conns = Cs}, {call, _, cmd_down, [C, _]}) ->
    maps:get(C, Cs, absent) =:= connected;
precondition(#st{conns = Cs}, {call, _, cmd_presume, [C]}) ->
    maps:get(C, Cs, absent) =:= disconnected;
precondition(#st{conns = Cs}, {call, _, cmd_reconnect, [C]}) ->
    lists:member(maps:get(C, Cs, absent), [disconnected, presumed_down]);
precondition(#st{setup = true}, _) ->
    true.

%% Safety, checked after every command: at most one active consumer per group at
%% every reachable state. Liveness (convergence to exactly one active) is checked
%% once at the end of the run.
postcondition(_State, {call, _, _, _}, _Result) ->
    at_most_one_active(world()).

at_most_one_active(undefined) ->
    true;
at_most_one_active(World) ->
    length([C || #consumer{status = St} = C <- consumers(group_of(World)),
                 is_active(St)]) =< 1.

%% mirrors rabbit_stream_sac_coordinator:is_active/1
is_active({presumed_down, _}) -> false;
is_active({_, active}) -> true;
is_active({_, deactivating}) -> true;
is_active(_) -> false.

next_state(St, _Res, {call, _, cmd_setup, _}) ->
    St#st{setup = true};
next_state(#st{conns = Cs} = St, _Res, {call, _, cmd_register, [C]}) ->
    St#st{conns = Cs#{C => connected}};
next_state(#st{conns = Cs} = St, _Res, {call, _, cmd_unregister, [C]}) ->
    St#st{conns = maps:remove(C, Cs)};
next_state(#st{conns = Cs} = St, _Res, {call, _, cmd_down, [C, normal]}) ->
    St#st{conns = maps:remove(C, Cs)};
next_state(#st{conns = Cs} = St, _Res, {call, _, cmd_down, [C, noconnection]}) ->
    St#st{conns = Cs#{C => disconnected}};
next_state(#st{conns = Cs} = St, _Res, {call, _, cmd_presume, [C]}) ->
    St#st{conns = Cs#{C => presumed_down}};
next_state(#st{conns = Cs} = St, _Res, {call, _, cmd_reconnect, [C]}) ->
    St#st{conns = Cs#{C => connected}};
next_state(St, _Res, {call, _, _, _}) ->
    St.

%% -------------------------------------------------------------------
%% Commands: operate on the model world held in the process dictionary.
%% World = {Idx, State, PartitionIndex}.
%% -------------------------------------------------------------------

cmd_setup(PartitionIndex) ->
    put(world, {1, ?MOD:init_state(), PartitionIndex}),
    ok.

cmd_register(Conn) ->
    {_Idx, _State, PI} = world(),
    P = conn_pid(Conn),
    apply_world(#command_register_consumer{vhost = ?VH, stream = ?STREAM,
                                           partition_index = PI,
                                           consumer_name = ?NAME,
                                           connection_pid = P, owner = <<"o">>,
                                           subscription_id = 0}).

cmd_unregister(Conn) ->
    apply_world(#command_unregister_consumer{vhost = ?VH, stream = ?STREAM,
                                             consumer_name = ?NAME,
                                             connection_pid = conn_pid(Conn),
                                             subscription_id = 0}).

cmd_down(Conn, Reason) ->
    apply_world({down, conn_pid(Conn), Reason}).

cmd_presume(Conn) ->
    apply_world({presume, conn_pid(Conn)}).

cmd_reconnect(Conn) ->
    apply_world(#command_connection_reconnected{pid = conn_pid(Conn)}).

cmd_activate() ->
    apply_world(#command_activate_consumer{vhost = ?VH, stream = ?STREAM,
                                           consumer_name = ?NAME}).

cmd_evaluate() ->
    apply_world(#command_evaluate_group{vhost = ?VH, stream = ?STREAM,
                                        consumer_name = ?NAME, dead_pids = []}).

apply_world(Cmd) ->
    put(world, apply_cmd(world(), Cmd)),
    ok.

%% -------------------------------------------------------------------
%% The model world.
%% -------------------------------------------------------------------

world() -> get(world).

erase_state() ->
    _ = erase(world),
    %% forget memoised connection pids from the previous run
    [erase({pid, C}) || C <- ?CONNS],
    ok.

meta(Idx) ->
    #{index => Idx, system_time => Idx * 2, machine_version => 8, term => 1}.

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

%% A stable pid per connection name for the duration of a run, so a connection's
%% identity (and node) is consistent across the commands that reference it.
conn_pid(Name) ->
    case get({pid, Name}) of
        undefined -> P = fake_pid(Name), put({pid, Name}, P), P;
        P -> P
    end.

apply_cmd({Idx, State, PI}, {down, Pid, Reason}) ->
    {State1, _Eff} = ?MOD:handle_connection_down(meta(Idx), Pid, Reason, State),
    {Idx + 1, State1, PI};
apply_cmd({Idx, State, PI}, {presume, Pid}) ->
    {State1, _Eff} = ?MOD:presume_connection_down(meta(Idx), Pid, State),
    {Idx + 1, State1, PI};
apply_cmd({Idx, State, PI}, Cmd) ->
    {State1, _Reply, _Eff} = ?MOD:apply(Cmd, State),
    %% the real coordinator calls ensure_monitors/4 after apply to maintain
    %% pids_groups, which handle_connection_down/4 relies on
    State2 = ensure_pids_groups(Cmd, State1),
    {Idx + 1, State2, PI}.

ensure_pids_groups(#command_register_consumer{} = Cmd, State) ->
    {S, _M, _E} = ?MOD:ensure_monitors(Cmd, State, #{}, []),
    S;
ensure_pids_groups(#command_unregister_consumer{} = Cmd, State) ->
    {S, _M, _E} = ?MOD:ensure_monitors(Cmd, State, #{}, []),
    S;
ensure_pids_groups(_Cmd, State) ->
    State.

group_of({_Idx, State, _PI}) ->
    maps:get(?GID, State#rabbit_stream_sac_coordinator.groups, undefined).

consumers(undefined) -> [];
consumers(#group{consumers = Cs}) -> Cs.

%% -------------------------------------------------------------------
%% Convergence oracle.
%% -------------------------------------------------------------------

%% A non-empty group is converged iff it has exactly one {connected, active}
%% consumer.
converged(World) ->
    Cs = consumers(group_of(World)),
    case Cs of
        [] ->
            true;
        _ ->
            NumActive = length([C || #consumer{status = {connected, active}} = C <- Cs]),
            NumActive == 1
    end.

%% Cooperative drain: reconnect every disconnected/presumed-down consumer,
%% answer every `deactivating` step-down with an activate_consumer, and
%% evaluate, to a fixpoint. If even this cannot reach a single active consumer
%% the group is a genuine convergence dead-end.
drain(World) -> drain(World, 500).
drain(World, 0) -> {false, World};
drain(World, Fuel) ->
    Cs = consumers(group_of(World)),
    Disconnected = [C || #consumer{status = {S, _}} = C <- Cs,
                         S =:= disconnected orelse S =:= presumed_down],
    Deactivating = [C || #consumer{status = {_, deactivating}} = C <- Cs],
    case {Disconnected, Deactivating} of
        {[#consumer{pid = P} | _], _} ->
            drain(apply_cmd(World,
                            #command_connection_reconnected{pid = P}), Fuel - 1);
        {[], [_ | _]} ->
            drain(apply_cmd(World,
                            #command_activate_consumer{vhost = ?VH, stream = ?STREAM,
                                                       consumer_name = ?NAME}), Fuel - 1);
        {[], []} ->
            {true, apply_cmd(World,
                             #command_evaluate_group{vhost = ?VH, stream = ?STREAM,
                                                     consumer_name = ?NAME,
                                                     dead_pids = []})}
    end.

%% -------------------------------------------------------------------
%% Pretty-printing for counterexamples.
%% -------------------------------------------------------------------

format_commands(Commands) ->
    [io_lib:format("  ~p~n", [list_to_tuple([F | A])])
     || {set, _Var, {call, _M, F, A}} <- Commands].

format_group(undefined) ->
    "  <no group set up>\n";
format_group(World) ->
    case group_of(World) of
        undefined ->
            "  <group empty>\n";
        #group{consumers = Cs, partition_index = PI} ->
            [io_lib:format("  partition_index=~p~n", [PI]) |
             [io_lib:format("    ~p sub=~p ~p~n",
                            [node(C#consumer.pid), C#consumer.subscription_id,
                             C#consumer.status])
              || C <- Cs]]
    end.
