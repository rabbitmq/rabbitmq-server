%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_control_misc).

-export([emitting_map/4, emitting_map/5, emitting_map_with_exit_handler/4,
         emitting_map_with_exit_handler/5, wait_for_info_messages/6,
         spawn_emitter_caller/7, await_emitters_termination/1,
         print_cmd_result/2]).

-spec emitting_map(pid(), reference(), fun(), list()) -> 'ok'.
-spec emitting_map(pid(), reference(), fun(), list(), atom()) -> 'ok'.
-spec emitting_map_with_exit_handler
        (pid(), reference(), fun(), list()) -> 'ok'.
-spec emitting_map_with_exit_handler
        (pid(), reference(), fun(), list(), 'continue') -> 'ok'.

-type fold_fun() :: fun((Item :: term(), AccIn :: term()) -> AccOut :: term()).

-spec wait_for_info_messages(pid(), reference(), fold_fun(), InitialAcc, timeout(), non_neg_integer()) -> OK | Err when
      InitialAcc :: term(), Acc :: term(), OK :: {ok, Acc}, Err :: {error, term()}.
-spec spawn_emitter_caller(node(), module(), atom(), [term()], reference(), pid(), timeout()) -> 'ok'.
-spec await_emitters_termination([pid()]) -> 'ok'.

-spec print_cmd_result(atom(), term()) -> 'ok'.

emitting_map(AggregatorPid, Ref, Fun, List) ->
    emitting_map(AggregatorPid, Ref, Fun, List, continue),
    AggregatorPid ! {Ref, finished},
    ok.

emitting_map(AggregatorPid, Ref, Fun, List, continue) ->
    _ = emitting_map0(AggregatorPid, Ref, Fun, List, fun step/4),
    ok.

emitting_map_with_exit_handler(AggregatorPid, Ref, Fun, List) ->
    emitting_map_with_exit_handler(AggregatorPid, Ref, Fun, List, continue),
    AggregatorPid ! {Ref, finished},
    ok.

emitting_map_with_exit_handler(AggregatorPid, Ref, Fun, List, continue) ->
    _ = emitting_map0(AggregatorPid, Ref, Fun, List, fun step_with_exit_handler/4),
    ok.

emitting_map0(AggregatorPid, Ref, Fun, List, StepFun) ->
    [StepFun(AggregatorPid, Ref, Fun, Item) || Item <- List].

step(AggregatorPid, Ref, Fun, Item) ->
    AggregatorPid ! {Ref, Fun(Item), continue},
    ok.

step_with_exit_handler(AggregatorPid, Ref, Fun, Item) ->
    Noop = make_ref(),
    case rabbit_misc:with_exit_handler(
           fun () -> Noop end,
           fun () -> Fun(Item) end) of
        Noop ->
            ok;
        Res  ->
            AggregatorPid ! {Ref, Res, continue},
            ok
    end.

%% Invokes RPC for async info collection in separate (but linked to
%% the caller) process. Separate process waits for RPC to finish and
%% in case of errors sends them in wait_for_info_messages/5-compatible
%% form to aggregator process. Calling process is then expected to
%% do blocking call of wait_for_info_messages/5.
%%
%% Remote function MUST use calls to emitting_map/4 (and other
%% emitting_map's) to properly deliver requested information to an
%% aggregator process.
%%
%% If for performance reasons several parallel emitting_map's need to
%% be run, remote function MUST NOT return until all this
%% emitting_map's are done. And during all this time remote RPC
%% process MUST be linked to emitting
%% processes. await_emitters_termination/1 helper can be used as a
%% last statement of remote function to ensure this behaviour.
spawn_emitter_caller(Node, Mod, Fun, Args, Ref, Pid, Timeout) ->
    _ = spawn_monitor(
          fun () ->
                  case rpc_call_emitter(Node, Mod, Fun, Args, Ref, Pid, Timeout) of
                      {error, _} = Error        ->
                          Pid ! {Ref, error, Error};
                      {bad_argument, _} = Error ->
                          Pid ! {Ref, error, Error};
                      {badrpc, _} = Error       ->
                          Pid ! {Ref, error, Error};
                      _                         ->
                          ok
                  end
          end),
    ok.

rpc_call_emitter(Node, Mod, Fun, Args, Ref, Pid, Timeout) ->
    rabbit_misc:rpc_call(Node, Mod, Fun, Args++[Ref, Pid], Timeout).

%% Aggregator process expects correct numbers of explicits ACKs about
%% finished emission process. While everything is linked, we still
%% need somehow to wait for termination of all emitters before
%% returning from RPC call - otherwise links will be just broken with
%% reason 'normal' and we can miss some errors, and subsequently
%% hang.
await_emitters_termination(Pids) ->
    Monitors = [erlang:monitor(process, Pid) || Pid <- Pids],
    collect_monitors(Monitors).

collect_monitors([]) ->
    ok;
collect_monitors([Monitor|Rest]) ->
    receive
        {'DOWN', Monitor, process, _Pid, normal} ->
            collect_monitors(Rest);
        {'DOWN', Monitor, process, _Pid, noproc} ->
            %% There is a link and a monitor to a process. Matching
            %% this clause means that process has gracefully
            %% terminated even before we've started monitoring.
            collect_monitors(Rest);
        {'DOWN', _, process, Pid, Reason} when Reason =/= normal,
                                               Reason =/= noproc ->
            exit({emitter_exit, Pid, Reason})
    end.

%% Wait for result of one or more calls to emitting_map-family
%% functions.
%%
%% Number of expected acknowledgments is specified by ChunkCount
%% argument. Most common usage will be with ChunkCount equals to
%% number of live nodes, but it's not mandatory - thus more generic
%% name of 'ChunkCount' was chosen.
wait_for_info_messages(Pid, Ref, Fun, Acc0, Timeout, ChunkCount) ->
    _ = notify_if_timeout(Pid, Ref, Timeout),
    wait_for_info_messages(Ref, Fun, Acc0, ChunkCount).

wait_for_info_messages(Ref, Fun, Acc0, ChunksLeft) ->
    receive
        {Ref, finished} when ChunksLeft =:= 1 ->
            {ok, Acc0};
        {Ref, finished} ->
            wait_for_info_messages(Ref, Fun, Acc0, ChunksLeft - 1);
        {Ref, {timeout, T}} ->
            exit({error, {timeout, (T / 1000)}});
        {Ref, []} ->
            wait_for_info_messages(Ref, Fun, Acc0, ChunksLeft);
        {Ref, Result, continue} ->
            wait_for_info_messages(Ref, Fun, Fun(Result, Acc0), ChunksLeft);
        {Ref, error, Error} ->
            {error, simplify_emission_error(Error)};
        {'DOWN', _MRef, process, _Pid, normal} ->
            wait_for_info_messages(Ref, Fun, Acc0, ChunksLeft);
        {'DOWN', _MRef, process, _Pid, Reason} ->
            {error, simplify_emission_error(Reason)};
        _Msg ->
            wait_for_info_messages(Ref, Fun, Acc0, ChunksLeft)
    end.

simplify_emission_error({badrpc, {'EXIT', {{nocatch, EmissionError}, _Stacktrace}}}) ->
    EmissionError;
simplify_emission_error({{nocatch, EmissionError}, _Stacktrace}) ->
    EmissionError;
simplify_emission_error({error, _} = Error) ->
    Error;
simplify_emission_error({bad_argument, _} = Error) ->
    Error;
simplify_emission_error(Anything) ->
    {error, Anything}.

notify_if_timeout(_, _, infinity) ->
    ok;
notify_if_timeout(Pid, Ref, Timeout) ->
    erlang:send_after(Timeout, Pid, {Ref, {timeout, Timeout}}).

print_cmd_result(authenticate_user, _Result) -> io:format("Success~n");
print_cmd_result(join_cluster, already_member) -> io:format("The node is already a member of this cluster~n").
