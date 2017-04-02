%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_control_misc).

-export([emitting_map/4, emitting_map/5, emitting_map_with_exit_handler/4,
         emitting_map_with_exit_handler/5, wait_for_info_messages/5,
         print_cmd_result/2]).

-spec emitting_map(pid(), reference(), fun(), list()) -> 'ok'.
-spec emitting_map(pid(), reference(), fun(), list(), atom()) -> 'ok'.
-spec emitting_map_with_exit_handler
        (pid(), reference(), fun(), list()) -> 'ok'.
-spec emitting_map_with_exit_handler
        (pid(), reference(), fun(), list(), atom()) -> 'ok'.
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

wait_for_info_messages(Pid, Ref, ArgAtoms, DisplayFun, Timeout) ->
    _ = notify_if_timeout(Pid, Ref, Timeout),
    wait_for_info_messages(Ref, ArgAtoms, DisplayFun).

wait_for_info_messages(Ref, InfoItemKeys, DisplayFun) when is_reference(Ref) ->
    receive
        {Ref,  finished}         ->
            ok;
        {Ref,  {timeout, T}}     ->
            exit({error, {timeout, (T / 1000)}});
        {Ref,  []}               ->
            wait_for_info_messages(Ref, InfoItemKeys, DisplayFun);
        {Ref,  Result, continue} ->
            DisplayFun(Result, InfoItemKeys),
            wait_for_info_messages(Ref, InfoItemKeys, DisplayFun);
        {error, Error}           ->
            Error;
        _                        ->
            wait_for_info_messages(Ref, InfoItemKeys, DisplayFun)
    end.

notify_if_timeout(Pid, Ref, Timeout) ->
    timer:send_after(Timeout, Pid, {Ref, {timeout, Timeout}}).

print_cmd_result(authenticate_user, _Result) -> io:format("Success~n");
print_cmd_result(join_cluster, already_member) -> io:format("The node is already a member of this cluster~n").
