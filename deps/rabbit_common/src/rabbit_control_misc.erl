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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_control_misc).

-export([emitting_map/4, emitting_map/5, emitting_map_with_wrapper_fun/5,
         emitting_map_with_wrapper_fun/6, wait_for_info_messages/5]).

-ifdef(use_specs).

-spec(emitting_map/4 :: (pid(), reference(), fun(), list()) -> 'ok').
-spec(emitting_map/5 :: (pid(), reference(), fun(), list(), atom()) -> 'ok').

-endif.

emitting_map(AggregatorPid, Ref, Fun, List) ->
    emitting_map(AggregatorPid, Ref, Fun, List, finished).
emitting_map(AggregatorPid, Ref, Fun, List, continue) ->
    [AggregatorPid ! {Ref, Fun(Item), continue} || Item <- List];
emitting_map(AggregatorPid, Ref, Fun, List, finished) ->
    [AggregatorPid ! {Ref, Fun(Item)} || Item <- List],
    AggregatorPid ! {Ref, finished},
    ok.

emitting_map_with_wrapper_fun(AggregatorPid, Ref, Fun, WrapperFun, List) ->
    emitting_map_with_wrapper_fun(AggregatorPid, Ref, Fun, WrapperFun, List,
                              finished).
emitting_map_with_wrapper_fun(AggregatorPid, Ref, Fun, WrapperFun, List,
                              continue) ->
    WrapperFun(fun(Item) -> AggregatorPid ! {Ref, Fun(Item), continue} end,
               List);
emitting_map_with_wrapper_fun(AggregatorPid, Ref, Fun, WrapperFun, List,
                          finished) ->
    WrapperFun(fun(Item) -> AggregatorPid ! {Ref, Fun(Item)} end, List),
    AggregatorPid ! {Ref, finished},
    ok.

wait_for_info_messages(Pid, Ref, ArgAtoms, DisplayFun, Timeout) ->
    notify_if_timeout(Pid, Ref, Timeout),
    wait_for_info_messages(Ref, ArgAtoms, DisplayFun).

wait_for_info_messages(Ref, InfoItemKeys, DisplayFun) when is_reference(Ref) ->
    receive
        {Ref,  finished}     -> ok;
        {Ref,  {timeout, T}} -> exit({error, {timeout, (T / 1000)}});
        {Ref,  []}           -> wait_for_info_messages(Ref, InfoItemKeys, DisplayFun);
        {Ref,  Result}       -> DisplayFun(Result, InfoItemKeys),
                                wait_for_info_messages(Ref, InfoItemKeys, DisplayFun);
        {Ref,  Result, continue} ->
            DisplayFun(Result, InfoItemKeys),
            wait_for_info_messages(Ref, InfoItemKeys, DisplayFun);
        _                        ->
            wait_for_info_messages(Ref, InfoItemKeys, DisplayFun)
    end.

notify_if_timeout(Pid, Ref, Timeout) ->
    timer:send_after(Timeout, Pid, {Ref, {timeout, Timeout}}).
