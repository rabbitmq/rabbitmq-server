%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(gatherer).

%% Gatherer is a queue which has producer and consumer processes. Before producers
%% push items to the queue using gatherer:in/2 they need to declare their intent
%% to do so with gatherer:fork/1. When a publisher's work is done, it states so
%% using gatherer:finish/1.
%%
%% Consumers pop messages off queues with gatherer:out/1. If a queue is empty
%% and there are producers that haven't finished working, the caller is blocked
%% until an item is available. If there are no active producers, gatherer:out/1
%% immediately returns 'empty'.
%%
%% This module is primarily used to collect results from asynchronous tasks
%% running in a worker pool, e.g. when recovering bindings or rebuilding
%% message store indices.

-behaviour(gen_server2).

-export([start_link/0, stop/1, fork/1, finish/1, in/2, sync_in/2, out/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

-record(gstate, { forks, values, blocked }).

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_server2:start_link(?MODULE, [], [{timeout, infinity}]).

-spec stop(pid()) -> 'ok'.

stop(Pid) ->
    unlink(Pid),
    gen_server2:call(Pid, stop, infinity).

-spec fork(pid()) -> 'ok'.

fork(Pid) ->
    gen_server2:call(Pid, fork, infinity).

-spec finish(pid()) -> 'ok'.

finish(Pid) ->
    gen_server2:cast(Pid, finish).

-spec in(pid(), any()) -> 'ok'.

in(Pid, Value) ->
    gen_server2:cast(Pid, {in, Value}).

-spec sync_in(pid(), any()) -> 'ok'.

sync_in(Pid, Value) ->
    gen_server2:call(Pid, {in, Value}, infinity).

-spec out(pid()) -> {'value', any()} | 'empty'.

out(Pid) ->
    gen_server2:call(Pid, out, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #gstate { forks = 0, values = queue:new(), blocked = queue:new() },
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(fork, _From, State = #gstate { forks = Forks }) ->
    {reply, ok, State #gstate { forks = Forks + 1 }, hibernate};

handle_call({in, Value}, From, State) ->
    {noreply, in(Value, From, State), hibernate};

handle_call(out, From, State = #gstate { forks   = Forks,
                                         values  = Values,
                                         blocked = Blocked }) ->
    case queue:out(Values) of
        {empty, _} when Forks == 0 ->
            {reply, empty, State, hibernate};
        {empty, _} ->
            {noreply, State #gstate { blocked = queue:in(From, Blocked) },
             hibernate};
        {{value, {PendingIn, Value}}, NewValues} ->
            reply(PendingIn, ok),
            {reply, {value, Value}, State #gstate { values = NewValues },
             hibernate}
    end;

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(finish, State = #gstate { forks = Forks, blocked = Blocked }) ->
    NewForks = Forks - 1,
    NewBlocked = case NewForks of
                     0 -> _ = [gen_server2:reply(From, empty) ||
                                  From <- queue:to_list(Blocked)],
                          queue:new();
                     _ -> Blocked
                 end,
    {noreply, State #gstate { forks = NewForks, blocked = NewBlocked },
     hibernate};

handle_cast({in, Value}, State) ->
    {noreply, in(Value, undefined, State), hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.

%%----------------------------------------------------------------------------

in(Value, From,  State = #gstate { values = Values, blocked = Blocked }) ->
    case queue:out(Blocked) of
        {empty, _} ->
            State #gstate { values = queue:in({From, Value}, Values) };
        {{value, PendingOut}, NewBlocked} ->
            reply(From, ok),
            gen_server2:reply(PendingOut, {value, Value}),
            State #gstate { blocked = NewBlocked }
    end.

reply(undefined, _Reply) -> ok;
reply(From,       Reply) -> gen_server2:reply(From, Reply).
