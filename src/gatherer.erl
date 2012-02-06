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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(gatherer).

-behaviour(gen_server2).

-export([start_link/0, stop/1, fork/1, finish/1, in/2, out/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(stop/1 :: (pid()) -> 'ok').
-spec(fork/1 :: (pid()) -> 'ok').
-spec(finish/1 :: (pid()) -> 'ok').
-spec(in/2 :: (pid(), any()) -> 'ok').
-spec(out/1 :: (pid()) -> {'value', any()} | 'empty').

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

-record(gstate, { forks, values, blocked }).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link(?MODULE, [], [{timeout, infinity}]).

stop(Pid) ->
    gen_server2:call(Pid, stop, infinity).

fork(Pid) ->
    gen_server2:call(Pid, fork, infinity).

finish(Pid) ->
    gen_server2:cast(Pid, finish).

in(Pid, Value) ->
    gen_server2:cast(Pid, {in, Value}).

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

handle_call(out, From, State = #gstate { forks   = Forks,
                                         values  = Values,
                                         blocked = Blocked }) ->
    case queue:out(Values) of
        {empty, _} ->
            case Forks of
                0 -> {reply, empty, State, hibernate};
                _ -> {noreply,
                      State #gstate { blocked = queue:in(From, Blocked) },
                      hibernate}
            end;
        {{value, _Value} = V, NewValues} ->
            {reply, V, State #gstate { values = NewValues }, hibernate}
    end;

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(finish, State = #gstate { forks = Forks, blocked = Blocked }) ->
    NewForks = Forks - 1,
    NewBlocked = case NewForks of
                     0 -> [gen_server2:reply(From, empty) ||
                              From <- queue:to_list(Blocked)],
                          queue:new();
                     _ -> Blocked
                 end,
    {noreply, State #gstate { forks = NewForks, blocked = NewBlocked },
     hibernate};

handle_cast({in, Value}, State = #gstate { values  = Values,
                                           blocked = Blocked }) ->
    {noreply, case queue:out(Blocked) of
                  {empty, _} ->
                      State #gstate { values = queue:in(Value, Values) };
                  {{value, From}, NewBlocked} ->
                      gen_server2:reply(From, {value, Value}),
                      State #gstate { blocked = NewBlocked }
              end, hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.
