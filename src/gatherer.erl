%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(gatherer).

-behaviour(gen_server2).

-export([start_link/0, stop/1, fork/1, finish/1, in/2, out/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-record(gstate, { forks, values, blocked }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | {'error', any()}).
-spec(stop/1 :: (pid()) -> 'ok').
-spec(fork/1 :: (pid()) -> 'ok').
-spec(finish/1 :: (pid()) -> 'ok').
-spec(in/2 :: (pid(), any()) -> 'ok').
-spec(out/1 :: (pid()) -> {'value', any()} | 'empty').

-spec(code_change/3 :: (_,_,_) -> {'ok',_}).
-spec(handle_call/3 :: (_,_,_) ->
			    {'noreply',
			     #gstate{values::queue(),blocked::queue()},
			     'hibernate'} |
			    {'stop',{'unexpected_call',_},_} |
			    {'reply',
			     'empty' | 'ok' | {'value',_},
			     #gstate{},'hibernate'} |
			    {'stop','normal','ok',_}).
-spec(handle_cast/2 :: (_,_) ->
			    {'noreply',#gstate{},'hibernate'} |
			    {'stop',{'unexpected_cast',_},_}).
-spec(handle_info/2 :: (_,_) -> {'stop',{'unexpected_info',_},_}).
-spec(init/1 :: ([]) ->
		     {'ok',
		      #gstate{forks::0,values::queue(),blocked::queue()},
		      'hibernate',{'backoff',1000,1000,10000}}).
-spec(terminate/2 :: (_,_) -> any()).

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

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
