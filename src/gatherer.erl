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

-export([start_link/0, wait_on/2, produce/2, finished/2, fetch/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(wait_on/2 :: (pid(), any()) -> 'ok').
-spec(produce/2 :: (pid(), any()) -> 'ok').
-spec(finished/2 :: (pid(), any()) -> 'ok').
-spec(fetch/1 :: (pid()) -> {'value', any()} | 'finished').

-endif.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

%%----------------------------------------------------------------------------

-record(gstate, { waiting_on, results, blocking }).

%%----------------------------------------------------------------------------

wait_on(Pid, Token) ->
    gen_server2:call(Pid, {wait_on, Token}, infinity).

produce(Pid, Result) ->
    gen_server2:cast(Pid, {produce, Result}).

finished(Pid, Token) ->
    gen_server2:call(Pid, {finished, Token}, infinity).

fetch(Pid) ->
    gen_server2:call(Pid, fetch, infinity).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link(?MODULE, [], [{timeout, infinity}]).

init([]) ->
    {ok, #gstate { waiting_on = sets:new(), results = queue:new(),
                   blocking = queue:new() }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({wait_on, Token}, _From, State = #gstate { waiting_on = Tokens }) ->
    {reply, ok, State #gstate { waiting_on = sets:add_element(Token, Tokens) },
     hibernate};

handle_call({finished, Token}, _From,
            State = #gstate { waiting_on = Tokens, results = Results,
                              blocking = Blocking }) ->
    Tokens1 = sets:del_element(Token, Tokens),
    State1 = State #gstate { waiting_on = Tokens1 },
    case 0 =:= sets:size(Tokens1) andalso queue:is_empty(Results) andalso
        not queue:is_empty(Blocking) of
        true  -> {stop, normal, ok, State1};
        false -> {reply, ok, State1, hibernate}
    end;

handle_call(fetch, From,
            State = #gstate { waiting_on = Tokens, results = Results,
                              blocking = Blocking }) ->
    case queue:out(Results) of
        {empty, _Results} ->
            case sets:size(Tokens) of
                0 -> {stop, normal, finished, State};
                _ -> {noreply,
                      State #gstate { blocking = queue:in(From, Blocking) },
                      hibernate}
            end;
        {{value, Result}, Results1} ->
            {reply, {value, Result}, State #gstate { results = Results1 },
             hibernate}
    end;

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({produce, Result},
            State = #gstate { blocking = Blocking, results = Results }) ->
    {noreply, case queue:out(Blocking) of
                  {empty, _Blocking} ->
                      State #gstate { results = queue:in(Result, Results) };
                  {{value, Blocked}, Blocking1} ->
                      gen_server2:reply(Blocked, {value, Result}),
                      State #gstate { blocking = Blocking1 }
              end, hibernate};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #gstate { blocking = Blocking } ) ->
    [gen_server2:reply(Blocked, finished) ||
        Blocked <- queue:to_list(Blocking)],
    State.
