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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_stats).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_event).

-export([start/0]).

-export([get_queue_stats/1]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queue_stats}).

%%----------------------------------------------------------------------------

start() ->
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

get_queue_stats(QPids) ->
    gen_event:call(rabbit_event, ?MODULE, {get_queue_stats, QPids}, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{queue_stats = ets:new(anon, [private])}}.

handle_call({get_queue_stats, QPids}, State = #state{queue_stats = Table}) ->
    {ok, [ets:lookup_element(Table, QPid, 2) || QPid <- QPids], State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(#event{type = queue_stats, props = Stats},
             State = #state{queue_stats = Table}) ->
    ets:insert(Table, {proplists:get_value(qpid, Stats), Stats}),
    {ok, State};

handle_event(Event, State) ->
    io:format("Got event ~p~n", [Event]),
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
