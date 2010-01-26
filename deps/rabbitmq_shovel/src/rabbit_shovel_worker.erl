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
%%   The Original Code is RabbitMQ-shovel.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_shovel_worker).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%---------------------------
% Gen Server Implementation
% --------------------------

init([]) ->
    io:format("~p alive!~n", [self()]),
    gen_server:cast(self(), die_soon),
    {ok, ok}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(die_soon, State) ->
    Sleep = 2000,
    io:format("~p dying in ~p~n", [self(), Sleep]),
    timer:sleep(Sleep),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    io:format("~p terminating with reason ~p~n", [self(), Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
