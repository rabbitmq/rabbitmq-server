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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_cache_dict).
-behaviour(gen_server2).
-compile({no_auto_import,[get/1]}).
-compile({no_auto_import,[put/2]}).

-export([start_link/0,
         get/1, put/2, delete/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {ttl}).

start_link() -> gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) -> gen_server2:call(?MODULE, {get, Key}).
put(Key, Value) -> gen_server2:cast(?MODULE, {put, Key, Value}).
delete(Key) -> gen_server2:call(?MODULE, {delete, Key}).

init(_Args) ->
    {ok, TTL}  = application:get_env(rabbitmq_auth_backend_cache, cache_ttl),
    {ok, #state{ttl = TTL}}.

handle_call({get, Key}, _From, State = #state{}) ->
    Result = case erlang:get({item, Key}) of
        {ok, Val} -> {ok, Val};
        error     -> {error, not_found}
    end,
    {reply, Result, State};
handle_call({delete, Key}, _From, State = #state{}) ->
    do_delete(Key),
    {reply, ok, State}.

handle_cast({put, Key, Value}, State = #state{ttl = TTL}) ->
    erlang:put({items, Key}, Value),
    {ok, TRef} = timer:apply_after(TTL, auth_cache_dict, delete, Key),
    put({timers, Key}, TRef),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #state{}) ->
    State.

do_delete(Key) ->
    erase({items, Key}),
    case get({timers, Key}) of
        {ok, Tref} -> erlang:cancel_timer(Tref),
                      erase({timers, Key});
        error      -> ok
    end.
