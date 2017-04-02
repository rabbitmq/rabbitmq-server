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

-module(rabbit_auth_cache_dict).
-behaviour(gen_server2).
-compile({no_auto_import,[get/1]}).
-compile({no_auto_import,[put/2]}).

-behaviour(rabbit_auth_cache).

-export([start_link/0,
         get/1, put/3, delete/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() -> gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) -> gen_server2:call(?MODULE, {get, Key}).
put(Key, Value, TTL) -> gen_server2:cast(?MODULE, {put, Key, Value, TTL}).
delete(Key) -> gen_server2:call(?MODULE, {delete, Key}).

init(_Args) -> {ok, nostate}.

handle_call({get, Key}, _From, nostate) ->
    Result = case erlang:get({items, Key}) of
        undefined -> {error, not_found};
        Val       -> {ok, Val}
    end,
    {reply, Result, nostate};
handle_call({delete, Key}, _From, nostate) ->
    do_delete(Key),
    {reply, ok, nostate}.

handle_cast({put, Key, Value, TTL}, nostate) ->
    erlang:put({items, Key}, Value),
    {ok, TRef} = timer:apply_after(TTL, rabbit_auth_cache_dict, delete, [Key]),
    erlang:put({timers, Key}, TRef),
    {noreply, nostate}.

handle_info(_Msg, nostate) ->
    {noreply, nostate}.

code_change(_OldVsn, nostate, _Extra) ->
    {ok, nostate}.

terminate(_Reason, nostate) ->
    nostate.

do_delete(Key) ->
    erase({items, Key}),
    case erlang:get({timers, Key}) of
        undefined -> ok;
        Tref      -> timer:cancel(Tref),
                     erase({timers, Key})
        
    end.
