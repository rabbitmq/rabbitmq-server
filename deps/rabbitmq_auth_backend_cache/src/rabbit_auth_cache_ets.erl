%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_cache_ets).
-behaviour(gen_server).
-compile({no_auto_import,[get/1]}).
-compile({no_auto_import,[put/2]}).

-include("rabbit_auth_backend_cache.hrl").

-behaviour(rabbit_auth_cache).

-export([start_link/0,
         get/1, put/3, delete/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    cache,
    timers,
    ttl
}).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) -> gen_server:call(?MODULE, {get, Key}, ?CACHE_OPERATION_TIMEOUT).

put(Key, Value, TTL) ->
    Expiration = rabbit_auth_cache:expiration(TTL),
    gen_server:cast(?MODULE, {put, Key, Value, TTL, Expiration}).

delete(Key) -> gen_server:call(?MODULE, {delete, Key}, ?CACHE_OPERATION_TIMEOUT).

init([]) ->
    {ok, #state{cache = ets:new(?MODULE, [set, private]),
                timers = ets:new(auth_cache_ets_timers, [set, private])}}.

handle_call({get, Key}, _From, State = #state{cache = Table}) ->
    Result = case ets:lookup(Table, Key) of
        [{Key, {Exp, Val}}] -> case rabbit_auth_cache:expired(Exp) of
                                   true  -> {error, not_found};
                                   false -> {ok, Val}
                               end;
        []                  -> {error, not_found}
    end,
    {reply, Result, State};
handle_call({delete, Key}, _From, State = #state{cache = Table, timers = Timers}) ->
    do_delete(Key, Table, Timers),
    {reply, ok, State}.

handle_cast({put, Key, Value, TTL, Expiration},
            State = #state{cache = Table, timers = Timers}) ->
    do_delete(Key, Table, Timers),
    ets:insert(Table, {Key, {Expiration, Value}}),
    {ok, TRef} = timer:apply_after(TTL, rabbit_auth_cache_ets, delete, [Key]),
    ets:insert(Timers, {Key, TRef}),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #state{}) ->
    State.

do_delete(Key, Table, Timers) ->
    true = ets:delete(Table, Key),
    case ets:lookup(Timers, Key) of
        [{Key, Tref}] -> timer:cancel(Tref),
                         true = ets:delete(Timers, Key);
        []            -> ok
    end.
