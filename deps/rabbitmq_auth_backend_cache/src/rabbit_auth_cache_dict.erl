%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_cache_dict).
-behaviour(gen_server).
-compile({no_auto_import,[get/1]}).
-compile({no_auto_import,[put/2]}).

-behaviour(rabbit_auth_cache).

-include("rabbit_auth_backend_cache.hrl").

-export([start_link/0,
         get/1, put/3, delete/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) -> gen_server:call(?MODULE, {get, Key}, ?CACHE_OPERATION_TIMEOUT).

put(Key, Value, TTL) -> gen_server:cast(?MODULE, {put, Key, Value, TTL}).

delete(Key) -> gen_server:call(?MODULE, {delete, Key}, ?CACHE_OPERATION_TIMEOUT).

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
