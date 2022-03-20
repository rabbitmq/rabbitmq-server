%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mnesia_sync).

%% mnesia:sync_transaction/3 fails to guarantee that the log is flushed to disk
%% at commit. This module is an attempt to minimise the risk of data loss by
%% performing a coalesced log fsync. Unfortunately this is performed regardless
%% of whether or not the log was appended to.

-behaviour(gen_server).

-export([sync/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {waiting, disc_node}).

%%----------------------------------------------------------------------------

-spec sync() -> 'ok'.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

sync() ->
    gen_server:call(?SERVER, sync, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{disc_node = mnesia:system_info(use_dir), waiting = []}}.

handle_call(sync, _From, #state{disc_node = false} = State) ->
    {reply, ok, State};
handle_call(sync, From, #state{waiting = Waiting} = State) ->
    {noreply, State#state{waiting = [From | Waiting]}, 0};
handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

handle_info(timeout, #state{waiting = Waiting} = State) ->
    ok = disk_log:sync(latest_log),
    _ = [gen_server:reply(From, ok) || From <- Waiting],
    {noreply, State#state{waiting = []}};
handle_info(Message, State) ->
    {stop, {unhandled_info, Message}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
