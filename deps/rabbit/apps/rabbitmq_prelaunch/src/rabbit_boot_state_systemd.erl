%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2015-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_state_systemd).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2,
         code_change/3]).

-define(LOG_PREFIX, "Boot state/systemd: ").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, _} = application:ensure_all_started(systemd),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({notify_boot_state, BootState}, State) ->
    _ = notify_boot_state(BootState),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private

notify_boot_state(ready = BootState) ->
    Status = boot_state_to_desc(BootState),
    ?LOG_DEBUG(
       ?LOG_PREFIX "notifying of state `~s`",
       [BootState],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    systemd:notify([BootState, {status, Status}]);
notify_boot_state(BootState) ->
    Status = boot_state_to_desc(BootState),
    ?LOG_DEBUG(
       ?LOG_PREFIX "sending non-systemd state (~s) as status description: "
       "\"~s\"",
       [BootState, Status],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    systemd:notify({status, Status}).

boot_state_to_desc(stopped) ->
    "Standing by";
boot_state_to_desc(booting) ->
    "Startup in progress";
boot_state_to_desc(core_started) ->
    "Startup in progress (core ready, starting plugins)";
boot_state_to_desc(ready) ->
    "";
boot_state_to_desc(stopping) ->
    "";
boot_state_to_desc(BootState) ->
    atom_to_list(BootState).
