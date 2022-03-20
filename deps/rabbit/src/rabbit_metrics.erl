%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_metrics).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------
%% Starts the raw metrics storage and owns the ETS tables.
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    rabbit_core_metrics:init(),
    {ok, none}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
