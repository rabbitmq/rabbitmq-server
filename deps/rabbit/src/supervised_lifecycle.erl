%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Invoke callbacks on startup and termination.
%%
%% Simply hook this process into a supervision hierarchy, to have the
%% callbacks invoked at a precise point during the establishment and
%% teardown of that hierarchy, respectively.
%%
%% Or launch the process independently, and link to it, to have the
%% callbacks invoked on startup and when the linked process
%% terminates, respectively.

-module(supervised_lifecycle).

-behavior(gen_server).

-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%----------------------------------------------------------------------------

-spec start_link(atom(), rabbit_types:mfargs(), rabbit_types:mfargs()) ->
          rabbit_types:ok_pid_or_error().

start_link(Name, StartMFA, StopMFA) ->
    gen_server:start_link({local, Name}, ?MODULE, [StartMFA, StopMFA], []).

%%----------------------------------------------------------------------------

init([{M, F, A}, StopMFA]) ->
    process_flag(trap_exit, true),
    apply(M, F, A),
    {ok, StopMFA}.

handle_call(_Request, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, {M, F, A}) ->
    apply(M, F, A),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
