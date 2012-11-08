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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_app_marker).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

%% We want to know when another node has *started* shutting down (to
%% write the cluster status file). The rabbit application goes away
%% pretty much when we have *finished* shutting down. So we have this
%% process to monitor instead - it;s the last thing to be started so
%% the first thing to go.

start_link() ->
    gen_server:start_link({local, rabbit_running}, ?MODULE, [], []).

%%----------------------------------------------------------------------------

init([]) -> {ok, state, hibernate}.

handle_call(_Msg, _From, State) -> {stop, not_understood, State}.
handle_cast(_Msg, State)        -> {stop, not_understood, State}.
handle_info(_Msg, State)        -> {stop, not_understood, State}.

terminate(_Arg, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
