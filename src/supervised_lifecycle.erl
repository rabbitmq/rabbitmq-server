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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
