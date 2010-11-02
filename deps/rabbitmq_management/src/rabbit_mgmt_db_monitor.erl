%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_db_monitor).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%----------------------------------------------------------------------------

acquire_monitor() ->
    case global:whereis_name(rabbit_mgmt_db) of
        undefined ->
            timer:sleep(1000),
            acquire_monitor();
        Pid ->
            erlang:monitor(process, Pid)
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, acquire_monitor()}.

handle_call(_Request, _From, State) ->
    {reply, not_understood, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

%% We're mainly interested in the noconnection reason, since otherwise
%% the supervisor on the remote node will restart the process
handle_info({'DOWN', _MonitorRef, _Type, _Object, noconnection}, _State) ->
    rabbit_log:info("Statistics database node down.~n", []),
    ok = rabbit_sup:stop_child(rabbit_mgmt_global_sup),
    ok = rabbit_sup:start_child(rabbit_mgmt_global_sup),
    {noreply, acquire_monitor()};

handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, _State) ->
    rabbit_log:info(
      "Statistics database down, reacquiring monitor.~n", []),
    {noreply, acquire_monitor()};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
