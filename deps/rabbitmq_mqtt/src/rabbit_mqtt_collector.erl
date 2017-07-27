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

-module(rabbit_mqtt_collector).

-behaviour(gen_server).

-export([start_link/0, register/2, unregister/2, list/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {client_ids}).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register(ClientId, Pid) ->
    gen_server:call(rabbit_mqtt_collector, {register, ClientId, Pid}, infinity).

unregister(ClientId, Pid) ->
    gen_server:call(rabbit_mqtt_collector, {unregister, ClientId, Pid}, infinity).

list() ->
    gen_server:call(rabbit_mqtt_collector, list).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{client_ids = dict:new()}}. % clientid -> {pid, monitor}

%%--------------------------------------------------------------------------

handle_call({register, ClientId, Pid}, _From,
            State = #state{client_ids = Ids}) ->
    Ids1 = case dict:find(ClientId, Ids) of
               {ok, {OldPid, MRef}} when Pid =/= OldPid ->
                   catch gen_server2:cast(OldPid, duplicate_id),
                   erlang:demonitor(MRef),
                   dict:erase(ClientId, Ids);
               error ->
                   Ids
           end,
    Ids2 = dict:store(ClientId, {Pid, erlang:monitor(process, Pid)}, Ids1),
    {reply, ok, State#state{client_ids = Ids2}};

handle_call({unregister, ClientId, Pid}, _From, State = #state{client_ids = Ids}) ->
    {Reply, Ids1} = case dict:find(ClientId, Ids) of
                        {ok, {Pid, MRef}} -> erlang:demonitor(MRef),
                                             {ok, dict:erase(ClientId, Ids)};
                        _                 -> {ok, Ids}
                    end,
    {reply, Reply, State#state{ client_ids = Ids1 }};

handle_call(list, _From, State = #state{client_ids = Ids}) ->
    {reply, dict:to_list(Ids), State};

handle_call(Msg, _From, State) ->
    {stop, {unhandled_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({'EXIT', _, {shutdown, closed}}, State) ->
    {stop, {shutdown, closed}, State};

handle_info({'DOWN', MRef, process, DownPid, _Reason},
            State = #state{client_ids = Ids}) ->
    Ids1 = dict:filter(fun (ClientId, {Pid, M})
                           when Pid =:= DownPid, MRef =:= M ->
                               rabbit_log:log(connection, warning,
                                              "MQTT disconnect from ~p~n",
                                              [ClientId]),
                               false;
                           (_, _) ->
                               true
                       end, Ids),
    {noreply, State #state{ client_ids = Ids1 }}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
