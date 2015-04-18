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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_retainer).

-behaviour(gen_server2).
-include("rabbit_mqtt.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, start_link/2]).

-export([retain/3, fetch/2, clear/2, store_module/0]).

-define(SERVER, ?MODULE).

-record(retainer_state, {store_mod,
                        store}).

%%----------------------------------------------------------------------------

start_link(RetainStoreMod, VHost) ->
    gen_server2:start_link(?MODULE, [RetainStoreMod, VHost], []).

retain(Pid, Topic, Msg) ->
    gen_server2:cast(Pid, {retain, Topic, Msg}).

fetch(Pid, Topic) ->
    gen_server2:call(Pid, {fetch, Topic}).

clear(Pid, Topic) ->
    gen_server2:cast(Pid, {clear, Topic}).

%%----------------------------------------------------------------------------

init([RetainStoreMod, VHost]) ->
    {ok, #retainer_state{store     = RetainStoreMod:new(store_dir(), VHost),
                store_mod = RetainStoreMod}}.

store_module() ->
    case application:get_env(rabbitmq_mqtt, retained_message_store) of
        {ok, Mod} -> Mod;
        undefined -> undefined
    end.

%%----------------------------------------------------------------------------

handle_cast({retain, Topic, Msg},
    State = #retainer_state{store = Store, store_mod = Mod}) ->
    ok = Mod:insert(Topic, Msg, Store),
    {noreply, State};
handle_cast({clear, Topic},
    State = #retainer_state{store = Store, store_mod = Mod}) ->
    ok = Mod:delete(Topic, Store),
    {noreply, State}.

handle_call({fetch, Topic}, _From,
    State = #retainer_state{store = Store, store_mod = Mod}) ->
    #retained_message{mqtt_msg = Msg} = Mod:lookup(Topic, Store),
    {reply, Msg, State}.

handle_info(stop, State) ->
    {stop, normal, State};

handle_info(Info, State) ->
    {stop, {unknown_info, Info}, State}.

store_dir() ->
    rabbit_mnesia:dir().

terminate(_Reason, _State) ->
    %% TODO: notify the store
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
