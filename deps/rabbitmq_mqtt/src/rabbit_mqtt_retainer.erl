%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retainer).

-behaviour(gen_server2).
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_frame.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, start_link/2]).

-export([retain/3, fetch/2, clear/2, store_module/0]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 30000).

-record(retainer_state, {store_mod,
                         store}).

-spec retain(pid(), string(), mqtt_msg()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.

%%----------------------------------------------------------------------------

start_link(RetainStoreMod, VHost) ->
    gen_server2:start_link(?MODULE, [RetainStoreMod, VHost], []).

retain(Pid, Topic, Msg = #mqtt_msg{retain = true}) ->
    gen_server2:cast(Pid, {retain, Topic, Msg});

retain(_Pid, _Topic, Msg = #mqtt_msg{retain = false}) ->
    throw({error, {retain_is_false, Msg}}).

fetch(Pid, Topic) ->
    gen_server2:call(Pid, {fetch, Topic}, ?TIMEOUT).

clear(Pid, Topic) ->
    gen_server2:cast(Pid, {clear, Topic}).

%%----------------------------------------------------------------------------

init([StoreMod, VHost]) ->
    process_flag(trap_exit, true),
    State = case StoreMod:recover(store_dir(), VHost) of
                {ok, Store} -> #retainer_state{store = Store,
                                               store_mod = StoreMod};
                {error, _}  -> #retainer_state{store = StoreMod:new(store_dir(), VHost),
                                               store_mod = StoreMod}
            end,
    {ok, State}.

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
    Reply = case Mod:lookup(Topic, Store) of
                #retained_message{mqtt_msg = Msg} -> Msg;
                not_found                         -> undefined
            end,
    {reply, Reply, State}.

handle_info(stop, State) ->
    {stop, normal, State};

handle_info(Info, State) ->
    {stop, {unknown_info, Info}, State}.

store_dir() ->
    rabbit_mnesia:dir().

terminate(_Reason, #retainer_state{store = Store, store_mod = Mod}) ->
    Mod:terminate(Store),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
