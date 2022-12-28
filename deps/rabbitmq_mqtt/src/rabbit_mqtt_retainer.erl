%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retainer).

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, start_link/2]).

-export([retain/3, fetch/2, clear/2, store_module/0]).

-define(TIMEOUT, 30_000).

-record(retainer_state, {store_mod,
                         store}).

%%----------------------------------------------------------------------------

start_link(RetainStoreMod, VHost) ->
    gen_server:start_link(?MODULE, [RetainStoreMod, VHost], []).

-spec retain(pid(), binary(), mqtt_msg()) -> ok.
retain(Pid, Topic, Msg = #mqtt_msg{retain = true}) ->
    gen_server:cast(Pid, {retain, Topic, Msg});
retain(_Pid, _Topic, Msg = #mqtt_msg{retain = false}) ->
    throw({error, {retain_is_false, Msg}}).

-spec fetch(pid(), binary()) ->
    undefined | mqtt_msg().
fetch(Pid, Topic) ->
    gen_server:call(Pid, {fetch, Topic}, ?TIMEOUT).

-spec clear(pid(), binary()) -> ok.
clear(Pid, Topic) ->
    gen_server:cast(Pid, {clear, Topic}).

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

-spec store_module() -> undefined | module().
store_module() ->
    case application:get_env(?APP_NAME, retained_message_store) of
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
    rabbit:data_dir().

terminate(_Reason, #retainer_state{store = Store, store_mod = Mod}) ->
    Mod:terminate(Store).
