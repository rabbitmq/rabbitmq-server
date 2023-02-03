%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retainer).

-include("rabbit_mqtt_packet.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, start_link/1]).

-export([retain/3, fetch/2, clear/2]).

-define(TIMEOUT, 30_000).

-spec start_link(rabbit_types:vhost()) ->
    gen_server:start_ret().
start_link(VHost) ->
    gen_server:start_link(?MODULE, VHost, []).

-spec retain(pid(), binary(), mqtt_msg()) -> ok.
retain(Pid, Topic, Msg = #mqtt_msg{retain = true}) ->
    gen_server:cast(Pid, {retain, Topic, Msg}).

-spec fetch(pid(), binary()) ->
    undefined | mqtt_msg().
fetch(Pid, Topic) ->
    gen_server:call(Pid, {fetch, Topic}, ?TIMEOUT).

-spec clear(pid(), binary()) -> ok.
clear(Pid, Topic) ->
    gen_server:cast(Pid, {clear, Topic}).

-spec init(rabbit_types:vhost()) ->
    {ok, rabbit_mqtt_retained_msg_store:state()}.
init(VHost) ->
    process_flag(trap_exit, true),
    State = rabbit_mqtt_retained_msg_store:start(VHost),
    {ok, State}.

handle_cast({retain, Topic, Msg}, State) ->
    ok = rabbit_mqtt_retained_msg_store:insert(Topic, Msg, State),
    {noreply, State};
handle_cast({clear, Topic}, State) ->
    ok = rabbit_mqtt_retained_msg_store:delete(Topic, State),
    {noreply, State}.

handle_call({fetch, Topic}, _From, State) ->
    Reply = rabbit_mqtt_retained_msg_store:lookup(Topic, State),
    {reply, Reply, State}.

handle_info(stop, State) ->
    {stop, normal, State};
handle_info(Info, State) ->
    {stop, {unknown_info, Info}, State}.

terminate(_Reason, State) ->
    rabbit_mqtt_retained_msg_store:terminate(State).
