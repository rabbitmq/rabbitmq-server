%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mqtt_retainer).

-include("rabbit_mqtt_packet.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, start_link/1]).

-export([retain/3, fetch/2, clear/2]).

-define(TIMEOUT, 30_000).

-define(STATE, ?MODULE).
-record(?STATE, {store_state :: rabbit_mqtt_retained_msg_store:state(),
                 expire :: #{topic() := TimerRef :: reference()}
                }).
-type state() :: #?STATE{}.

-spec start_link(rabbit_types:vhost()) ->
    gen_server:start_ret().
start_link(VHost) ->
    gen_server:start_link(?MODULE, VHost, []).

-spec retain(pid(), topic(), mqtt_msg()) -> ok.
retain(Pid, Topic, Msg = #mqtt_msg{retain = true}) ->
    gen_server:cast(Pid, {retain, Topic, Msg}).

-spec fetch(pid(), topic()) ->
    undefined | mqtt_msg().
fetch(Pid, Topic) ->
    gen_server:call(Pid, {fetch, Topic}, ?TIMEOUT).

-spec clear(pid(), topic()) -> ok.
clear(Pid, Topic) ->
    gen_server:cast(Pid, {clear, Topic}).

-spec init(rabbit_types:vhost()) ->
    {ok, state()}.
init(VHost) ->
    process_flag(trap_exit, true),
    {StoreState, Expire0} = rabbit_mqtt_retained_msg_store:start(VHost),
    Now = os:system_time(second),
    Expire = maps:map(fun(Topic, {Timestamp, Expiry}) ->
                              TimerSecs = max(0, Expiry - (Now - Timestamp)),
                              start_timer(TimerSecs, Topic)
                      end, Expire0),
    {ok, #?STATE{store_state = StoreState,
                 expire = Expire}}.

handle_cast({retain, Topic, Msg0 = #mqtt_msg{props = Props}},
            State = #?STATE{store_state = StoreState,
                            expire = Expire0}) ->
    Expire2 = case maps:take(Topic, Expire0) of
                  {OldTimer, Expire1} ->
                      cancel_timer(OldTimer),
                      Expire1;
                  error ->
                      Expire0
              end,
    {Msg, Expire} = case maps:find('Message-Expiry-Interval', Props) of
                        {ok, ExpirySeconds} ->
                            Timer = start_timer(ExpirySeconds, Topic),
                            {Msg0#mqtt_msg{timestamp = os:system_time(second)},
                             maps:put(Topic, Timer, Expire2)};
                        error ->
                            {Msg0, Expire2}
                    end,
    ok = rabbit_mqtt_retained_msg_store:insert(Topic, Msg, StoreState),
    {noreply, State#?STATE{expire = Expire}};
handle_cast({clear, Topic}, State = #?STATE{store_state = StoreState,
                                            expire = Expire0}) ->
    Expire = case maps:take(Topic, Expire0) of
                 {OldTimer, Expire1} ->
                     cancel_timer(OldTimer),
                     Expire1;
                 error ->
                     Expire0
             end,
    ok = rabbit_mqtt_retained_msg_store:delete(Topic, StoreState),
    {noreply, State#?STATE{expire = Expire}}.

handle_call({fetch, Topic}, _From, State = #?STATE{store_state = StoreState}) ->
    Reply = case rabbit_mqtt_retained_msg_store:lookup(Topic, StoreState) of
                #mqtt_msg{props = #{'Message-Expiry-Interval' := Expiry0} = Props,
                          timestamp = Timestamp} = MqttMsg ->
                    %% “The PUBLISH packet sent to a Client by the Server MUST contain a Message
                    %% Expiry Interval set to the received value minus the time that the
                    %% Application Message has been waiting in the Server [MQTT-3.3.2-6].”
                    Expiry = max(0, Expiry0 - (os:system_time(second) - Timestamp)),
                    MqttMsg#mqtt_msg{props = maps:put('Message-Expiry-Interval', Expiry, Props)};
                Other ->
                    Other
            end,
    {reply, Reply, State}.

handle_info({timeout, Timer, Topic}, State = #?STATE{store_state = StoreState,
                                                     expire = Expire0}) ->
    Expire = case maps:take(Topic, Expire0) of
                 {Timer, Expire1} ->
                     ok = rabbit_mqtt_retained_msg_store:delete(Topic, StoreState),
                     Expire1;
                 _ ->
                     Expire0
             end,
    {noreply, State#?STATE{expire = Expire}};
handle_info(stop, State) ->
    {stop, normal, State};
handle_info(Info, State) ->
    {stop, {unknown_info, Info}, State}.

terminate(_Reason, #?STATE{store_state = StoreState}) ->
    rabbit_mqtt_retained_msg_store:terminate(StoreState).

-spec start_timer(integer(), topic()) -> reference().
start_timer(Seconds, Topic)
  when is_binary(Topic) ->
    erlang:start_timer(timer:seconds(Seconds), self(), Topic).

-spec cancel_timer(reference()) -> ok.
cancel_timer(TimerRef) ->
    ok = erlang:cancel_timer(TimerRef, [{async, true},
                                        {info, false}]).
