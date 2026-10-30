%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retainer).

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, start_link/1, start_link/2]).

-export([retain/3, fetch/2, clear/2]).

-define(TIMEOUT, 30_000).
-define(LIMIT_LOG_INTERVAL_MS, 60_000).

-type limit() :: non_neg_integer() | infinity.

-define(STATE, ?MODULE).
-record(?STATE, {store_state :: rabbit_mqtt_retained_msg_store:state(),
                 expire :: #{topic() := TimerRef :: reference()},
                 vhost :: rabbit_types:vhost(),
                 max_messages :: limit(),
                 max_size_bytes :: limit(),
                 incarnation :: vhost:id() | undefined,
                 storage_id :: vhost:id() | legacy,
                 last_limit_log :: integer() | undefined
                }).
-type state() :: #?STATE{}.

-spec start_link(rabbit_types:vhost()) ->
    gen_server:start_ret().
start_link(VHost) ->
    case rabbit_db_vhost:identity(VHost) of
        {ok, _, Id} -> start_link(VHost, {incarnation, Id});
        {error, _} -> ignore
    end.

start_link(VHost, {incarnation, Id}) ->
    gen_server:start_link(?MODULE, {VHost, Id}, []).

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

-spec init({rabbit_types:vhost(), vhost:id() | legacy}) -> {ok, state()} | ignore.
init({VHost, StorageId}) ->
    case rabbit_db_vhost:identity(VHost) of
        {ok, Id, StorageId} ->
            try init_store(VHost, Id, StorageId) of
                {ok, _} = Result ->
                    true = rabbit_mqtt_retainer_sup:register_retainer(VHost, Id, StorageId),
                    Result
            catch Class:Reason ->
                ?LOG_ERROR("Cannot open MQTT retained store for vhost '~ts': ~tp", [VHost, {Class, Reason}]),
                ignore
            end;
        _ -> ignore
    end.

init_store(VHost, Id, StorageId) ->
    process_flag(trap_exit, true),
    {StoreState, Expire0} = rabbit_mqtt_retained_msg_store:start(VHost, StorageId),
    _ = erlang:send_after(5000, self(), check_vhost),
    Now = os:system_time(second),
    Expire = maps:map(fun(Topic, {Timestamp, Expiry}) ->
                              TimerSecs = max(0, Expiry - (Now - Timestamp)),
                              start_timer(TimerSecs, Topic)
                      end, Expire0),
    {ok, #?STATE{store_state = StoreState,
                 expire = Expire,
                 vhost = VHost,
                 incarnation = Id,
                 storage_id = StorageId,
                 max_messages = get_limit(retained_message_store_max_messages),
                 max_size_bytes = get_limit(retained_message_store_max_size_bytes)}}.

handle_cast({retain, Topic, Msg}, State) ->
    case retain_permitted(Topic, Msg, State) of
        true ->
            {noreply, do_retain(Topic, Msg, State)};
        false ->
            {noreply, log_limit_reached(State)}
    end;
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
handle_info(check_vhost, #?STATE{vhost = VHost, incarnation = Id} = State) ->
    case rabbit_db_vhost:get(VHost) of
        undefined -> check_identity(State);
        V ->
            case vhost:get_id(V) of
                Id -> continue_checking(State);
                _ -> check_identity(State)
            end
    end;
handle_info(stop, State) ->
    {stop, normal, State};
handle_info(Info, State) ->
    {stop, {unknown_info, Info}, State}.

check_identity(#?STATE{vhost = VHost, storage_id = StorageId} = State) ->
    case rabbit_db_vhost:identity(VHost) of
        {ok, Id, StorageId} ->
            true = rabbit_mqtt_retainer_sup:register_retainer(VHost, Id, StorageId),
            continue_checking(State#?STATE{incarnation = Id});
        {error, Reason} when Reason =/= not_found -> continue_checking(State);
        _ -> {stop, normal, State}
    end.

continue_checking(State) ->
    _ = erlang:send_after(5000, self(), check_vhost),
    {noreply, State}.

terminate(_Reason, #?STATE{store_state = StoreState}) ->
    rabbit_mqtt_retained_msg_store:terminate(StoreState).

-spec do_retain(topic(), mqtt_msg(), state()) -> state().
do_retain(Topic, Msg0 = #mqtt_msg{props = Props},
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
    State#?STATE{expire = Expire}.

-spec get_limit(atom()) -> limit().
get_limit(Key) ->
    case application:get_env(?APP_NAME, Key, infinity) of
        infinity -> infinity;
        Int when is_integer(Int), Int >= 0 -> Int
    end.

%% Overwriting a topic that is already retained does not add an entry, so it
%% stays permitted once the store holds `max_messages` entries.
-spec retain_permitted(topic(), mqtt_msg(), state()) -> boolean().
retain_permitted(_Topic, _Msg, #?STATE{max_messages = infinity,
                                       max_size_bytes = infinity}) ->
    true;
retain_permitted(Topic, Msg,
                 #?STATE{store_state = StoreState,
                         max_messages = MaxMessages,
                         max_size_bytes = MaxSizeBytes}) ->
    #{count := Count,
      size_bytes := SizeBytes} = rabbit_mqtt_retained_msg_store:info(StoreState),
    Existing = rabbit_mqtt_retained_msg_store:lookup(Topic, StoreState),
    %% The overwritten entry's size is subtracted so that overwrites are
    %% not double-counted.
    PreviousBytesVal = case Existing of
                           undefined ->
                               0;
                           #mqtt_msg{} ->
                               entry_size(Existing)
                       end,
    NewBytesVal = entry_size(Msg),
    within_limit(SizeBytes - PreviousBytesVal + NewBytesVal, MaxSizeBytes) andalso
    (Existing =/= undefined orelse within_limit(Count + 1, MaxMessages)).

entry_size(Msg) ->
    {MetadataSize, PayloadSize} = mc_mqtt:size(Msg),
    MetadataSize + PayloadSize.

-spec within_limit(non_neg_integer(), limit()) -> boolean().
within_limit(_Value, infinity) ->
    true;
within_limit(Value, Limit) ->
    Value =< Limit.

-spec log_limit_reached(state()) -> state().
log_limit_reached(State = #?STATE{vhost = VHost,
                                  max_messages = MaxMessages,
                                  max_size_bytes = MaxSizeBytes,
                                  last_limit_log = LastLog}) ->
    Now = erlang:monotonic_time(millisecond),
    case LastLog =:= undefined orelse Now - LastLog >= ?LIMIT_LOG_INTERVAL_MS of
        true ->
            ?LOG_WARNING(
               "MQTT retained message store for virtual host '~ts' reached its limit "
               "(max_messages: ~p, max_size_bytes: ~p), new retained messages are dropped",
               [VHost, MaxMessages, MaxSizeBytes]),
            State#?STATE{last_limit_log = Now};
        false ->
            State
    end.

-spec start_timer(integer(), topic()) -> reference().
start_timer(Seconds, Topic)
  when is_binary(Topic) ->
    erlang:start_timer(timer:seconds(Seconds), self(), Topic).

-spec cancel_timer(reference()) -> ok.
cancel_timer(TimerRef) ->
    ok = erlang:cancel_timer(TimerRef, [{async, true},
                                        {info, false}]).
