%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store).

%% TODO Support retained messages in RabbitMQ cluster, for
%% 1. support PUBLISH with retain on a different node than SUBSCRIBE
%% 2. replicate retained message for data safety
%%
%% Possible solution for 1.
%% * retained message store backend does RPCs to peer nodes to lookup and delete
%%
%% Possible solutions for 2.
%% * rabbitmq_mqtt_retained_msg_store_khepri
%% * rabbitmq_mqtt_retained_msg_store_ra (implementing our own ra machine)

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").
-include_lib("kernel/include/logger.hrl").
-export([start/1, insert/3, lookup/2, delete/2, terminate/1]).
-export([expire/2]).
-export_type([state/0, expire/0]).

-define(STATE, ?MODULE).
-record(?STATE, {store_mod :: module(),
                 store_state :: term()}).
-opaque state() :: #?STATE{}.

-type expire() :: #{topic() :=
                    {InsertionTimestamp :: integer(),
                     MessageExpiryInterval :: pos_integer()}}.

-callback new(Directory :: file:name_all(), rabbit_types:vhost()) ->
    State :: any().

-callback recover(Directory :: file:name_all(), rabbit_types:vhost()) ->
    {ok, State :: any(), expire()} |
    {error, uninitialized}.

-callback insert(topic(), mqtt_msg(), State :: any()) ->
    ok.

-callback lookup(topic(), State :: any()) ->
    mqtt_msg() | mqtt_msg_v0() | undefined.

-callback delete(topic(), State :: any()) ->
    ok.

-callback terminate(State :: any()) ->
    ok.

-spec start(rabbit_types:vhost()) -> {state(), expire()}.
start(VHost) ->
    {ok, Mod} = application:get_env(?APP_NAME, retained_message_store),
    Dir = rabbit:data_dir(),
    ?LOG_INFO("Starting MQTT retained message store ~s for vhost '~ts'",
              [Mod, VHost]),
    {S, Expire} = case Mod:recover(Dir, VHost) of
                      {ok, StoreState, Expire0} ->
                          ?LOG_INFO("Recovered MQTT retained message store ~s for vhost '~ts'",
                                    [Mod, VHost]),
                          {StoreState, Expire0};
                      {error, uninitialized} ->
                          StoreState = Mod:new(Dir, VHost),
                          ?LOG_INFO("Initialized MQTT retained message store ~s for vhost '~ts'",
                                    [Mod, VHost]),
                          {StoreState, #{}}
                  end,
    {#?STATE{store_mod = Mod,
             store_state = S}, Expire}.

-spec insert(topic(), mqtt_msg(), state()) -> ok.
insert(Topic, Msg, #?STATE{store_mod = Mod,
                           store_state = StoreState}) ->
    ok = Mod:insert(Topic, Msg, StoreState).

-spec lookup(topic(), state()) ->
    mqtt_msg() | undefined.
lookup(Topic, #?STATE{store_mod = Mod,
                      store_state = StoreState}) ->
    case Mod:lookup(Topic, StoreState) of
        OldMsg when is_record(OldMsg, mqtt_msg, 7) ->
            convert_mqtt_msg(OldMsg);
        Other ->
            Other
    end.

-spec delete(topic(), state()) -> ok.
delete(Topic, #?STATE{store_mod = Mod,
                      store_state = StoreState}) ->
    ok = Mod:delete(Topic, StoreState).

-spec terminate(state()) -> ok.
terminate(#?STATE{store_mod = Mod,
                  store_state = StoreState}) ->
    ok = Mod:terminate(StoreState).

-spec expire(ets | dets, ets:tid() | dets:tab_name()) -> expire().
expire(Mod, Tab) ->
    Now = os:system_time(second),
    Mod:foldl(
      fun(#retained_message{topic = Topic,
                            mqtt_msg = #mqtt_msg{props = #{'Message-Expiry-Interval' := Expiry},
                                                 timestamp = Timestamp}}, Acc)
            when is_integer(Expiry) andalso
                 is_integer(Timestamp) ->
              if Now - Timestamp >= Expiry ->
                     Mod:delete(Tab, Topic),
                     Acc;
                 true ->
                     maps:put(Topic, {Timestamp, Expiry}, Acc)
              end;
         (_, Acc) ->
              Acc
      end, #{}, Tab).

%% Retained messages written in 3.12 (or earlier) are converted when read in 3.13 (or later).
-spec convert_mqtt_msg(mqtt_msg_v0()) -> mqtt_msg().
convert_mqtt_msg({mqtt_msg, Retain, Qos, Topic, Dup, PacketId, Payload}) ->
    #mqtt_msg{retain = Retain,
              qos = Qos,
              topic = Topic,
              dup = Dup,
              packet_id = PacketId,
              payload = Payload,
              props = #{}}.
