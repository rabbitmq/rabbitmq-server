%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
-export([start/1, start/2, insert/3, lookup/2, delete/2, info/1, terminate/1]).
-export([expire/2, incarnation_path/2, delete_file/2, remove_leftover_files/3,
         delete_vhost/2, remove_leftovers/1]).
-export_type([state/0, expire/0, store_info/0]).

-define(STATE, ?MODULE).
-record(?STATE, {store_mod :: module(),
                 store_state :: term()}).
-opaque state() :: #?STATE{}.

-type store_info() :: #{count := non_neg_integer(),
                        size_bytes := non_neg_integer()}.

-type expire() :: #{topic() :=
                    {InsertionTimestamp :: integer(),
                     MessageExpiryInterval :: pos_integer()}}.

-callback new(Directory :: file:name_all(), rabbit_types:vhost()) ->
    State :: any().

-callback recover(Directory :: file:name_all(), rabbit_types:vhost()) ->
    {ok, State :: any(), expire()} |
    {error, uninitialized}.

-callback open(file:name_all(), rabbit_types:vhost(), vhost:id()) ->
    {ok, any(), expire()}.
-callback delete_vhost(rabbit_types:vhost(), vhost:id() | undefined) -> ok | {error, term()}.
-callback remove_leftovers(file:name_all(), #{rabbit_types:vhost() => vhost:id() | undefined}) ->
    ok | {error, term()}.
-optional_callbacks([open/3, delete_vhost/2, remove_leftovers/2]).

-callback insert(topic(), mqtt_msg(), State :: any()) ->
    ok.

-callback lookup(topic(), State :: any()) ->
    mqtt_msg() | mqtt_msg_v0() | undefined.

-callback delete(topic(), State :: any()) ->
    ok.

-callback info(State :: any()) ->
    store_info().

-callback terminate(State :: any()) ->
    ok.

-spec start(rabbit_types:vhost()) -> {state(), expire()}.
start(VHost) ->
    start_legacy(VHost, rabbit_vhost:legacy_data_dir()).

start_legacy(VHost, Dir) ->
    ok = filelib:ensure_path(Dir),
    {ok, Mod} = application:get_env(?APP_NAME, retained_message_store),
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

-spec start(rabbit_types:vhost(), vhost:id() | legacy) -> {state(), expire()}.
start(VHost, legacy) -> start(VHost);
start(VHost, Id) when is_integer(Id) ->
    {ok, Mod} = application:get_env(?APP_NAME, retained_message_store),
    {module, Mod} = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, open, 3) of
        true ->
            {ok, State, Expire} = Mod:open(rabbit:data_dir(), VHost, Id),
            {#?STATE{store_mod = Mod, store_state = State}, Expire};
        false ->
            Dir = filename:join(rabbit:data_dir(), "mqtt_retained_" ++ integer_to_list(Id)),
            ok = filelib:ensure_path(Dir),
            start_legacy(VHost, Dir)
    end.

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

-spec info(state()) -> store_info().
info(#?STATE{store_mod = Mod,
             store_state = StoreState}) ->
    Mod:info(StoreState).

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

-spec delete_vhost(rabbit_types:vhost(), vhost:id() | undefined) -> ok | {error, term()}.
delete_vhost(VHost, Id) ->
    optional(delete_vhost, [VHost, Id]).

-spec remove_leftovers(#{rabbit_types:vhost() => vhost:id() | undefined}) -> ok | {error, term()}.
remove_leftovers(Live) ->
    optional(remove_leftovers, [rabbit:data_dir(), Live]).

optional(Fun, Args) ->
    {ok, Mod} = application:get_env(?APP_NAME, retained_message_store),
    try
        case code:ensure_loaded(Mod) =:= {module, Mod} andalso
             erlang:function_exported(Mod, Fun, length(Args)) of
            true -> erlang:apply(Mod, Fun, Args);
            false -> ok
        end
    catch Class:Reason -> {error, {Class, Reason}}
    end.

-spec incarnation_path(file:filename(), vhost:id()) -> file:filename().
incarnation_path(Path, Id) when is_integer(Id), Id > 0 ->
    Path ++ "." ++ integer_to_list(Id).

-spec delete_file(file:filename(), vhost:id() | undefined) -> ok | {error, term()}.
delete_file(_Path, undefined) -> ok;
delete_file(Path, Id) ->
    remove(incarnation_path(Path, Id)).

-spec remove_leftover_files(file:name_all(), string(), #{rabbit_types:vhost() => term()}) ->
    ok | {error, term()}.
remove_leftover_files(Dir, Extension, _Live) ->
    Candidates = [Path || Path <- filelib:wildcard(filename:join(Dir, "mqtt_retained_*" ++ Extension ++ ".*")),
                          valid_file_id(filename:extension(Path))],
    case rabbit_db_vhost:cleanup_snapshot() of
        {ok, Epoch, Live} ->
            Current = maps:groups_from_list(
                        fun({VHost, _}) -> rabbit_mqtt_util:path_for(Dir, VHost, Extension) end,
                        fun({_, Id}) -> Id end, maps:to_list(Live)),
            lists:foldl(fun(Path, ok) ->
                            [$. | Digits] = filename:extension(Path),
                            Id = list_to_integer(Digits),
                            Owners = maps:find(filename:rootname(Path), Current),
                            case {vhost:id_epoch(Id) =:= Epoch, Owners} of
                                {false, _} -> ok;
                                {true, error} -> remove(Path);
                                {true, {ok, Ids}} ->
                                    case lists:all(fun(New) -> is_integer(New) andalso New > Id end, Ids) of
                                        true -> remove(Path);
                                        false -> ok
                                    end
                            end;
                           (_, Error) -> Error
                        end, ok, Candidates);
        {error, _} = Error -> Error
    end.

valid_file_id([$. | Digits]) ->
    case string:to_integer(Digits) of
        {Id, []} when Id > 0 -> integer_to_list(Id) =:= Digits;
        _ -> false
    end;
valid_file_id(_) -> false.

remove(Path) ->
    case file:delete(Path) of
        {error, enoent} -> ok;
        Result -> Result
    end.
