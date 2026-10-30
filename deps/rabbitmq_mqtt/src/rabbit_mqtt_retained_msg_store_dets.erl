%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_dets).

-behaviour(rabbit_mqtt_retained_msg_store).

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").
-include_lib("kernel/include/logger.hrl").

-export([new/2, recover/2, open/3, delete_vhost/2, remove_leftovers/2, insert/3, lookup/2, delete/2, info/1, terminate/1]).

-record(store_state, {table :: dets:tab_name()}).

-type store_state() :: #store_state{}.

-spec new(file:name_all(), rabbit_types:vhost()) -> store_state().
new(Dir, VHost) ->
    {ok, TabName} = open_legacy_table(Dir, VHost),
    #store_state{table = TabName}.

-spec recover(file:name_all(), rabbit_types:vhost()) ->
    {ok, store_state(), rabbit_mqtt_retained_msg_store:expire()} |
    {error, uninitialized}.
recover(Dir, VHost) ->
    case open_legacy_table(Dir, VHost) of
        {ok, TabName}  ->
            {ok,
             #store_state{table = TabName},
             rabbit_mqtt_retained_msg_store:expire(dets, TabName)};
        {error, Reason} ->
            ?LOG_ERROR("~s failed to open table: ~p", [?MODULE, Reason]),
            {error, uninitialized}
    end.

-spec open(file:name_all(), rabbit_types:vhost(), vhost:id()) ->
    {ok, store_state(), rabbit_mqtt_retained_msg_store:expire()}.
open(Dir, VHost, Id) ->
    Path = rabbit_mqtt_retained_msg_store:incarnation_path(path(Dir, VHost), Id),
    {ok, Tab} = open_table(Path, Path),
    {ok, #store_state{table = Tab}, rabbit_mqtt_retained_msg_store:expire(dets, Tab)}.

-spec insert(topic(), mqtt_msg(), store_state()) -> ok.
insert(Topic, Msg, #store_state{table = T}) ->
  ok = dets:insert(T, #retained_message{topic = Topic, mqtt_msg = Msg}).

-spec lookup(topic(), store_state()) ->
    mqtt_msg() | mqtt_msg_v0() | undefined.
lookup(Topic, #store_state{table = T}) ->
  case dets:lookup(T, Topic) of
    []      -> undefined;
    [#retained_message{mqtt_msg = Msg}] -> Msg
  end.

-spec delete(topic(), store_state()) -> ok.
delete(Topic, #store_state{table = T}) ->
  ok = dets:delete(T, Topic).

-spec info(store_state()) -> rabbit_mqtt_retained_msg_store:store_info().
info(#store_state{table = T}) ->
  %% `dets:info/1` returns both items, `dets:info/2` would take two calls.
  Info = dets:info(T),
  #{count => proplists:get_value(size, Info),
    size_bytes => proplists:get_value(file_size, Info)}.

-spec terminate(store_state()) -> ok.
terminate(#store_state{table = T}) ->
  ok = dets:close(T).

open_legacy_table(Dir, VHost) ->
    open_table(rabbit_mqtt_util:vhost_name_to_table_name(VHost), path(Dir, VHost)).

open_table(Tab, Path) ->
    AutoSave = rabbit_misc:get_env(?APP_NAME, retained_message_store_dets_sync_interval, 2000),
    dets:open_file(Tab, [{type, set},
                         {keypos, #retained_message.topic},
                         {file, Path},
                         {ram_file, false},
                         {repair, true},
                         {auto_save, AutoSave}]).

-spec delete_vhost(rabbit_types:vhost(), vhost:id() | undefined) -> ok | {error, term()}.
delete_vhost(VHost, Id) ->
  rabbit_mqtt_retained_msg_store:delete_file(path(rabbit:data_dir(), VHost), Id).

-spec remove_leftovers(file:name_all(), #{rabbit_types:vhost() => term()}) -> ok | {error, term()}.
remove_leftovers(Dir, Live) ->
    rabbit_mqtt_retained_msg_store:remove_leftover_files(Dir, ".dets", Live).

path(Dir, VHost) -> rabbit_mqtt_util:path_for(Dir, VHost, ".dets").
