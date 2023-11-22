%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_ets).

-behaviour(rabbit_mqtt_retained_msg_store).

-include("rabbit_mqtt_packet.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

-record(store_state, {
          table :: ets:tid(),
          filename :: file:filename_all()
         }).

-type store_state() :: #store_state{}.

-spec new(file:name_all(), rabbit_types:vhost()) -> store_state().
new(Dir, VHost) ->
  Path = rabbit_mqtt_util:path_for(Dir, VHost),
  TableName = rabbit_mqtt_util:vhost_name_to_table_name(VHost),
  _ = file:delete(Path),
  Tid = ets:new(TableName, [set, public, {keypos, #retained_message.topic}]),
  #store_state{table = Tid, filename = Path}.

-spec recover(file:name_all(), rabbit_types:vhost()) ->
    {ok, store_state(), rabbit_mqtt_retained_msg_store:expire()} |
    {error, uninitialized}.
recover(Dir, VHost) ->
    Path = rabbit_mqtt_util:path_for(Dir, VHost),
    case ets:file2tab(Path) of
        {ok, Tid} ->
            _ = file:delete(Path),
            {ok,
             #store_state{table = Tid, filename = Path},
             rabbit_mqtt_retained_msg_store:expire(ets, Tid)};
        {error, _} ->
            {error, uninitialized}
    end.

-spec insert(topic(), mqtt_msg(), store_state()) -> ok.
insert(Topic, Msg, #store_state{table = T}) ->
  true = ets:insert(T, #retained_message{topic = Topic, mqtt_msg = Msg}),
  ok.

-spec lookup(topic(), store_state()) -> mqtt_msg() | mqtt_msg_v0() | undefined.
lookup(Topic, #store_state{table = T}) ->
  case ets:lookup(T, Topic) of
    []      -> undefined;
    [#retained_message{mqtt_msg = Msg}] -> Msg
  end.

-spec delete(topic(), store_state()) -> ok.
delete(Topic, #store_state{table = T}) ->
  true = ets:delete(T, Topic),
  ok.

-spec terminate(store_state()) -> ok.
terminate(#store_state{table = T, filename = Path}) ->
  ok = ets:tab2file(T, Path, [{extended_info, [object_count]}]).
