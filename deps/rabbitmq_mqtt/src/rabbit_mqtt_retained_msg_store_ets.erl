%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_ets).

-behaviour(rabbit_mqtt_retained_msg_store).
-include("rabbit_mqtt.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

-record(store_state, {
  %% ETS table ID
  table,
  %% where the table is stored on disk
  filename
}).


new(Dir, VHost) ->
  Path = rabbit_mqtt_util:path_for(Dir, VHost),
  TableName = rabbit_mqtt_retained_msg_store:table_name_for(VHost),
  file:delete(Path),
  Tid = ets:new(TableName, [set, public, {keypos, #retained_message.topic}]),
  #store_state{table = Tid, filename = Path}.

recover(Dir, VHost) ->
  Path = rabbit_mqtt_util:path_for(Dir, VHost),
  case ets:file2tab(Path) of
    {ok, Tid}  -> file:delete(Path),
                  {ok, #store_state{table = Tid, filename = Path}};
    {error, _} -> {error, uninitialized}
  end.

insert(Topic, Msg, #store_state{table = T}) ->
  true = ets:insert(T, #retained_message{topic = Topic, mqtt_msg = Msg}),
  ok.

lookup(Topic, #store_state{table = T}) ->
  case ets:lookup(T, Topic) of
    []      -> not_found;
    [Entry] -> Entry
  end.

delete(Topic, #store_state{table = T}) ->
  true = ets:delete(T, Topic),
  ok.

terminate(#store_state{table = T, filename = Path}) ->
  ok = ets:tab2file(T, Path,
                    [{extended_info, [object_count]}]).
