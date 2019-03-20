%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
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
