%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
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

-module(rabbit_mqtt_retained_msg_store_dets).

-behaviour(rabbit_mqtt_retained_msg_store).
-include("rabbit_mqtt.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

-record(store_state, {
  %% DETS table name
  table
}).


new(Dir, VHost) ->
  Tid = open_table(Dir, VHost),
  #store_state{table = Tid}.

recover(Dir, VHost) ->
  case open_table(Dir, VHost) of
    {error, _} -> {error, uninitialized};
    {ok, Tid}  -> {ok, #store_state{table = Tid}}
  end.

insert(Topic, Msg, #store_state{table = T}) ->
  ok = dets:insert(T, #retained_message{topic = Topic, mqtt_msg = Msg}).

lookup(Topic, #store_state{table = T}) ->
  case dets:lookup(T, Topic) of
    []      -> not_found;
    [Entry] -> Entry
  end.

delete(Topic, #store_state{table = T}) ->
  ok = dets:delete(T, Topic).

terminate(#store_state{table = T}) ->
  ok = dets:close(T).

open_table(Dir, VHost) ->
  dets:open_file(rabbit_mqtt_retained_msg_store:table_name_for(VHost),
    table_options(rabbit_mqtt_util:path_for(Dir, VHost, ".dets"))).

table_options(Path) ->
  [{type, set}, {keypos, #retained_message.topic},
    {file, Path}, {ram_file, true}, {repair, true},
    {auto_save, rabbit_misc:get_env(rabbit_mqtt,
                                    retained_message_store_dets_sync_interval, 2000)}].
