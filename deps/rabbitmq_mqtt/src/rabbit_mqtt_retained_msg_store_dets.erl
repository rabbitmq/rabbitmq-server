%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_dets).

-behaviour(rabbit_mqtt_retained_msg_store).

-include("rabbit_mqtt_packet.hrl").

-include_lib("kernel/include/logger.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

-record(store_state,
        {node_table :: dets:tab_name(),    % Stores {node_id, edge_count, is_topic}
         edge_table :: dets:tab_name(),    % Stores {{from_id, word}, to_id}
         msg_table :: dets:tab_name(),     % Stores {node_id, topic, mqtt_msg}
         root_id :: binary(),              % Root node ID
         dir :: file:filename_all(),
         vhost :: rabbit_types:vhost(),
         max_retained_messages_count :: pos_integer()}).

-type store_state() :: #store_state{}.

-spec new(file:name_all(), rabbit_types:vhost()) -> store_state().
new(Dir, VHost) ->
  {ok, NodeTable} = open_table(Dir, VHost, <<"nodes">>, set),
  {ok, EdgeTable} = open_table(Dir, VHost, <<"edges">>, set),
  {ok, MsgTable} = open_table(Dir, VHost, <<"msgs">>, set),
  {ok, RootId} = find_or_insert_root_node(NodeTable, EdgeTable),
  MaxRetainedMessagesCount =
    rabbit_mqtt_retained_msg_store:get_max_retained_messages_count(),
  #store_state{node_table = NodeTable,
               edge_table = EdgeTable,
               msg_table = MsgTable,
               root_id = RootId,
               dir = Dir,
               vhost = VHost,
               max_retained_messages_count = MaxRetainedMessagesCount}.

-spec recover(file:name_all(), rabbit_types:vhost()) ->
               {ok, store_state(), rabbit_mqtt_retained_msg_store:expire()} |
               {error, uninitialized}.
recover(Dir, VHost) ->
  try
    {ok, MsgTable} = open_table(Dir, VHost, <<"msgs">>, set),
    Expire = rabbit_mqtt_retained_msg_store:expire(dets, MsgTable),
    {ok, NodeTable} = open_table(Dir, VHost, <<"nodes">>, set),
    {ok, EdgeTable} = open_table(Dir, VHost, <<"edges">>, set),
    {ok, RootId} = find_or_insert_root_node(NodeTable, EdgeTable),
    MaxRetainedMessagesCount =
      rabbit_mqtt_retained_msg_store:get_max_retained_messages_count(),
    State =
      #store_state{node_table = NodeTable,
                   edge_table = EdgeTable,
                   msg_table = MsgTable,
                   root_id = RootId,
                   dir = Dir,
                   vhost = VHost,
                   max_retained_messages_count = MaxRetainedMessagesCount},
    {ok, State, Expire}
  catch
    error:Reason ->
      ?LOG_ERROR("Failed to recover MQTT retained message store: ~p", [Reason]),
      {error, uninitialized}
  end.

-spec insert(topic(), mqtt_msg(), store_state()) -> ok.
insert(Topic, Msg, #store_state{} = State) ->
  Words = split_topic(Topic),
  NodeId = follow_or_create_path(Words, State),
  % Mark node as topic end and store message
  update_node(NodeId, true, State),
  dets:insert(State#store_state.msg_table, {NodeId, Topic, Msg}),
  ok.

-spec lookup(topic(), store_state()) -> [mqtt_msg()] | [mqtt_msg_v0()] | [].
lookup(Topic,
       #store_state{root_id = RootId,
                    msg_table = MsgTable,
                    max_retained_messages_count = Limit} =
         State) ->
  Words = split_topic(Topic),
  Matches = lists:sublist(match_pattern_words(Words, RootId, State, []), Limit),
  Values =
    lists:flatmap(fun(NodeId) ->
                     case dets:lookup(MsgTable, NodeId) of
                       [] -> [];
                       [{_NodeId, _Topic, Value} | _] -> [Value];
                       {error, _Reason} ->
                         ?LOG_ERROR("Failed to lookup MQTT retained message for node ~p", [NodeId]),
                         []
                     end
                  end,
                  Matches),
  Values.

-spec delete(topic(), store_state()) -> ok.
delete(Topic, State) ->
  Words = split_topic(Topic),
  case follow_path(Words, State) of
    {ok, NodeId} ->
      dets:match_delete(State#store_state.msg_table, {NodeId, Topic, '_'}),
      case dets:lookup(State#store_state.msg_table, NodeId) of
        [] ->
          update_node(NodeId, false, State),
          maybe_clean_path(NodeId, State);
        _ ->
          ok
      end;
    error ->
      ok
  end,
  ok.

-spec terminate(store_state()) -> ok.
terminate(#store_state{node_table = NodeTable,
                       edge_table = EdgeTable,
                       msg_table = MsgTable}) ->
  ok = dets:close(NodeTable),
  ok = dets:close(EdgeTable),
  ok = dets:close(MsgTable),
  ok.

%% Internal functions

split_topic(Topic) ->
  binary:split(Topic, <<"/">>, [global]).

make_node_id() ->
  crypto:strong_rand_bytes(16).

get_table_name(VHost, Type) ->
  TableName = rabbit_mqtt_util:vhost_name_to_table_name(VHost),
  Suffix = erlang:iolist_to_binary([<<"_">>, Type]),
  erlang:list_to_atom(erlang:atom_to_list(TableName) ++ erlang:binary_to_list(Suffix)).

get_table_path(Dir, VHost, Type) ->
  % rabbit_mqtt_util:path_for(Dir, VHost, Type ++ ".dets").
  % Suffix = erlang:iolist_to_binary([VHost, <<"_">>, Type]),
  rabbit_mqtt_util:path_for(Dir, erlang:iolist_to_binary([VHost, Type]), ".dets").

find_root_node(NodeTable, EdgeTable) ->
  NodeIds = dets:match(NodeTable, {'$1', '_', '_'}),
  DestNodeIds = dets:match(EdgeTable, {'_', '$1'}),
  case lists:flatten(NodeIds) -- lists:flatten(DestNodeIds) of
    [RootId] ->
      {ok, RootId};
    [] ->
      error;
    _ ->
      error  % Multiple root nodes would indicate corruption
  end.

find_or_insert_root_node(NodeTable, EdgeTable) ->
  case find_root_node(NodeTable, EdgeTable) of
    {ok, Id} ->
      {ok, Id};
    error ->
      NewId = make_node_id(),
      ok = dets:insert(NodeTable, {NewId, 0, false}),
      {ok, NewId}
  end.

follow_or_create_path(Words, State) ->
  follow_or_create_path(Words, State#store_state.root_id, State).

follow_or_create_path([], NodeId, _State) ->
  NodeId;
follow_or_create_path([Word | Rest], NodeId, State) ->
  case find_edge(NodeId, Word, State) of
    {ok, ChildId} ->
      follow_or_create_path(Rest, ChildId, State);
    error ->
      ChildId = make_node_id(),
      add_edge(NodeId, Word, ChildId, State),
      follow_or_create_path(Rest, ChildId, State)
  end.

follow_path(Words, State) ->
  follow_path(Words, State#store_state.root_id, State).

follow_path([], NodeId, _State) ->
  {ok, NodeId};
follow_path([Word | Rest], NodeId, State) ->
  case find_edge(NodeId, Word, State) of
    {ok, ChildId} ->
      follow_path(Rest, ChildId, State);
    error ->
      error
  end.

match_pattern_words([], NodeId, _State, Acc) ->
  [NodeId | Acc];
match_pattern_words([<<"+">> | RestWords], NodeId, State, Acc) ->
  % + matches any single word
  Edges = get_all_edges(NodeId, State),
  lists:foldl(fun({_Key, ChildId}, EdgeAcc) ->
                 match_pattern_words(RestWords, ChildId, State, EdgeAcc)
              end,
              Acc,
              Edges);
match_pattern_words([<<"#">> | _], NodeId, State, Acc) ->
  % # matches zero or more words
  collect_descendants(NodeId, State, [NodeId | Acc]);
match_pattern_words([Word | RestWords], NodeId, State, Acc) ->
  case find_edge(NodeId, Word, State) of
    {ok, ChildId} ->
      match_pattern_words(RestWords, ChildId, State, Acc);
    error ->
      Acc
  end.

collect_descendants(NodeId, State, Acc) ->
  Edges = get_all_edges(NodeId, State),
  lists:foldl(fun({_Key, ChildId}, EdgeAcc) ->
                 collect_descendants(ChildId, State, [ChildId | EdgeAcc])
              end,
              Acc,
              Edges).

find_edge(NodeId, Word, State) ->
  Key = {NodeId, Word},
  case dets:lookup(State#store_state.edge_table, Key) of
    [{_Key, ToNode}] ->
      {ok, ToNode};
    [] ->
      error
  end.

get_all_edges(NodeId, State) ->
  Pattern = {{NodeId, '_'}, '_'},
  dets:match_object(State#store_state.edge_table, Pattern).

add_edge(FromId, Word, ToId, State) ->
  Key = {FromId, Word},
  EdgeEntry = {Key, ToId},
  ok = dets:insert(State#store_state.edge_table, EdgeEntry),
  NodeEntry = {ToId, 0, false},
  ok = dets:insert(State#store_state.node_table, NodeEntry),
  update_edge_count(FromId, +1, State).

update_edge_count(NodeId, Delta, State) ->
  case dets:lookup(State#store_state.node_table, NodeId) of
    [{NodeId, EdgeCount, IsTopic}] ->
      NewCount = EdgeCount + Delta,
      ok = dets:insert(State#store_state.node_table, {NodeId, NewCount, IsTopic});
    [] ->
      error
  end.

update_node(NodeId, IsTopic, State) ->
  case dets:lookup(State#store_state.node_table, NodeId) of
    [{NodeId, EdgeCount, _OldIsTopic}] ->
      ok = dets:insert(State#store_state.node_table, {NodeId, EdgeCount, IsTopic});
    [] ->
      error
  end.

maybe_clean_path(NodeId, State) ->
  case dets:lookup(State#store_state.node_table, NodeId) of
    [{NodeId, 0, false}] ->
      Pattern = {'_', '_'},
      Edges = dets:match_object(State#store_state.edge_table, {Pattern, NodeId}),
      case Edges of
        [{{ParentId, Word}, NodeId}] ->
          remove_edge(ParentId, Word, State),
          ok = dets:delete(State#store_state.node_table, NodeId),
          maybe_clean_path(ParentId, State);
        [] ->
          ok
      end;
    _ ->
      ok
  end.

remove_edge(FromId, Word, State) ->
  ok = dets:delete(State#store_state.edge_table, {FromId, Word}),
  update_edge_count(FromId, -1, State).

%% Setup functions

open_table(Dir, VHost, Type, TableType) ->
  Tab = get_table_name(VHost, Type),
  Path = get_table_path(Dir, VHost, Type),
  AutoSave =
    rabbit_misc:get_env(rabbit_mqtt, retained_message_store_dets_sync_interval, 2000),
  dets:open_file(Tab,
                 [{type, TableType},
                  {file, Path},
                  {ram_file, true},
                  {repair, true},
                  {auto_save, AutoSave}]).
