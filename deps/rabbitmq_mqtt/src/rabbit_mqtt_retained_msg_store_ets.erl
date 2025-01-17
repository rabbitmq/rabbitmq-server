%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_retained_msg_store_ets).

-behaviour(rabbit_mqtt_retained_msg_store).

-include("rabbit_mqtt_packet.hrl").

-export([new/2, recover/2, insert/3, lookup/2, delete/2, terminate/1]).

% TODO: -define(DEFAULT_MATCH_LIMIT, 1000).

-record(store_state,
        {node_table :: ets:tid(),    % Stores {node_id, edge_count, is_topic}
         edge_table :: ets:tid(),    % Stores {{from_id, word}, to_id}
         msg_table :: ets:tid(),    % Stores {node_id, topic, mqtt_msg}
         root_id :: binary(),     % Root node ID
         dir :: file:filename_all(),
         vhost :: rabbit_types:vhost()}).

-type store_state() :: #store_state{}.

-spec new(file:name_all(), rabbit_types:vhost()) -> store_state().
new(Dir, VHost) ->
  BaseTableName = rabbit_mqtt_util:vhost_name_to_table_name(VHost),

  % Clean up any existing files
  delete_table_files(Dir, VHost),

  % Node table - will store tuples of {node_id, edge_count, is_topic}
  NodeTable = ets:new(append_suffix(BaseTableName, "_nodes"), [set, public]),
  % Edge table - will store {{from_id, word}, to_id}
  EdgeTable = ets:new(append_suffix(BaseTableName, "_edges"), [ordered_set, public]),
  % Topic table - will store {node_id, topic, value}
  % TODO: consider whether, set might still be the best option here
  MsgTable = ets:new(append_suffix(BaseTableName, "_msgs"), [bag, public]),

  RootId = make_node_id(),
  ets:insert(NodeTable, {RootId, 0, false}),

  #store_state{node_table = NodeTable,
               edge_table = EdgeTable,
               msg_table = MsgTable,
               root_id = RootId,
               dir = Dir,
               vhost = VHost}.

-spec recover(file:name_all(), rabbit_types:vhost()) ->
               {ok, store_state(), rabbit_mqtt_retained_msg_store:expire()} |
               {error, uninitialized}.
recover(Dir, VHost) ->
  try
    {ok, NodeTable} = recover_table(Dir, VHost, "nodes"),
    {ok, EdgeTable} = recover_table(Dir, VHost, "edges"),
    {ok, MsgTable} = recover_table(Dir, VHost, "msgs"),

    % Find root node (should be the only node with no incoming edges)
    [{RootId, 0, false}] = ets:match_object(NodeTable, {'$1', 0, false}),

    State =
      #store_state{node_table = NodeTable,
                   edge_table = EdgeTable,
                   msg_table = MsgTable,
                   root_id = RootId,
                   dir = Dir,
                   vhost = VHost},

    delete_table_files(Dir, VHost),

    % ? should we expire all tables here?
    {ok, State, rabbit_mqtt_retained_msg_store:expire(ets, MsgTable)}
  catch
    _:_ ->
      {error, uninitialized}
  end.

-spec terminate(store_state()) -> ok.
terminate(#store_state{node_table = NodeTable,
                       edge_table = EdgeTable,
                       msg_table = MsgTable,
                       dir = Dir,
                       vhost = VHost} =
            _State) ->
  ok =
    ets:tab2file(NodeTable,
                 get_table_path(Dir, VHost, "nodes"),
                 [{extended_info, [object_count]}]),
  ok =
    ets:tab2file(EdgeTable,
                 get_table_path(Dir, VHost, "edges"),
                 [{extended_info, [object_count]}]),
  ok =
    ets:tab2file(MsgTable,
                 get_table_path(Dir, VHost, "msgs"),
                 [{extended_info, [object_count]}]).

-spec insert(topic(), mqtt_msg(), store_state()) -> ok.
insert(Topic, Msg, #store_state{} = State) ->
  Words = split_topic(Topic),
  NodeId = follow_or_create_path(Words, State),
  % Mark node as topic end and store message
  update_node(NodeId, true, State),
  % Replace any existing message for this topic
  ets:delete_object(State#store_state.msg_table, {NodeId, Topic, '_'}),
  ets:insert(State#store_state.msg_table, {NodeId, Topic, Msg}),
  ok.

-spec lookup(topic(), store_state()) -> [mqtt_msg()] | [mqtt_msg_v0()] | undefined.
lookup(Topic, #store_state{} = State) ->
  Words = split_topic(Topic),
  Matches = match_pattern_words(Words, State#store_state.root_id, State, []),
  Values =
    lists:flatmap(fun(NodeId) ->
                     case ets:lookup(State#store_state.msg_table, NodeId) of
                       [{_NodeId, _Topic, Value} | _] -> [Value];
                       [] -> []
                     end
                  end,
                  Matches),
  Values.

-spec delete(topic(), store_state()) -> ok.
delete(Topic, State) ->
  Words = split_topic(Topic),
  case follow_path(Words, State) of
    {ok, NodeId} ->
      % Remove message
      ets:delete_object(State#store_state.msg_table, {NodeId, Topic, '_'}),
      % If no more messages at this node, mark as non-topic
      case ets:lookup(State#store_state.msg_table, NodeId) of
        [] ->
          update_node(NodeId, false, State),
          % Clean up unused path
          maybe_clean_path(NodeId, State);
        _ ->
          ok
      end;
    error ->
      ok
  end,
  ok.

%% Internal setup/teardown functions

append_suffix(TableName, Suffix) ->
  list_to_atom(atom_to_list(TableName) ++ Suffix).

delete_table_files(Dir, VHost) ->
  Types = ["nodes", "edges", "msgs"],
  lists:foreach(fun(Type) -> delete_table(Dir, VHost, Type) end, Types),
  ok.

delete_table(Dir, VHost, Type) ->
  Path = get_table_path(Dir, VHost, Type),
  file:delete(Path).

recover_table(Dir, VHost, Type) ->
  Path = get_table_path(Dir, VHost, Type),
  ets:file2tab(Path).

get_table_path(Dir, VHost, Type) ->
  rabbit_mqtt_util:path_for(Dir, iolist_to_binary([VHost, Type])).

% Internal trie methods
split_topic(Topic) ->
  binary:split(Topic, <<"/">>, [global]).

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
  case ets:lookup(State#store_state.edge_table, Key) of
    [{_Key, ToNode}] ->
      {ok, ToNode};
    [] ->
      error
  end.

get_all_edges(NodeId, State) ->
  % Match all edges from this node
  Pattern = {{NodeId, '_'}, '_'},
  ets:match_object(State#store_state.edge_table, Pattern).

make_node_id() ->
  crypto:strong_rand_bytes(16).

add_edge(FromId, Word, ToId, State) ->
  Key = {FromId, Word},
  EdgeEntry = {Key, ToId},
  ets:insert(State#store_state.edge_table, EdgeEntry),
  NodeEntry = {ToId, 0, false},
  ets:insert(State#store_state.node_table, NodeEntry),
  update_edge_count(FromId, +1, State).

update_edge_count(NodeId, Delta, State) ->
  case ets:lookup(State#store_state.node_table, NodeId) of
    [{NodeId, EdgeCount, IsTopic}] ->
      NewCount = EdgeCount + Delta,
      ets:insert(State#store_state.node_table, {NodeId, NewCount, IsTopic});
    [] ->
      error
  end.

update_node(NodeId, IsTopic, State) ->
  case ets:lookup(State#store_state.node_table, NodeId) of
    [{NodeId, EdgeCount, _OldIsTopic}] ->
      ets:insert(State#store_state.node_table, {NodeId, EdgeCount, IsTopic});
    [] ->
      error
  end.

maybe_clean_path(NodeId, State) ->
  case ets:lookup(State#store_state.node_table, NodeId) of
    [{NodeId, 0, false}] ->
      Pattern = {'_', '_'},
      Edges = ets:match_object(State#store_state.edge_table, {Pattern, NodeId}),
      case Edges of
        [{{ParentId, Word}, NodeId}] ->
          remove_edge(ParentId, Word, State),
          ets:delete(State#store_state.node_table, NodeId),
          maybe_clean_path(ParentId, State);
        [] ->
          ok
      end;
    _ ->
      ok
  end.

remove_edge(FromId, Word, State) ->
  ets:delete(State#store_state.edge_table, {FromId, Word}),
  update_edge_count(FromId, -1, State).
