%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([set/1, delete_all_for_exchange/1, delete/1, match/2]).

%% For testing
-export([clear/0]).

-define(MNESIA_NODE_TABLE, rabbit_topic_trie_node).
-define(MNESIA_EDGE_TABLE, rabbit_topic_trie_edge).
-define(MNESIA_BINDING_TABLE, rabbit_topic_trie_binding).

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Binding) -> ok when
      Binding :: rabbit_types:binding().
%% @doc Sets a topic binding.
%%
%% @private

set(#binding{source = XName, key = RoutingKey, destination = Destination, args = Args}) ->
    rabbit_db:run(
      #{mnesia => fun() -> set_in_mnesia(XName, RoutingKey, Destination, Args) end
       }).

%% -------------------------------------------------------------------
%% delete_all_for_exchange().
%% -------------------------------------------------------------------

-spec delete_all_for_exchange(ExchangeName) -> ok when
      ExchangeName :: rabbit_exchange:name().
%% @doc Deletes all topic bindings for the exchange named `ExchangeName'
%%
%% @private

delete_all_for_exchange(XName) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_all_for_exchange_in_mnesia(XName) end
       }).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete([Binding]) -> ok when
      Binding :: rabbit_types:binding().
%% @doc Deletes all given topic bindings
%%
%% @private

delete(Bs) when is_list(Bs) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(Bs) end
       }).

%% -------------------------------------------------------------------
%% match().
%% -------------------------------------------------------------------

-spec match(ExchangeName, RoutingKey) -> ok when
      ExchangeName :: rabbit_exchange:name(),
      RoutingKey :: binary().
%% @doc Finds the topic binding matching the given exchange and routing key and returns
%% the destination of the binding
%%
%% @returns a list of resources
%%
%% @private

match(XName, RoutingKey) ->
    rabbit_db:run(
      #{mnesia =>
            fun() ->
                    match_in_mnesia(XName, RoutingKey)
            end
       }).

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all topic bindings
%%
%% @private

clear() ->
    rabbit_db:run(
      #{mnesia => fun() -> clear_in_mnesia() end
       }).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_NODE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_EDGE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_BINDING_TABLE),
    ok.

%% Internal
%% --------------------------------------------------------------

split_topic_key(Key) ->
    split_topic_key(Key, [], []).

set_in_mnesia(XName, RoutingKey, Destination, Args) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              FinalNode = follow_down_create(XName, split_topic_key(RoutingKey)),
              trie_add_binding(XName, FinalNode, Destination, Args),
              ok
      end).

delete_all_for_exchange_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              trie_remove_all_nodes(XName),
              trie_remove_all_edges(XName),
              trie_remove_all_bindings(XName),
              ok
      end).

match_in_mnesia(XName, RoutingKey) ->
    Words = split_topic_key(RoutingKey),
    mnesia:async_dirty(fun trie_match/2, [XName, Words]).

trie_remove_all_nodes(X) ->
    remove_all(?MNESIA_NODE_TABLE,
               #topic_trie_node{trie_node = #trie_node{exchange_name = X,
                                                       _             = '_'},
                                _         = '_'}).

trie_remove_all_edges(X) ->
    remove_all(?MNESIA_EDGE_TABLE,
               #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
                                                       _             = '_'},
                                _         = '_'}).

trie_remove_all_bindings(X) ->
    remove_all(?MNESIA_BINDING_TABLE,
               #topic_trie_binding{
                 trie_binding = #trie_binding{exchange_name = X, _ = '_'},
                 _            = '_'}).

remove_all(Table, Pattern) ->
    lists:foreach(fun (R) -> mnesia:delete_object(Table, R, write) end,
                  mnesia:match_object(Table, Pattern, write)).

delete_in_mnesia_tx(Bs) ->
    %% See rabbit_binding:lock_route_tables for the rationale for
    %% taking table locks.
    _ = case Bs of
        [_] -> ok;
        _   -> [mnesia:lock({table, T}, write) ||
                   T <- [?MNESIA_NODE_TABLE,
                         ?MNESIA_EDGE_TABLE,
                         ?MNESIA_BINDING_TABLE]]
    end,
    [case follow_down_get_path(X, split_topic_key(K)) of
         {ok, Path = [{FinalNode, _} | _]} ->
             trie_remove_binding(X, FinalNode, D, Args),
             remove_path_if_empty(X, Path);
         {error, _Node, _RestW} ->
             %% We're trying to remove a binding that no longer exists.
             %% That's unexpected, but shouldn't be a problem.
             ok
     end ||  #binding{source = X, key = K, destination = D, args = Args} <- Bs],
    ok.

delete_in_mnesia(Bs) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> delete_in_mnesia_tx(Bs) end).

split_topic_key(<<>>, [], []) ->
    [];
split_topic_key(<<>>, RevWordAcc, RevResAcc) ->
    lists:reverse([lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<$., Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [], [lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<C:8, Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [C | RevWordAcc], RevResAcc).

trie_match(X, Words) ->
    trie_match(X, root, Words, []).

trie_match(X, Node, [], ResAcc) ->
    trie_match_part(X, Node, "#", fun trie_match_skip_any/4, [],
                    trie_bindings(X, Node) ++ ResAcc);
trie_match(X, Node, [W | RestW] = Words, ResAcc) ->
    lists:foldl(fun ({WArg, MatchFun, RestWArg}, Acc) ->
                        trie_match_part(X, Node, WArg, MatchFun, RestWArg, Acc)
                end, ResAcc, [{W, fun trie_match/4, RestW},
                              {"*", fun trie_match/4, RestW},
                              {"#", fun trie_match_skip_any/4, Words}]).

trie_match_part(X, Node, Search, MatchFun, RestW, ResAcc) ->
    case trie_child(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, ResAcc);
        error          -> ResAcc
    end.

trie_match_skip_any(X, Node, [], ResAcc) ->
    trie_match(X, Node, [], ResAcc);
trie_match_skip_any(X, Node, [_ | RestW] = Words, ResAcc) ->
    trie_match_skip_any(X, Node, RestW,
                        trie_match(X, Node, Words, ResAcc)).

follow_down_create(X, Words) ->
    case follow_down_last_node(X, Words) of
        {ok, FinalNode}      -> FinalNode;
        {error, Node, RestW} -> lists:foldl(
                                  fun (W, CurNode) ->
                                          NewNode = new_node_id(),
                                          trie_add_edge(X, CurNode, NewNode, W),
                                          NewNode
                                  end, Node, RestW)
    end.

new_node_id() ->
    rabbit_guid:gen().

follow_down_last_node(X, Words) ->
    follow_down(X, fun (_, Node, _) -> Node end, root, Words).

follow_down_get_path(X, Words) ->
    follow_down(X, fun (W, Node, PathAcc) -> [{Node, W} | PathAcc] end,
                [{root, none}], Words).

follow_down(X, AccFun, Acc0, Words) ->
    follow_down(X, root, AccFun, Acc0, Words).

follow_down(_X, _CurNode, _AccFun, Acc, []) ->
    {ok, Acc};
follow_down(X, CurNode, AccFun, Acc, Words = [W | RestW]) ->
    case trie_child(X, CurNode, W) of
        {ok, NextNode} -> follow_down(X, NextNode, AccFun,
                                      AccFun(W, NextNode, Acc), RestW);
        error          -> {error, Acc, Words}
    end.

remove_path_if_empty(_, [{root, none}]) ->
    ok;
remove_path_if_empty(X, [{Node, W} | [{Parent, _} | _] = RestPath]) ->
    case mnesia:read(?MNESIA_NODE_TABLE,
                     #trie_node{exchange_name = X, node_id = Node}, write) of
        [] -> trie_remove_edge(X, Parent, Node, W),
              remove_path_if_empty(X, RestPath);
        _  -> ok
    end.

trie_child(X, Node, Word) ->
    case mnesia:read({?MNESIA_EDGE_TABLE,
                      #trie_edge{exchange_name = X,
                                 node_id       = Node,
                                 word          = Word}}) of
        [#topic_trie_edge{node_id = NextNode}] -> {ok, NextNode};
        []                                     -> error
    end.

trie_bindings(X, Node) ->
    MatchHead = #topic_trie_binding{
      trie_binding = #trie_binding{exchange_name = X,
                                   node_id       = Node,
                                   destination   = '$1',
                                   arguments     = '_'}},
    mnesia:select(?MNESIA_BINDING_TABLE, [{MatchHead, [], ['$1']}]).

trie_update_node_counts(X, Node, Field, Delta) ->
    E = case mnesia:read(?MNESIA_NODE_TABLE,
                         #trie_node{exchange_name = X,
                                    node_id       = Node}, write) of
            []   -> #topic_trie_node{trie_node = #trie_node{
                                       exchange_name = X,
                                       node_id       = Node},
                                     edge_count    = 0,
                                     binding_count = 0};
            [E0] -> E0
        end,
    case setelement(Field, E, element(Field, E) + Delta) of
        #topic_trie_node{edge_count = 0, binding_count = 0} ->
            ok = mnesia:delete_object(?MNESIA_NODE_TABLE, E, write);
        EN ->
            ok = mnesia:write(?MNESIA_NODE_TABLE, EN, write)
    end.

trie_add_edge(X, FromNode, ToNode, W) ->
    trie_update_node_counts(X, FromNode, #topic_trie_node.edge_count, +1),
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:write/3).

trie_remove_edge(X, FromNode, ToNode, W) ->
    trie_update_node_counts(X, FromNode, #topic_trie_node.edge_count, -1),
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:delete_object/3).

trie_edge_op(X, FromNode, ToNode, W, Op) ->
    ok = Op(?MNESIA_EDGE_TABLE,
            #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
                                                    node_id       = FromNode,
                                                    word          = W},
                             node_id   = ToNode},
            write).

trie_add_binding(X, Node, D, Args) ->
    trie_update_node_counts(X, Node, #topic_trie_node.binding_count, +1),
    trie_binding_op(X, Node, D, Args, fun mnesia:write/3).

trie_remove_binding(X, Node, D, Args) ->
    trie_update_node_counts(X, Node, #topic_trie_node.binding_count, -1),
    trie_binding_op(X, Node, D, Args, fun mnesia:delete_object/3).

trie_binding_op(X, Node, D, Args, Op) ->
    ok = Op(?MNESIA_BINDING_TABLE,
            #topic_trie_binding{
              trie_binding = #trie_binding{exchange_name = X,
                                           node_id       = Node,
                                           destination   = D,
                                           arguments     = Args}},
            write).
