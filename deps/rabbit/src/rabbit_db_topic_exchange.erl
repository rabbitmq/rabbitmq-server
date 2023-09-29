%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([set/1, delete_all_for_exchange/1, delete/1, match/3]).

%% These functions are used to process mnesia deletion events generated during the
%% migration from mnesia to khepri
-export([
         split_topic_key/1,
         split_topic_key_binary/1,
         trie_binding_to_key/1,
         trie_records_to_key/1
        ]).

%% For testing
-export([clear/0]).

-define(MNESIA_NODE_TABLE, rabbit_topic_trie_node).
-define(MNESIA_EDGE_TABLE, rabbit_topic_trie_edge).
-define(MNESIA_BINDING_TABLE, rabbit_topic_trie_binding).

-type match_result() :: [rabbit_types:binding_destination() |
                         {rabbit_amqqueue:name(), rabbit_types:binding_key()}].

-define(COMPILED_TOPIC_SPLIT_PATTERN, dot_binary_pattern).

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Binding) -> ok when
      Binding :: rabbit_types:binding().
%% @doc Sets a topic binding.
%%
%% @private

set(#binding{source = XName, key = BindingKey, destination = Destination, args = Args}) ->
    rabbit_khepri:handle_fallback(
      #{
        mnesia => fun() -> set_in_mnesia(XName, BindingKey, Destination, Args) end,
        khepri => fun() -> set_in_khepri(XName, BindingKey, Destination, Args) end
       }).

set_in_mnesia(XName, BindingKey, Destination, Args) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              FinalNode = follow_down_create(XName, split_topic_key(BindingKey)),
              trie_add_binding(XName, FinalNode, Destination, Args),
              ok
      end).

set_in_khepri(_XName, _RoutingKey, _Destination, _Args) ->
    ok.

%% -------------------------------------------------------------------
%% delete_all_for_exchange().
%% -------------------------------------------------------------------

-spec delete_all_for_exchange(ExchangeName) -> ok when
      ExchangeName :: rabbit_exchange:name().
%% @doc Deletes all topic bindings for the exchange named `ExchangeName'
%%
%% @private

delete_all_for_exchange(XName) ->
    rabbit_khepri:handle_fallback(
      #{
        mnesia => fun() -> delete_all_for_exchange_in_mnesia(XName) end,
        khepri => fun() -> delete_all_for_exchange_in_khepri(XName) end
       }).

delete_all_for_exchange_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              trie_remove_all_nodes(XName),
              trie_remove_all_edges(XName),
              trie_remove_all_bindings(XName),
              ok
      end).

delete_all_for_exchange_in_khepri(_XName) ->
    ok.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete([Binding]) -> ok when
      Binding :: rabbit_types:binding().
%% @doc Deletes all given topic bindings
%%
%% @private

delete(Bs) when is_list(Bs) ->
    rabbit_khepri:handle_fallback(
      #{
        mnesia => fun() -> delete_in_mnesia(Bs) end,
        khepri => fun() -> delete_in_khepri(Bs) end
       }).

delete_in_mnesia(Bs) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> delete_in_mnesia_tx(Bs) end).

delete_in_khepri(_Bs) ->
    ok.

%% -------------------------------------------------------------------
%% match().
%% -------------------------------------------------------------------

-spec match(rabbit_exchange:name(),
            rabbit_types:routing_key(),
            rabbit_exchange:route_opts()) -> match_result().
%% @doc Finds the topic bindings matching the given exchange and routing key and returns
%% the destination of the bindings potentially with the matched binding key.
%%
%% @returns destinations with matched binding key
%%
%% @private

match(XName, RoutingKey, Opts) ->
    BKeys = maps:get(return_binding_keys, Opts, false),
    rabbit_khepri:handle_fallback(
      #{
        mnesia =>
            fun() ->
                    match_in_mnesia(XName, RoutingKey, BKeys)
            end,
        khepri =>
            fun() ->
                    match_in_khepri(XName, RoutingKey, BKeys)
            end
       }).

match_in_mnesia(XName, RoutingKey, BKeys) ->
    Words = split_topic_key(RoutingKey),
    mnesia:async_dirty(fun trie_match/3, [XName, Words, BKeys]).

match_in_khepri(XName, RoutingKey, BKeys) ->
    Words = split_topic_key_binary(RoutingKey),
    trie_match_in_khepri(XName, Words, BKeys).

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all topic bindings
%%
%% @private

clear() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> clear_in_mnesia() end,
        khepri => fun() -> clear_in_khepri() end
       }).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_NODE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_EDGE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_BINDING_TABLE),
    ok.

clear_in_khepri() ->
    ok.

%% --------------------------------------------------------------
%% split_topic_key().
%% --------------------------------------------------------------

-spec split_topic_key(RoutingKey) -> Words when
      RoutingKey :: binary(),
      Words :: [[byte()]].

split_topic_key(Key) ->
    split_topic_key(Key, [], []).

split_topic_key(<<>>, [], []) ->
    [];
split_topic_key(<<>>, RevWordAcc, RevResAcc) ->
    lists:reverse([lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<$., Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [], [lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<C:8, Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [C | RevWordAcc], RevResAcc).

%% --------------------------------------------------------------
%% split_topic_key_binary().
%% --------------------------------------------------------------

-spec split_topic_key_binary(RoutingKey) -> Words when
      RoutingKey :: binary(),
      Words :: [binary()].

split_topic_key_binary(<<>>) ->
    [];
split_topic_key_binary(RoutingKey) ->
    Pattern =
    case persistent_term:get(?COMPILED_TOPIC_SPLIT_PATTERN, undefined) of
        undefined ->
            P = binary:compile_pattern(<<".">>),
            persistent_term:put(?COMPILED_TOPIC_SPLIT_PATTERN, P),
            P;
        P ->
            P
    end,
    binary:split(RoutingKey, Pattern, [global]).

%% --------------------------------------------------------------
%% trie_binding_to_key().
%% --------------------------------------------------------------

-spec trie_binding_to_key(#topic_trie_binding{}) -> RoutingKey :: binary().

trie_binding_to_key(#topic_trie_binding{trie_binding = #trie_binding{node_id = NodeId}}) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              follow_up_get_path(mnesia, rabbit_topic_trie_edge, NodeId)
      end).

%% --------------------------------------------------------------
%% trie_records_to_key().
%% --------------------------------------------------------------

-spec trie_records_to_key([#topic_trie_binding{}]) ->
          [{#trie_binding{}, RoutingKey :: binary()}].

trie_records_to_key(Records) ->
    Tab = ensure_topic_deletion_ets(),
    TrieBindings = lists:foldl(fun(#topic_trie_binding{} = R, Acc) ->
                                       [R | Acc];
                                  (#topic_trie_edge{} = R, Acc) ->
                                       ets:insert(Tab, R),
                                       Acc;
                                  (_, Acc) ->
                                       Acc
                               end, [], Records),
    List = lists:foldl(
             fun(#topic_trie_binding{trie_binding = #trie_binding{node_id = Node} = TB} = B,
                 Acc) ->
                     case follow_up_get_path(ets, Tab, Node) of
                         {error, not_found} -> [{TB, trie_binding_to_key(B)} | Acc];
                         RK -> [{TB, RK} | Acc]
                     end
             end, [], TrieBindings),
    ets:delete(Tab),
    List.

%% --------------------------------------------------------------
%% Internal
%% --------------------------------------------------------------

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

follow_up_get_path(Mod, Tab, Node) ->
    follow_up_get_path(Mod, Tab, Node, []).

follow_up_get_path(_Mod, _Tab, root, Acc) ->
    Acc;
follow_up_get_path(Mod, Tab, Node, Acc) ->
    MatchHead = #topic_trie_edge{node_id = Node,
                                 trie_edge = '$1'},
    case Mod:select(Tab, [{MatchHead, [], ['$1']}]) of
        [#trie_edge{node_id = PreviousNode,
                    word = Word}] ->
            follow_up_get_path(Mod, Tab, PreviousNode, [Word | Acc]);
        [] ->
            {error, not_found}
    end.

trie_match(X, Words, BKeys) ->
    trie_match(X, root, Words, BKeys, []).

trie_match(X, Node, [], BKeys, ResAcc0) ->
    Destinations = trie_bindings(X, Node, BKeys),
    ResAcc = add_matched(Destinations, BKeys, ResAcc0),
    trie_match_part(X, Node, "#", fun trie_match_skip_any/5, [], BKeys, ResAcc);
trie_match(X, Node, [W | RestW] = Words, BKeys, ResAcc) ->
    lists:foldl(fun ({WArg, MatchFun, RestWArg}, Acc) ->
                        trie_match_part(X, Node, WArg, MatchFun, RestWArg, BKeys, Acc)
                end, ResAcc, [{W, fun trie_match/5, RestW},
                              {"*", fun trie_match/5, RestW},
                              {"#", fun trie_match_skip_any/5, Words}]).

trie_match_part(X, Node, Search, MatchFun, RestW, BKeys, ResAcc) ->
    case trie_child(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, BKeys, ResAcc);
        error          -> ResAcc
    end.

trie_match_skip_any(X, Node, [], BKeys, ResAcc) ->
    trie_match(X, Node, [], BKeys, ResAcc);
trie_match_skip_any(X, Node, [_ | RestW] = Words, BKeys, ResAcc) ->
    trie_match_skip_any(X, Node, RestW, BKeys,
                        trie_match(X, Node, Words, BKeys, ResAcc)).

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

trie_bindings(X, Node, BKeys) ->
    {Args, ActionTerm} = case BKeys of
                             false -> {'_', '$1'};
                             true -> {'$2', {{'$1', '$2'}}}
                         end,
    MatchHead = #topic_trie_binding{
                   trie_binding = #trie_binding{exchange_name = X,
                                                node_id       = Node,
                                                destination   = '$1',
                                                arguments     = Args}},
    mnesia:select(?MNESIA_BINDING_TABLE, [{MatchHead, [], [ActionTerm]}]).

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

-spec add_matched([rabbit_types:binding_destination() |
                   {rabbit_types:binding_destination(), BindingArgs :: list()}],
                  ReturnBindingKeys :: boolean(),
                  match_result()) ->
    match_result().
add_matched(Destinations, false, Acc) ->
    Destinations ++ Acc;
add_matched(DestinationsArgs, true, Acc) ->
    lists:foldl(
      fun({DestQ = #resource{kind = queue}, BindingArgs}, L) ->
              case rabbit_misc:table_lookup(BindingArgs, <<"x-binding-key">>) of
                  {longstr, BKey} ->
                      [{DestQ, BKey} | L];
                  _ ->
                      [DestQ | L]
              end;
         ({DestX, _BindingArgs}, L) ->
              [DestX | L]
      end, Acc, DestinationsArgs).

ensure_topic_deletion_ets() ->
    Tab = rabbit_db_topic_exchange_delete_table,
    case ets:whereis(Tab) of
        undefined ->
            ets:new(Tab, [public, named_table, {keypos, #topic_trie_edge.trie_edge}]);
        Tid ->
            Tid
    end.

%% Khepri topic graph

trie_match_in_khepri(X, Words, BKeys) ->
    trie_match_in_khepri(X, root, Words, BKeys, []).

trie_match_in_khepri(X, Node, [], BKeys, ResAcc0) ->
    Destinations = trie_bindings_in_khepri(X, Node, BKeys),
    ResAcc = add_matched(Destinations, BKeys, ResAcc0),
    trie_match_part_in_khepri(
      X, Node, <<"#">>,
      fun trie_match_skip_any_in_khepri/5, [], BKeys, ResAcc);
trie_match_in_khepri(X, Node, [W | RestW] = Words, BKeys, ResAcc) ->
    lists:foldl(fun ({WArg, MatchFun, RestWArg}, Acc) ->
                        trie_match_part_in_khepri(
                          X, Node, WArg, MatchFun, RestWArg, BKeys, Acc)
                end, ResAcc, [{W, fun trie_match_in_khepri/5, RestW},
                              {<<"*">>, fun trie_match_in_khepri/5, RestW},
                              {<<"#">>,
                               fun trie_match_skip_any_in_khepri/5, Words}]).

trie_match_part_in_khepri(X, Node, Search, MatchFun, RestW, BKeys, ResAcc) ->
    case trie_child_in_khepri(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, BKeys, ResAcc);
        error          -> ResAcc
    end.

trie_match_skip_any_in_khepri(X, Node, [], BKeys, ResAcc) ->
    trie_match_in_khepri(X, Node, [], BKeys, ResAcc);
trie_match_skip_any_in_khepri(X, Node, [_ | RestW] = Words, BKeys, ResAcc) ->
    trie_match_skip_any_in_khepri(
      X, Node, RestW, BKeys,
      trie_match_in_khepri(X, Node, Words, BKeys, ResAcc)).

trie_child_in_khepri(X, Node, Word) ->
    case ets:lookup(rabbit_khepri_topic_trie,
                    #trie_edge{exchange_name = X,
                               node_id       = Node,
                               word          = Word}) of
        [#topic_trie_edge{node_id = NextNode}] -> {ok, NextNode};
        []                                     -> error
    end.

trie_bindings_in_khepri(X, Node, BKeys) ->
    case ets:lookup(rabbit_khepri_topic_trie,
                    #trie_edge{exchange_name = X,
                               node_id       = Node,
                               word          = bindings}) of
        [#topic_trie_edge{node_id = {bindings, Bindings}}] ->
            [case BKeys of
                 true ->
                     {Dest, Args};
                 false ->
                     Dest
             end || #binding{destination = Dest,
                             args        = Args} <- sets:to_list(Bindings)];
        [] ->
            []
    end.

