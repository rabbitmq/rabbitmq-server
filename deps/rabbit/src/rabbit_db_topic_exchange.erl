%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_topic_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([match/3]).

%% Used by Khepri projections and the Mnesia-to-Khepri migration.
-export([split_topic_key_binary/1]).

-define(KHEPRI_PROJECTION_V3, rabbit_khepri_topic_trie_v3).

-type match_result() :: [rabbit_types:binding_destination() |
                         {rabbit_amqqueue:name(), rabbit_types:binding_key()}].

-define(COMPILED_TOPIC_SPLIT_PATTERN, cp_dot).

-spec match(rabbit_exchange:name(),
            rabbit_types:routing_key(),
            rabbit_exchange:route_opts()) -> match_result().
%% @doc Finds the topic bindings matching the given exchange and routing key and returns
%% the destination of the bindings potentially with the matched binding key.
%%
%% @returns destinations with matched binding key
%%
%% @private

match(#resource{virtual_host = VHost, name = XName} = X, RoutingKey, Opts) ->
    BKeys = maps:get(return_binding_keys, Opts, false),
    Words = split_topic_key_binary(RoutingKey),
    case rabbit_khepri:get_effective_topic_binding_projection_version() of
        V when V >= 4 ->
            XSrc = {VHost, XName},
            {TrieTab, BindingTab} = rabbit_khepri:topic_trie_table_names(V),
            Root = case V of
                       4 -> root;
                       _ -> {root, XSrc}
                   end,
            try
                trie_match(XSrc, TrieTab, BindingTab, Root, Words, BKeys, [])
            catch
                error:badarg ->
                    []
            end;
        _ ->
            trie_match_v3(X, Words, BKeys)
    end.

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

%% ==============================================================
%% Trie-based routing
%%
%% Uses two ETS tables:
%%
%% 1. Trie edges table (set): {Key, ChildNodeId, ChildCount}
%%    Key = {XSrc, ParentNodeId, Word}
%%    Navigation: ets:lookup_element/4 for O(1) edge traversal.
%%
%% 2. Leaf bindings table (ordered_set): {{NodeId, BindingKey, Dest}}
%%    Collection: ets:next/2 probes for fanout 0-2 (fast path), then
%%    ets:select/2 with a partially bound key for fanout > 2 (does an
%%    O(log N) seek followed by O(F) range scan).
%%
%% Routing walks the trie (branching on literal word, <<"*">>, <<"#">>)
%% then collects destinations from the bindings table at each matching
%% leaf. This is O(depth * 3) for the trie walk, plus O(log N) per
%% leaf for fanout 0-2, or O(log N + F) per leaf for fanout F > 2.
%% ==============================================================

trie_match(XSrc, TrieTab, BindTab, Node, [], BKeys, Acc0) ->
    Acc1 = trie_bindings(BindTab, Node, BKeys, Acc0),
    trie_match_try(XSrc, TrieTab, BindTab, Node, <<"#">>,
                   fun trie_match_skip_any/7,
                   [], BKeys, Acc1);
trie_match(XSrc, TrieTab, BindTab, Node, [W | RestW] = Words, BKeys, Acc0) ->
    Acc1 = trie_match_try(XSrc, TrieTab, BindTab, Node, W,
                          fun trie_match/7,
                          RestW, BKeys, Acc0),
    Acc2 = trie_match_try(XSrc, TrieTab, BindTab, Node, <<"*">>,
                          fun trie_match/7,
                          RestW, BKeys, Acc1),
    trie_match_try(XSrc, TrieTab, BindTab, Node, <<"#">>,
                   fun trie_match_skip_any/7,
                   Words, BKeys, Acc2).

trie_match_try(XSrc, TrieTab, BindTab, Node, Word, MatchFun, RestW, BKeys, Acc) ->
    case ets:lookup_element(TrieTab, {XSrc, Node, Word}, 2, undefined) of
        undefined ->
            Acc;
        NextNode ->
            MatchFun(XSrc, TrieTab, BindTab, NextNode, RestW, BKeys, Acc)
    end.

trie_match_skip_any(XSrc, TrieTab, BindTab, Node, [], BKeys, Acc) ->
    trie_match(XSrc, TrieTab, BindTab, Node, [], BKeys, Acc);
trie_match_skip_any(XSrc, TrieTab, BindTab, Node, [_ | RestW] = Words, BKeys, Acc) ->
    trie_match_skip_any(
      XSrc, TrieTab, BindTab, Node, RestW, BKeys,
      trie_match(XSrc, TrieTab, BindTab, Node, Words, BKeys, Acc)).

%% Collect all destinations bound at the given trie node.
%%
%% Uses ets:next/2 for up to two elements (fast path for the common
%% fanout 0-2 cases), then switches to ets:select/2 when fanout > 2.
%%
%% ets:select/2 occurs the expensive match spec compilation overhead.
%% For larger fanouts, the cost for compiling the match spec amortises.
%% ets:select/2 occurs an O(log N) seek followed by an O(F) range scan,
%% which is cheaper than F individual ets:next/2 calls
%% (each O(log N) due to CATree fresh-stack allocation).
trie_bindings(BindingTab, NodeId, BKeys, Acc) ->
    StartKey = {NodeId, <<>>, {}},
    case ets:next(BindingTab, StartKey) of
        {NodeId, BKey1, Dest1} = Key1 ->
            case ets:next(BindingTab, Key1) of
                {NodeId, BKey2, Dest2} = Key2 ->
                    case ets:next(BindingTab, Key2) of
                        {NodeId, _, _} ->
                            collect_select(BindingTab, NodeId, BKeys, Acc);
                        _ ->
                            Acc1 = collect_binding(Dest1, BKey1, BKeys, Acc),
                            collect_binding(Dest2, BKey2, BKeys, Acc1)
                    end;
                _ ->
                    collect_binding(Dest1, BKey1, BKeys, Acc)
            end;
        _ ->
            Acc
    end.

collect_binding(#resource{kind = queue} = Dest, BindingKey, true, Acc) ->
    [{Dest, BindingKey} | Acc];
collect_binding(Dest, _BindingKey, _ReturnBindingKeys, Acc) ->
    [Dest | Acc].

collect_select(BindingTab, NodeId, false, Acc) ->
    Dests = ets:select(BindingTab,
                       [{{{NodeId, '_', '$1'}}, [], ['$1']}]),
    Dests ++ Acc;
collect_select(BindingTab, NodeId, true, Acc) ->
    DestsAndBKeys = ets:select(BindingTab,
                               [{{{NodeId, '$1', '$2'}}, [], [{{'$2', '$1'}}]}]),
    format_dest_bkeys(DestsAndBKeys, Acc).

format_dest_bkeys([], Acc) ->
    Acc;
format_dest_bkeys([{#resource{kind = queue} = Dest, BKey} | Rest], Acc) ->
    format_dest_bkeys(Rest, [{Dest, BKey} | Acc]);
format_dest_bkeys([{Dest, _BKey} | Rest], Acc) ->
    format_dest_bkeys(Rest, [Dest | Acc]).

%% ==============================================================
%% Old v3 Khepri topic graph.
%% Delete these *_v3 functions when feature flag
%% topic_binding_projection_v4 becomes required.
%% ==============================================================

trie_match_v3(X, Words, BKeys) ->
    try
        trie_match_v3(X, root, Words, BKeys, [])
    catch
        error:badarg ->
            []
    end.

trie_match_v3(X, Node, [], BKeys, ResAcc0) ->
    Destinations = trie_bindings_v3(X, Node, BKeys),
    ResAcc = add_matched_v3(Destinations, BKeys, ResAcc0),
    trie_match_part_v3(
      X, Node, <<"#">>,
      fun trie_match_skip_any_v3/5, [], BKeys, ResAcc);
trie_match_v3(X, Node, [W | RestW] = Words, BKeys, ResAcc) ->
    lists:foldl(fun ({WArg, MatchFun, RestWArg}, Acc) ->
                        trie_match_part_v3(
                          X, Node, WArg, MatchFun, RestWArg, BKeys, Acc)
                end, ResAcc, [{W, fun trie_match_v3/5, RestW},
                              {<<"*">>, fun trie_match_v3/5, RestW},
                              {<<"#">>,
                               fun trie_match_skip_any_v3/5, Words}]).

trie_match_part_v3(X, Node, Search, MatchFun, RestW, BKeys, ResAcc) ->
    case trie_child_v3(X, Node, Search) of
        {ok, NextNode} -> MatchFun(X, NextNode, RestW, BKeys, ResAcc);
        error          -> ResAcc
    end.

trie_match_skip_any_v3(X, Node, [], BKeys, ResAcc) ->
    trie_match_v3(X, Node, [], BKeys, ResAcc);
trie_match_skip_any_v3(X, Node, [_ | RestW] = Words, BKeys, ResAcc) ->
    trie_match_skip_any_v3(
      X, Node, RestW, BKeys,
      trie_match_v3(X, Node, Words, BKeys, ResAcc)).

trie_child_v3(X, Node, Word) ->
    case ets:lookup(
           ?KHEPRI_PROJECTION_V3,
           #trie_edge{exchange_name = X,
                      node_id       = Node,
                      word          = Word}) of
        [#topic_trie_edge_v2{node_id = NextNode}] -> {ok, NextNode};
        []                                        -> error
    end.

trie_bindings_v3(X, Node, BKeys) ->
    case ets:lookup(
           ?KHEPRI_PROJECTION_V3,
           #trie_edge{exchange_name = X,
                      node_id       = Node,
                      word          = bindings}) of
        [#topic_trie_edge_v2{node_id = {bindings, Bindings}}] ->
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

add_matched_v3(Destinations, false, Acc) ->
    Destinations ++ Acc;
add_matched_v3(DestinationsArgs, true, Acc) ->
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
