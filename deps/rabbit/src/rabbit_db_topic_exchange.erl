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

-define(KHEPRI_PROJECTION, rabbit_khepri_topic_trie_v3).

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

match(XName, RoutingKey, Opts) ->
    BKeys = maps:get(return_binding_keys, Opts, false),
    Words = split_topic_key_binary(RoutingKey),
    trie_match_in_khepri(XName, Words, BKeys).

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
%% Internal
%% --------------------------------------------------------------

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

%% Khepri topic graph

trie_match_in_khepri(X, Words, BKeys) ->
    try
        trie_match_in_khepri(X, root, Words, BKeys, [])
    catch
        error:badarg ->
            []
    end.

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
    case ets:lookup(
           ?KHEPRI_PROJECTION,
           #trie_edge{exchange_name = X,
                      node_id       = Node,
                      word          = Word}) of
        [#topic_trie_edge_v2{node_id = NextNode}] -> {ok, NextNode};
        []                                        -> error
    end.

trie_bindings_in_khepri(X, Node, BKeys) ->
    case ets:lookup(
           ?KHEPRI_PROJECTION,
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
