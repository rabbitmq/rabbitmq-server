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
-export([split_topic_key_binary/1,
         split_topic_key/1]).

-define(TOPIC_TRIE_PROJECTION, rabbit_khepri_topic_trie_v4).
-define(TOPIC_BINDING_PROJECTION, rabbit_khepri_topic_binding_v4).

-type match_result() :: [rabbit_types:binding_destination() |
                         {rabbit_amqqueue:name(), rabbit_types:binding_key()}].

-define(COMPILED_TOPIC_SPLIT_PATTERN, cp_dot).

-spec match(rabbit_exchange:name(),
            rabbit_types:routing_key(),
            rabbit_exchange:route_opts()) -> match_result().
match(#resource{virtual_host = VHost, name = XName}, RoutingKey, Opts) ->
    BKeys = maps:get(return_binding_keys, Opts, false),
    Words = split_topic_key_binary(RoutingKey),
    try
        trie_match({VHost, XName}, root, Words, BKeys, [])
    catch
        error:badarg ->
            []
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

trie_match(XSrc, Node, [], BKeys, Acc0) ->
    Acc1 = trie_bindings(Node, BKeys, Acc0),
    trie_match_try(XSrc, Node, <<"#">>,
                   fun trie_match_skip_any/5,
                   [], BKeys, Acc1);
trie_match(XSrc, Node, [W | RestW] = Words, BKeys, Acc0) ->
    Acc1 = trie_match_try(XSrc, Node, W,
                          fun trie_match/5,
                          RestW, BKeys, Acc0),
    Acc2 = trie_match_try(XSrc, Node, <<"*">>,
                          fun trie_match/5,
                          RestW, BKeys, Acc1),
    trie_match_try(XSrc, Node, <<"#">>,
                   fun trie_match_skip_any/5,
                   Words, BKeys, Acc2).

trie_match_try(XSrc, Node, Word, MatchFun, RestW, BKeys, Acc) ->
    case ets:lookup_element(?TOPIC_TRIE_PROJECTION,
                            {XSrc, Node, Word}, 2, undefined) of
        undefined ->
            Acc;
        NextNode ->
            MatchFun(XSrc, NextNode, RestW, BKeys, Acc)
    end.

trie_match_skip_any(XSrc, Node, [], BKeys, Acc) ->
    trie_match(XSrc, Node, [], BKeys, Acc);
trie_match_skip_any(XSrc, Node, [_ | RestW] = Words, BKeys, Acc) ->
    trie_match_skip_any(
      XSrc, Node, RestW, BKeys,
      trie_match(XSrc, Node, Words, BKeys, Acc)).

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
trie_bindings(NodeId, BKeys, Acc) ->
    StartKey = {NodeId, <<>>, {}},
    case ets:next(?TOPIC_BINDING_PROJECTION, StartKey) of
        {NodeId, BKey1, Dest1} = Key1 ->
            case ets:next(?TOPIC_BINDING_PROJECTION, Key1) of
                {NodeId, BKey2, Dest2} = Key2 ->
                    case ets:next(?TOPIC_BINDING_PROJECTION, Key2) of
                        {NodeId, _, _} ->
                            collect_select(NodeId, BKeys, Acc);
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

collect_select(NodeId, false, Acc) ->
    Dests = ets:select(?TOPIC_BINDING_PROJECTION,
                       [{{{NodeId, '_', '$1'}}, [], ['$1']}]),
    Dests ++ Acc;
collect_select(NodeId, true, Acc) ->
    DestsAndBKeys = ets:select(?TOPIC_BINDING_PROJECTION,
                               [{{{NodeId, '$1', '$2'}}, [], [{{'$2', '$1'}}]}]),
    format_dest_bkeys(DestsAndBKeys, Acc).

format_dest_bkeys([], Acc) ->
    Acc;
format_dest_bkeys([{#resource{kind = queue} = Dest, BKey} | Rest], Acc) ->
    format_dest_bkeys(Rest, [{Dest, BKey} | Acc]);
format_dest_bkeys([{Dest, _BKey} | Rest], Acc) ->
    format_dest_bkeys(Rest, [Dest | Acc]).
