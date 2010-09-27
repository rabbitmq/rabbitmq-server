%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_exchange_type_topic).
-include_lib("stdlib/include/qlc.hrl").
-include("rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, publish/2]).
-export([validate/1, create/1, recover/2, delete/2, add_binding/2,
         remove_bindings/2, assert_args_equivalence/2]).
-include("rabbit_exchange_type_spec.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type topic"},
                    {mfa,         {rabbit_exchange_type_registry, register,
                                   [<<"topic">>, ?MODULE]}},
                    {requires,    rabbit_exchange_type_registry},
                    {enables,     kernel_ready}]}).

-export([which_matches/2]).

-ifdef(use_specs).

-spec(which_matches/2 ::
        (rabbit_exchange:name(), rabbit_router:routing_key()) ->
          [rabbit_amqqueue:name()]).

-endif.

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"topic">>},
     {description, <<"AMQP topic exchange, as per the AMQP specification">>}].

publish(#exchange{name = X}, Delivery =
        #delivery{message = #basic_message{routing_key = Key}}) ->
    rabbit_router:deliver_by_queue_names(which_matches(X, Key), Delivery).

validate(_X) -> ok.
create(_X) -> ok.
recover(_X, _Bs) -> ok.

delete(#exchange{name = X}, _Bs) ->
    rabbit_misc:execute_mnesia_transaction(fun() -> trie_remove_all_edges(X),
                                                    trie_remove_all_bindings(X)
                                           end),
    ok.

add_binding(_Exchange, #binding{exchange_name = X, key = K, queue_name = Q}) ->
    rabbit_misc:execute_mnesia_transaction(
        fun() -> FinalNode = follow_down_create(X, split_topic_key(K)),
                 trie_add_binding(X, FinalNode, Q)
        end),
    ok.

remove_bindings(_X, Bs) ->
    rabbit_misc:execute_mnesia_transaction(
        fun() -> lists:foreach(fun remove_binding/1, Bs) end),
    ok.

remove_binding(#binding{exchange_name = X, key = K, queue_name = Q}) ->
    Path = follow_down_get_path(X, split_topic_key(K)),
    {FinalNode, _} = hd(Path),
    trie_remove_binding(X, FinalNode, Q),
    remove_path_if_empty(X, Path),
    ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%% NB: This function may return duplicate results in some situations (that's ok)
which_matches(X, Key) ->
    Words = split_topic_key(Key),
    mnesia:async_dirty(fun trie_match/2, [X, Words]).

%%----------------------------------------------------------------------------

trie_match(X, Words) ->
    trie_match(X, root, Words).
trie_match(X, Node, []) ->
    FinalRes = trie_bindings(X, Node),
    HashRes = case trie_child(X, Node, "#") of
                  {ok, HashNode} -> trie_match(X, HashNode, []);
                  error          -> []
              end,
    FinalRes ++ HashRes;
trie_match(X, Node, [W | RestW] = Words) ->
    ExactRes = case trie_child(X, Node, W) of
                   {ok, NextNode} -> trie_match(X, NextNode, RestW);
                   error          -> []
               end,
    StarRes = case trie_child(X, Node, "*") of
                  {ok, StarNode} -> trie_match(X, StarNode, RestW);
                  error          -> []
              end,
    HashRes = case trie_child(X, Node, "#") of
                  {ok, HashNode} -> trie_match_skip_any(X, HashNode, Words);
                  error          -> []
              end,
    ExactRes ++ StarRes ++ HashRes.

trie_match_skip_any(X, Node, []) ->
    trie_match(X, Node, []);
trie_match_skip_any(X, Node, [_ | RestW] = Words) ->
    trie_match(X, Node, Words) ++ trie_match_skip_any(X, Node, RestW).

follow_down(X, Words) ->
    follow_down(X, root, Words).
follow_down(_X, CurNode, []) ->
    {ok, CurNode};
follow_down(X, CurNode, [W | RestW]) ->
    case trie_child(X, CurNode, W) of
        {ok, NextNode} -> follow_down(X, NextNode, RestW);
        error          -> {error, CurNode, [W | RestW]}
    end.

follow_down_create(X, Words) ->
    case follow_down(X, Words) of
        {ok, FinalNode}      -> FinalNode;
        {error, Node, RestW} -> lists:foldl(
                                  fun(W, CurNode) ->
                                         NewNode = new_node(),
                                         trie_add_edge(X, CurNode, NewNode, W),
                                         NewNode
                                  end, Node, RestW)
    end.

follow_down_get_path(X, Words) ->
    follow_down_get_path(X, root, Words, [{root, none}]).
follow_down_get_path(_, _, [], PathAcc) ->
    PathAcc;
follow_down_get_path(X, CurNode, [W | RestW], PathAcc) ->
    {ok, NextNode} = trie_child(X, CurNode, W),
    follow_down_get_path(X, NextNode, RestW, [{NextNode, W} | PathAcc]).

remove_path_if_empty(_, [{root, none}]) ->
    ok;
remove_path_if_empty(X, [{Node, W} | [{Parent, _} | _] = RestPath]) ->
    case trie_has_any_bindings(X, Node) orelse
             trie_has_any_children(X, Node) of
        true  -> ok;
        false -> trie_remove_edge(X, Parent, Node, W),
                 remove_path_if_empty(X, RestPath)
    end.

trie_child(X, Node, Word) ->
    Query = qlc:q([NextNode ||
        #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X1,
                                                node_id = Node1,
                                                word = Word1},
                         node_id = NextNode}
            <- mnesia:table(rabbit_topic_trie_edge),
        X1 == X,
        Node1 == Node,
        Word1 == Word]),
    case qlc:e(Query) of
        [NextNode] -> {ok, NextNode};
        []         -> error
    end.

trie_bindings(X, Node) ->
    MatchHead = #topic_trie_binding{
                    trie_binding = #trie_binding{exchange_name = X,
                                                 node_id = Node,
                                                 queue_name = '$1'}},
    mnesia:select(rabbit_topic_trie_binding, [{MatchHead, [], ['$1']}]).

trie_add_edge(X, FromNode, ToNode, W) ->
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:write/3).
trie_remove_edge(X, FromNode, ToNode, W) ->
    trie_edge_op(X, FromNode, ToNode, W, fun mnesia:delete_object/3).
trie_edge_op(X, FromNode, ToNode, W, Op) ->
    ok = Op(rabbit_topic_trie_edge,
            #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
                                                    node_id = FromNode,
                                                    word = W},
                             node_id = ToNode},
            write).

trie_add_binding(X, Node, Q) ->
    trie_binding_op(X, Node, Q, fun mnesia:write/3).
trie_remove_binding(X, Node, Q) ->
    trie_binding_op(X, Node, Q, fun mnesia:delete_object/3).
trie_binding_op(X, Node, Q, Op) ->
    ok = Op(rabbit_topic_trie_binding,
            #topic_trie_binding{trie_binding = #trie_binding{exchange_name = X,
                                                             node_id = Node,
                                                             queue_name = Q}},
            write).

trie_has_any_children(X, Node) ->
    MatchHead = #topic_trie_edge{trie_edge = #trie_edge{exchange_name = X,
                                                        node_id = Node,
                                                        word = '$1'},
                                 _='_'},
    Select = mnesia:select(rabbit_topic_trie_edge,
                           [{MatchHead, [], ['$1']}], 1, read),
    select_while_no_result(Select) /= '$end_of_table'.

trie_has_any_bindings(X, Node) ->
    MatchHead = #topic_trie_binding{
                    trie_binding = #trie_binding{exchange_name = X,
                                                 node_id = Node,
                                                 queue_name = '$1'},
                    _='_'},
    Select = mnesia:select(rabbit_topic_trie_binding,
                           [{MatchHead, [], ['$1']}], 1, read),
    select_while_no_result(Select) /= '$end_of_table'.

select_while_no_result({[], Cont}) ->
    select_while_no_result(mnesia:select(Cont));
select_while_no_result(Other) ->
    Other.

trie_remove_all_edges(X) ->
    Query = qlc:q([Entry ||
                   Entry = #topic_trie_edge{
                               trie_edge = #trie_edge{exchange_name = X1,
                                                      _='_'},
                               _='_'}
                       <- mnesia:table(rabbit_topic_trie_edge),
                   X1 == X]),
    lists:foreach(
        fun(O) -> mnesia:delete_object(rabbit_topic_trie_edge, O, write) end,
        qlc:e(Query)).

trie_remove_all_bindings(X) ->
    Query = qlc:q([Entry ||
                   Entry = #topic_trie_binding{
                               trie_binding = #trie_binding{exchange_name = X1,
                                                            _='_'},
                               _='_'}
                       <- mnesia:table(rabbit_topic_trie_binding),
                   X1 == X]),
    lists:foreach(
        fun(O) -> mnesia:delete_object(rabbit_topic_trie_binding, O, write) end,
        qlc:e(Query)).

new_node() ->
    rabbit_guid:guid().

split_topic_key(Key) ->
    string:tokens(binary_to_list(Key), ".").
