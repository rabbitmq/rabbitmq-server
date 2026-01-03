%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% Helper functions operating on the Abstract Syntax Tree (AST)
%% as returned by rabbit_amqp_sql_parser:parse/1
-module(rabbit_amqp_sql_ast).

-export([search/2,
         map/2]).

-type ast() :: tuple().
-export_type([ast/0]).

-spec search(fun((term()) -> boolean()), ast()) -> boolean().
search(Pred, Node) ->
    case Pred(Node) of
        true ->
            true;
        false ->
            case Node of
                {Op, Arg} when is_atom(Op) ->
                    search(Pred, Arg);
                {in, Arg, List} ->
                    search(Pred, Arg) orelse
                    lists:any(fun(N) -> search(Pred, N) end, List);
                {Op, Arg1, Arg2} when is_atom(Op) ->
                    search(Pred, Arg1) orelse
                    search(Pred, Arg2);
                {Op, Arg1, Arg2, Arg3} when is_atom(Op) ->
                    search(Pred, Arg1) orelse
                    search(Pred, Arg2) orelse
                    search(Pred, Arg3);
                _Other ->
                    false
            end
    end.

-spec map(fun((tuple()) -> tuple()), ast()) ->
    ast().
map(Fun, Ast) when is_function(Fun, 1) ->
    map_1(Ast, Fun).

map_1(Pattern, _Fun) when element(1, Pattern) =:= pattern ->
    Pattern;
map_1(Node, Fun) when is_atom(element(1, Node)) ->
    map_2(Fun(Node), Fun);
map_1(Other, _Fun) ->
    Other.

map_2({Op, Arg1}, Fun) ->
    {Op, map_1(Arg1, Fun)};
map_2({in, Arg1, List}, Fun) ->
    {in, map_1(Arg1, Fun), lists:map(fun(N) -> map_1(N, Fun) end, List)};
map_2({Op, Arg1, Arg2}, Fun) ->
    {Op, map_1(Arg1, Fun), map_1(Arg2, Fun)};
map_2({Op, Arg1, Arg2, Arg3}, Fun) ->
    {Op, map_1(Arg1, Fun), map_1(Arg2, Fun), map_1(Arg3, Fun)}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

search_test() ->
    false = search("TRUE"),
    true = search("user_key_1 <> 'fake'"),
    false = search("properties.subject = 'fake'"),

    false = search("NOT properties.group_id = 'test'"),
    false = search("properties.group_sequence IS NULL"),
    false = search("properties.group_sequence IS NOT NULL"),
    true = search("NOT user_key = 'test'"),
    true = search("custom_field IS NULL"),

    false = search("properties.group_id = 'g1' AND header.priority > 5"),
    false = search("properties.group_sequence * 10 < 100"),
    false = search("properties.creation_time >= 12345 OR properties.subject = 'test'"),
    true = search("user_key = 'g1' AND header.priority > 5"),
    true = search("header.priority > 5 AND user_key = 'g1'"),
    true = search("custom_metric * 10 < 100"),
    true = search("properties.creation_time >= 12345 OR user_data = 'test'"),

    false = search("properties.group_id LIKE 'group_%' ESCAPE '!'"),
    true = search("user_tag LIKE 'group_%' ESCAPE '!'"),

    true = search("user_category IN ('g1', 'g2', 'g3')"),
    true = search("p.group_id IN ('g1', user_key, 'g3')"),
    true = search("p.group_id IN ('g1', 'g2', a.user_key)"),
    false = search("p.group_id IN (p.reply_to_group_id, 'g2', 'g3')"),
    false = search("properties.group_id IN ('g1', 'g2', 'g3')"),

    false = search("(properties.group_sequence + 1) * 2 <= 100 AND " ++
                   "(properties.group_id LIKE 'prod_%' OR h.priority > 5)"),
    true = search("(properties.group_sequence + 1) * 2 <= 100 AND " ++
                  "(user_value LIKE 'prod_%' OR p.absolute_expiry_time < 10)"),
    ok.

search(Selector) ->
    {ok, Tokens, _EndLocation} = rabbit_amqp_sql_lexer:string(Selector),
    {ok, Ast} = rabbit_amqp_sql_parser:parse(Tokens),
    search(fun({identifier, Val}) ->
                   is_binary(Val);
              (_Node) ->
                   false
           end, Ast).
-endif.
