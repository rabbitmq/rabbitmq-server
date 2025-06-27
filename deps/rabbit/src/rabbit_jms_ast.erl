%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% Helper functions operating on the Abstract Syntax Tree (AST)
%% as returned by rabbit_jms_selector_parser:parse/1
-module(rabbit_jms_ast).

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
map_2({Op, Arg1, Arg2}, Fun) ->
    {Op, map_1(Arg1, Fun), map_1(Arg2, Fun)};
map_2({Op, Arg1, Arg2, Arg3}, Fun) ->
    {Op, map_1(Arg1, Fun), map_1(Arg2, Fun), map_1(Arg3, Fun)}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

has_binary_identifier_test() ->
    false = has_binary_identifier("TRUE"),
    true = has_binary_identifier("user_key_1 <> 'fake'"),
    false = has_binary_identifier("properties.subject = 'fake'"),

    false = has_binary_identifier("NOT properties.group-id = 'test'"),
    false = has_binary_identifier("properties.group-sequence IS NULL"),
    false = has_binary_identifier("properties.group-sequence IS NOT NULL"),
    true = has_binary_identifier("NOT user_key = 'test'"),
    true = has_binary_identifier("custom_field IS NULL"),

    false = has_binary_identifier("properties.group-id = 'g1' AND header.priority > 5"),
    false = has_binary_identifier("properties.group-sequence * 10 < 100"),
    false = has_binary_identifier("properties.creation-time >= 12345 OR properties.subject = 'test'"),
    true = has_binary_identifier("user_key = 'g1' AND header.priority > 5"),
    true = has_binary_identifier("header.priority > 5 and user_key = 'g1'"),
    true = has_binary_identifier("custom_metric * 10 < 100"),
    true = has_binary_identifier("properties.creation-time >= 12345 OR user_data = 'test'"),

    false = has_binary_identifier("properties.group-sequence BETWEEN 1 AND 10"),
    true = has_binary_identifier("user_score BETWEEN 1 AND 10"),

    false = has_binary_identifier("properties.group-id LIKE 'group_%' ESCAPE '!'"),
    true = has_binary_identifier("user_tag LIKE 'group_%' ESCAPE '!'"),

    false = has_binary_identifier("properties.group-id IN ('g1', 'g2', 'g3')"),
    true = has_binary_identifier("user_category IN ('g1', 'g2', 'g3')"),

    false = has_binary_identifier(
              "(properties.group-sequence + 1) * 2 <= 100 AND " ++
              "(properties.group-id LIKE 'prod_%' OR header.priority BETWEEN 5 AND 10)"),
    true = has_binary_identifier(
             "(properties.group-sequence + 1) * 2 <= 100 AND " ++
             "(user_value LIKE 'prod_%' OR properties.absolute-expiry-time BETWEEN 5 AND 10)"),
    ok.

has_binary_identifier(Selector) ->
    {ok, Tokens, _EndLocation} = rabbit_jms_selector_lexer:string(Selector),
    {ok, Ast0} = rabbit_jms_selector_parser:parse(Tokens),
    Ast = map(fun({identifier, Ident}) when is_binary(Ident) ->
                      {identifier, rabbit_amqp_util:section_field_name_to_atom(Ident)};
                 (Node) ->
                      Node
              end, Ast0),
    search(fun({identifier, Val}) ->
                   is_binary(Val);
              (_Node) ->
                   false
           end, Ast).

-endif.
