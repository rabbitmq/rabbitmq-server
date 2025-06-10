%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% Helper functions operating on the Abstract Syntax Tree (AST)
%% as returned by rabbit_jms_selector_parser:parse/1
-module(rabbit_jms_ast).

-export([map_identifiers/2,
         has_binary_identifier/1]).

-type ast() :: tuple().

-export_type([ast/0]).

-spec map_identifiers(fun((binary()) -> atom() | binary()), ast()) ->
    ast().
map_identifiers(Fun, AST) when is_function(Fun, 1) ->
    map_1(AST, Fun).

map_1({identifier, Value}, Fun) when is_binary(Value) ->
    {identifier, Fun(Value)};
map_1({Op, Arg}, Fun) when is_atom(Op) ->
    {Op, map_1(Arg, Fun)};
map_1({Op, Arg1, Arg2}, Fun) when is_atom(Op) ->
    {Op, map_1(Arg1, Fun), map_1(Arg2, Fun)};
map_1({Op, Arg1, Arg2, Arg3}, Fun) when is_atom(Op) ->
    {Op, map_1(Arg1, Fun), map_1(Arg2, Fun), map_1(Arg3, Fun)};
map_1(Other, _Fun) ->
    Other.

-spec has_binary_identifier(ast()) -> boolean().
has_binary_identifier(Ast) ->
    search(fun({identifier, Val}) ->
                   is_binary(Val);
              (_Node) ->
                   false
           end, Ast).

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

has_binary_identifier_test() ->
    false = has_binary_identifier_1("TRUE"),
    true = has_binary_identifier_1("user_key_1 <> 'fake'"),
    false = has_binary_identifier_1("properties.subject = 'fake'"),

    false = has_binary_identifier_1("NOT properties.group-id = 'test'"),
    false = has_binary_identifier_1("properties.group-sequence IS NULL"),
    false = has_binary_identifier_1("properties.group-sequence IS NOT NULL"),
    true = has_binary_identifier_1("NOT user_key = 'test'"),
    true = has_binary_identifier_1("custom_field IS NULL"),

    false = has_binary_identifier_1("properties.group-id = 'g1' AND header.priority > 5"),
    false = has_binary_identifier_1("properties.group-sequence * 10 < 100"),
    false = has_binary_identifier_1("properties.creation-time >= 12345 OR properties.subject = 'test'"),
    true = has_binary_identifier_1("user_key = 'g1' AND header.priority > 5"),
    true = has_binary_identifier_1("header.priority > 5 and user_key = 'g1'"),
    true = has_binary_identifier_1("custom_metric * 10 < 100"),
    true = has_binary_identifier_1("properties.creation-time >= 12345 OR user_data = 'test'"),

    false = has_binary_identifier_1("properties.group-sequence BETWEEN 1 AND 10"),
    true = has_binary_identifier_1("user_score BETWEEN 1 AND 10"),

    false = has_binary_identifier_1("properties.group-id LIKE 'group_%' ESCAPE '!'"),
    true = has_binary_identifier_1("user_tag LIKE 'group_%' ESCAPE '!'"),

    false = has_binary_identifier_1("properties.group-id IN ('g1', 'g2', 'g3')"),
    true = has_binary_identifier_1("user_category IN ('g1', 'g2', 'g3')"),

    false = has_binary_identifier_1(
        "(properties.group-sequence + 1) * 2 <= 100 AND " ++
        "(properties.group-id LIKE 'prod_%' OR header.priority BETWEEN 5 AND 10)"),
    true = has_binary_identifier_1(
        "(properties.group-sequence + 1) * 2 <= 100 AND " ++
        "(user_value LIKE 'prod_%' OR properties.absolute-expiry-time BETWEEN 5 AND 10)"),

    ok.

has_binary_identifier_1(Selector) ->
    {ok, Tokens, _EndLocation} = rabbit_jms_selector_lexer:string(Selector),
    {ok, Ast0} = rabbit_jms_selector_parser:parse(Tokens),
    Ast = map_identifiers(fun rabbit_amqp_util:section_field_name_to_atom/1, Ast0),
    has_binary_identifier(Ast).

-endif.
