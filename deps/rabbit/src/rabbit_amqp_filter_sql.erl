%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

-module(rabbit_amqp_filter_sql).
-feature(maybe_expr, enable).

-include_lib("amqp10_common/include/amqp10_filter.hrl").
-include_lib("kernel/include/logger.hrl").

-type parsed_expression() :: {ApplicationProperties :: boolean(),
                              rabbit_amqp_sql_ast:ast()}.

-export_type([parsed_expression/0]).

-export([parse/1,
         eval/2,
         is_control_char/1]).

%% [Filter-Expressions-v1.0 7.1]
%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929316
-define(MAX_EXPRESSION_LENGTH, 4096).
-define(MAX_TOKENS, 200).

-define(DEFAULT_MSG_PRIORITY, 4).

-spec parse(tuple()) ->
    {ok, parsed_expression()} | error.
parse({described, Descriptor, {utf8, SQL}}) ->
    maybe
        ok ?= check_descriptor(Descriptor),
        {ok, String} ?= sql_to_list(SQL),
        ok ?= check_length(String),
        {ok, Tokens} ?= tokenize(String, SQL),
        ok ?= check_token_count(Tokens, SQL),
        {ok, Ast0} ?= parse(Tokens, SQL),
        {ok, Ast} ?= transform_ast(Ast0, SQL),
        AppProps = has_binary_identifier(Ast),
        {ok, {AppProps, Ast}}
    end.

%% Evaluates a parsed SQL expression.
-spec eval(parsed_expression(), mc:state()) -> boolean().
eval({ApplicationProperties, Ast}, Msg) ->
    State = case ApplicationProperties of
                true ->
                    AppProps = mc:routing_headers(Msg, []),
                    {AppProps, Msg};
                false ->
                    Msg
            end,
    %% "a selector that evaluates to true matches;
    %% a selector that evaluates to false or unknown does not match."
    eval0(Ast, State) =:= true.

%% Literals
eval0({Type, Value}, _Msg)
  when Type =:= integer orelse
       Type =:= float orelse
       Type =:= boolean orelse
       Type =:= string orelse
       Type =:= binary ->
    Value;

%% Identifier lookup
eval0({identifier, Key}, State) when is_binary(Key) ->
    {AppProps, _Msg} = State,
    maps:get(Key, AppProps, undefined);
eval0({identifier, FieldName}, State) when is_atom(FieldName) ->
    Msg = case mc:is(State) of
              true ->
                  State;
              false ->
                  {_AppProps, Mc} = State,
                  Mc
          end,
    get_field_value(FieldName, Msg);

%% Function calls
eval0({function, 'UTC', []}, _Msg) ->
    os:system_time(millisecond);

%% Logical operators
%%
%% Table 3-4 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'and', Expr1, Expr2}, Msg) ->
    case eval0(Expr1, Msg) of
        true ->
            case eval0(Expr2, Msg) of
                true -> true;
                false -> false;
                _Unknown -> undefined
            end;
        false ->
            % Short-circuit
            false;
        _Unknown ->
            case eval0(Expr2, Msg) of
                false -> false;
                _ -> undefined
            end
    end;
%% Table 3-5 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'or', Expr1, Expr2}, Msg) ->
    case eval0(Expr1, Msg) of
        true ->
            %% Short-circuit
            true;
        false ->
            case eval0(Expr2, Msg) of
                true -> true;
                false -> false;
                _Unknown -> undefined
            end;
        _Unknown ->
            case eval0(Expr2, Msg) of
                true -> true;
                _ -> undefined
            end
    end;
%% Table 3-6 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'not', Expr}, Msg) ->
    case eval0(Expr, Msg) of
        true -> false;
        false -> true;
        _Unknown -> undefined
    end;

%% Comparison operators
eval0({Op, Expr1, Expr2}, Msg)
  when Op =:= '='  orelse
       Op =:= '<>' orelse
       Op =:= '>'  orelse
       Op =:= '<'  orelse
       Op =:= '>=' orelse
       Op =:= '<=' ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));

%% Arithmetic operators
eval0({Op, Expr1, Expr2}, Msg)
  when Op =:= '+' orelse
       Op =:= '-' orelse
       Op =:= '*' orelse
       Op =:= '/' orelse
       Op =:= '%' ->
    arithmetic(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));

%% Unary operators
eval0({unary_plus, Expr}, Msg) ->
    Val = eval0(Expr, Msg),
    case is_number(Val) of
        true -> Val;
        false -> undefined
    end;
eval0({unary_minus, Expr}, Msg) ->
    Val = eval0(Expr, Msg),
    case is_number(Val) of
        true -> -Val;
        false -> undefined
    end;

%% Special operators
eval0({'in', Expr, ExprList}, Msg) ->
    Value = eval0(Expr, Msg),
    is_in(Value, ExprList, Msg);

eval0({'is_null', Expr}, Msg) ->
    eval0(Expr, Msg) =:= undefined;

eval0({'like', Expr, {pattern, Pattern}}, Msg) ->
    Subject = eval0(Expr, Msg),
    case is_binary(Subject) of
        true ->
            like(Subject, Pattern);
        false ->
            %% "If identifier of a LIKE or NOT LIKE operation is NULL,
            %% the value of the operation is unknown."
            undefined
    end.

%% "Comparison or arithmetic with an unknown value always yields an unknown value."
compare(_Op, Left, Right) when Left =:= undefined orelse Right =:= undefined ->
    undefined;
%% "Only like type values can be compared. One exception is that it is valid to
%% compare exact numeric values and approximate numeric values"
compare('=', Left, Right) ->
    Left == Right;
compare('<>', Left, Right) ->
    Left /= Right;
compare('>', Left, Right) when is_number(Left) andalso is_number(Right) orelse
                               is_binary(Left) andalso is_binary(Right) ->
    Left > Right;
compare('<', Left, Right) when is_number(Left) andalso is_number(Right) orelse
                               is_binary(Left) andalso is_binary(Right) ->
    Left < Right;
compare('>=', Left, Right) when is_number(Left) andalso is_number(Right) orelse
                                is_binary(Left) andalso is_binary(Right) ->
    Left >= Right;
compare('<=', Left, Right) when is_number(Left) andalso is_number(Right) orelse
                                is_binary(Left) andalso is_binary(Right) ->
    Left =< Right;
compare(_, _, _) ->
    %% "If the comparison of non-like type values is attempted,
    %% the value of the operation is false."
    false.

arithmetic(_Op, Left, Right) when Left =:= undefined orelse Right =:= undefined ->
    undefined;
arithmetic('+', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left + Right;
arithmetic('-', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left - Right;
arithmetic('*', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left * Right;
arithmetic('/', Left, Right) when is_number(Left) andalso is_number(Right) andalso Right /= 0 ->
    Left / Right;
arithmetic('%', Left, Right) when is_integer(Left) andalso is_integer(Right) andalso Right =/= 0 ->
    Left rem Right;
arithmetic(_, _, _) ->
    undefined.

is_in(undefined, _, _) ->
    %% "If identifier of an IN or NOT IN operation is NULL,
    %% the value of the operation is unknown."
    undefined;
is_in(Value, ExprList, Msg) ->
    IsEqual = fun(Expr) -> eval0(Expr, Msg) == Value end,
    lists:any(IsEqual, ExprList).

like(Subject, {exact, Pattern}) ->
    Subject =:= Pattern;
like(Subject, {prefix, PrefixSize, Prefix}) ->
    case Subject of
        <<Prefix:PrefixSize/binary, _/binary>> ->
            true;
        _ ->
            false
    end;
like(Subject, {suffix, SuffixSize, Suffix}) ->
    case Subject of
        <<_:(byte_size(Subject) - SuffixSize)/binary, Suffix:SuffixSize/binary>> ->
            true;
        _ ->
            false
    end;
like(Subject,{{prefix, PrefixSize, _} = Prefix,
              {suffix, SuffixSize, _} = Suffix}) ->
    byte_size(Subject) >= PrefixSize + SuffixSize andalso
    like(Subject, Prefix) andalso
    like(Subject, Suffix);
like(Subject, CompiledRe)
  when element(1, CompiledRe) =:= re_pattern ->
    try re:run(Subject, CompiledRe, [{capture, none}]) of
        match ->
            true;
        _ ->
            false
    catch error:badarg ->
              %% This branch is hit if Subject is not a UTF-8 string.
              undefined
    end.

get_field_value(priority, Msg) ->
    case mc:priority(Msg) of
        undefined ->
            ?DEFAULT_MSG_PRIORITY;
        P ->
            P
    end;
get_field_value(creation_time, Msg) ->
    mc:timestamp(Msg);
get_field_value(Name, Msg) ->
    case mc:property(Name, Msg) of
        {_Type, Val} ->
            Val;
        undefined ->
            undefined
    end.

check_descriptor({symbol, ?DESCRIPTOR_NAME_SQL_FILTER}) ->
    ok;
check_descriptor({ulong, ?DESCRIPTOR_CODE_SQL_FILTER}) ->
    ok;
check_descriptor(_) ->
    error.

sql_to_list(SQL) ->
    case unicode:characters_to_list(SQL) of
        String when is_list(String) ->
            {ok, String};
        Error ->
            ?LOG_WARNING("SQL expression ~p is not UTF-8 encoded: ~p",
                         [SQL, Error]),
            error
    end.

check_length(String) ->
    Len = length(String),
    case Len =< ?MAX_EXPRESSION_LENGTH of
        true ->
            ok;
        false ->
            ?LOG_WARNING("SQL expression length ~b exceeds maximum length ~b",
                         [Len, ?MAX_EXPRESSION_LENGTH]),
            error
    end.

tokenize(String, SQL) ->
    case rabbit_amqp_sql_lexer:string(String) of
        {ok, Tokens, _EndLocation} ->
            {ok, Tokens};
        {error, {_Line, _Mod, ErrDescriptor}, _Location} ->
            ?LOG_WARNING("failed to scan SQL expression '~ts': ~tp",
                         [SQL, ErrDescriptor]),
            error
    end.

check_token_count(Tokens, SQL)
  when length(Tokens) > ?MAX_TOKENS ->
    ?LOG_WARNING("SQL expression '~ts' with ~b tokens exceeds token limit ~b",
                 [SQL, length(Tokens), ?MAX_TOKENS]),
    error;
check_token_count(_, _) ->
    ok.

parse(Tokens, SQL) ->
    case rabbit_amqp_sql_parser:parse(Tokens) of
        {error, Reason} ->
            ?LOG_WARNING("failed to parse SQL expression '~ts': ~p",
                         [SQL, Reason]),
            error;
        Ok ->
            Ok
    end.

transform_ast(Ast0, SQL) ->
    try rabbit_amqp_sql_ast:map(fun({'like', _Id, _Pat, _Esc} = Node) ->
                                        transform_pattern_node(Node);
                                   (Node) ->
                                        Node
                                end, Ast0) of
        Ast ->
            {ok, Ast}
    catch {invalid_pattern, Reason} ->
              ?LOG_WARNING(
                 "failed to parse LIKE pattern for SQL expression ~tp: ~tp",
                 [SQL, Reason]),
              error
    end.

has_binary_identifier(Ast) ->
    rabbit_amqp_sql_ast:search(fun({identifier, Val}) ->
                                       is_binary(Val);
                                  (_Node) ->
                                       false
                               end, Ast).

%% If the Pattern contains no wildcard or a single % wildcard,
%% we will evaluate messages using Erlang pattern matching since
%% that's faster than evaluating compiled regexes.
transform_pattern_node({Op, Ident, Pattern, Escape}) ->
    Pat = transform_pattern(Pattern, Escape),
    {Op, Ident, {pattern, Pat}}.

transform_pattern(Pattern, Escape) ->
    case scan_wildcards(Pattern, Escape) of
        {none, Chars} ->
            {exact, unicode:characters_to_binary(Chars)};
        {single_percent, Chars, PercentPos} ->
            single_percent(Chars, PercentPos);
        regex ->
            Re = pattern_to_regex(Pattern, Escape, []),
            case re:compile("^" ++ Re ++ "$", [unicode]) of
                {ok, CompiledRe} ->
                    CompiledRe;
                {error, Reason} ->
                    throw({invalid_pattern, Reason})
            end
    end.

scan_wildcards(Pattern, Escape) ->
    scan_wildcards_1(Pattern, Escape, [], -1).

scan_wildcards_1([], _, Acc, -1) ->
    {none, lists:reverse(Acc)};
scan_wildcards_1([], _, Acc, PctPos) ->
    {single_percent, lists:reverse(Acc), PctPos};
scan_wildcards_1([EscapeChar | Rest], EscapeChar, Acc, PctPos) ->
    case Rest of
        [] ->
            throw({invalid_pattern, invalid_escape_at_end});
        [NextChar | Rest1] ->
            scan_wildcards_1(Rest1, EscapeChar, [check_char(NextChar) | Acc], PctPos)
    end;
scan_wildcards_1([$_ | _Rest], _, _, _) ->
    regex;
scan_wildcards_1([$% | Rest], Escape, Acc, -1) ->
    %% This is the 1st % character.
    Pos = length(Acc),
    scan_wildcards_1(Rest, Escape, Acc, Pos);
scan_wildcards_1([$% | _], _, _, _) ->
    %% This is the 2nd % character.
    regex;
scan_wildcards_1([Char | Rest], Escape, Acc, PctPos) ->
    scan_wildcards_1(Rest, Escape, [check_char(Char) | Acc], PctPos).

single_percent(Chars, 0) ->
    %% % at start - suffix match
    Bin = unicode:characters_to_binary(Chars),
    {suffix, byte_size(Bin), Bin};
single_percent(Chars, Pos) when length(Chars) =:= Pos ->
    %% % at end - prefix match
    Bin = unicode:characters_to_binary(Chars),
    {prefix, byte_size(Bin), Bin};
single_percent(Chars, Pos) ->
    %% % in middle - prefix and suffix match
    {Prefix, Suffix} = lists:split(Pos, Chars),
    PrefixBin = unicode:characters_to_binary(Prefix),
    SuffixBin = unicode:characters_to_binary(Suffix),
    {{prefix, byte_size(PrefixBin), PrefixBin},
     {suffix, byte_size(SuffixBin), SuffixBin}}.

pattern_to_regex([], _Escape, Acc) ->
    lists:reverse(Acc);
pattern_to_regex([EscapeChar | Rest], EscapeChar, Acc) ->
    case Rest of
        [] ->
            throw({invalid_pattern, invalid_escape_at_end});
        [NextChar | Rest1] ->
            pattern_to_regex(Rest1, EscapeChar, escape_regex_char(NextChar) ++ Acc)
    end;
pattern_to_regex([$% | Rest], Escape, Acc) ->
    %% % matches any sequence of characters (0 or more)
    %% "The wildcard matching MUST consume as few characters as possible." (non-greedy)
    pattern_to_regex(Rest, Escape, [$?, $*, $. | Acc]);
pattern_to_regex([$_ | Rest], Escape, Acc) ->
    %% _ matches exactly one character
    pattern_to_regex(Rest, Escape, [$. | Acc]);
pattern_to_regex([Char | Rest], Escape, Acc) ->
    pattern_to_regex(Rest, Escape, escape_regex_char(Char) ++ Acc).

%% Escape user provided characters that have special meaning in Erlang regex.
escape_regex_char(Char0) ->
    Char = check_char(Char0),
    case lists:member(Char, ".\\|()[]{}^$*+?#") of
        true ->
            [Char, $\\];
        false ->
            [Char]
    end.

%% Let's disallow control characters in the user provided pattern.
check_char(C) ->
    case is_control_char(C) of
        true ->
            throw({invalid_pattern, {illegal_control_character, C}});
        false ->
            C
    end.

is_control_char(C) when C < 32 orelse C =:= 127 ->
    true;
is_control_char(_) ->
    false.
