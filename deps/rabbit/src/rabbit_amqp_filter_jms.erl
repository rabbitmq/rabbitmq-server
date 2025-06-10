%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

-module(rabbit_amqp_filter_jms).
-feature(maybe_expr, enable).

-include_lib("amqp10_common/include/amqp10_filter.hrl").

-type parsed_expression() :: {ApplicationProperties :: boolean(),
                              rabbit_jms_ast:ast()}.

-export_type([parsed_expression/0]).

-export([parse/1,
         eval/2]).

%% [filtex-v1.0-wd09 7.1]
-define(MAX_EXPRESSION_LENGTH, 4096).
-define(MAX_TOKENS, 200).

%% defined in both AMQP and JMS
-define(DEFAULT_MSG_PRIORITY, 4).

-define(IS_CONTROL_CHAR(C), C < 32 orelse C =:= 127).

-spec parse(tuple()) ->
    {ok, parsed_expression()} | error.
parse({described, Descriptor, {utf8, JmsSelector}}) ->
    maybe
        ok ?= check_descriptor(Descriptor),
        {ok, String} ?= jms_selector_to_list(JmsSelector),
        ok ?= check_length(String),
        {ok, Tokens} ?= tokenize(String, JmsSelector),
        ok ?= check_token_count(Tokens, JmsSelector),
        {ok, Ast0} ?= parse(Tokens, JmsSelector),
        {ok, Ast} ?= transform_ast(Ast0, JmsSelector),
        AppProps = has_binary_identifier(Ast),
        {ok, {AppProps, Ast}}
    end.

%% Evaluates a parsed JMS message selector expression.
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
       Type =:= string orelse
       Type =:= boolean ->
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
       Op =:= '/' ->
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
eval0({'between', Expr, From, To}, Msg) ->
    Value = eval0(Expr, Msg),
    FromVal = eval0(From, Msg),
    ToVal = eval0(To, Msg),
    between(Value, FromVal, ToVal);

eval0({'in', Expr, ValueList}, Msg) ->
    Value = eval0(Expr, Msg),
    is_in(Value, ValueList);

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
%% "Only like type values can be compared.
%% One exception is that it is valid to compare exact numeric values and approximate numeric values.
%% String and Boolean comparison is restricted to = and <>."
compare('=', Left, Right) ->
    Left == Right;
compare('<>', Left, Right) ->
    Left /= Right;
compare('>', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left > Right;
compare('<', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left < Right;
compare('>=', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left >= Right;
compare('<=', Left, Right) when is_number(Left) andalso is_number(Right) ->
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
arithmetic(_, _, _) ->
    undefined.

between(Value, From, To)
  when Value =:= undefined orelse
       From =:= undefined orelse
       To =:= undefined ->
    undefined;
between(Value, From, To)
  when is_number(Value) andalso
       is_number(From) andalso
       is_number(To) ->
    From =< Value andalso Value =< To;
between(_, _, _) ->
    %% BETWEEN requires arithmetic expressions
    %% "a string cannot be used in an arithmetic expression"
    false.

is_in(undefined, _) ->
    %% "If identifier of an IN or NOT IN operation is NULL,
    %% the value of the operation is unknown."
    undefined;
is_in(Value, List) ->
    lists:member(Value, List).

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

check_descriptor({symbol, ?DESCRIPTOR_NAME_SELECTOR_FILTER}) ->
    ok;
check_descriptor({ulong, ?DESCRIPTOR_CODE_SELECTOR_FILTER}) ->
    ok;
check_descriptor(_) ->
    error.

jms_selector_to_list(JmsSelector) ->
    case unicode:characters_to_list(JmsSelector) of
        String when is_list(String) ->
            {ok, String};
        Error ->
            rabbit_log:warning("JMS message selector ~p is not UTF-8 encoded: ~p",
                               [JmsSelector, Error]),
            error
    end.

check_length(String)
  when length(String) > ?MAX_EXPRESSION_LENGTH ->
    rabbit_log:warning("JMS message selector length ~b exceeds maximum length ~b",
                       [length(String), ?MAX_EXPRESSION_LENGTH]),
    error;
check_length(_) ->
    ok.

tokenize(String, JmsSelector) ->
    case rabbit_jms_selector_lexer:string(String) of
        {ok, Tokens, _EndLocation} ->
            {ok, Tokens};
        {error, {_Line, _Mod, ErrDescriptor}, _Location} ->
            rabbit_log:warning("failed to scan JMS message selector '~ts': ~tp",
                               [JmsSelector, ErrDescriptor]),
            error
    end.

check_token_count(Tokens, JmsSelector)
  when length(Tokens) > ?MAX_TOKENS ->
    rabbit_log:warning("JMS message selector '~ts' with ~b tokens exceeds token limit ~b",
                       [JmsSelector, length(Tokens), ?MAX_TOKENS]),
    error;
check_token_count(_, _) ->
    ok.

parse(Tokens, JmsSelector) ->
    case rabbit_jms_selector_parser:parse(Tokens) of
        {error, Reason} ->
            rabbit_log:warning("failed to parse JMS message selector '~ts': ~p",
                               [JmsSelector, Reason]),
            error;
        Ok ->
            Ok
    end.

transform_ast(Ast0, JmsSelector) ->
    try rabbit_jms_ast:map(
          fun({identifier, Ident})
                when is_binary(Ident) ->
                  {identifier, rabbit_amqp_util:section_field_name_to_atom(Ident)};
             ({'like', _Ident, _Pattern, _Escape} = Node) ->
                  transform_pattern_node(Node);
             (Node) ->
                  Node
          end, Ast0) of
        Ast ->
            {ok, Ast}
    catch {unsupported_field, Name} ->
              rabbit_log:warning(
                "identifier ~ts in JMS message selector ~tp is unsupported",
                [Name, JmsSelector]),
              error;
          {invalid_pattern, Reason} ->
              rabbit_log:warning(
                "failed to parse LIKE pattern for JMS message selector ~tp: ~tp",
                [JmsSelector, Reason]),
              error
    end.

has_binary_identifier(Ast) ->
    rabbit_jms_ast:search(fun({identifier, Val}) ->
                                  is_binary(Val);
                             (_Node) ->
                                  false
                          end, Ast).

%% If the Pattern contains no wildcard or a single % wildcard,
%% we will optimise message evaluation by using Erlang pattern matching.
%% Otherwise, we will match with a regex. Even though we compile regexes,
%% they are slower compared to Erlang pattern matching.
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
            Re = jms_pattern_to_regex(Pattern, Escape, []),
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

jms_pattern_to_regex([], _Escape, Acc) ->
    lists:reverse(Acc);
jms_pattern_to_regex([EscapeChar | Rest], EscapeChar, Acc) ->
    case Rest of
        [] ->
            throw({invalid_pattern, invalid_escape_at_end});
        [NextChar | Rest1] ->
            jms_pattern_to_regex(Rest1, EscapeChar, escape_regex_char(NextChar) ++ Acc)
    end;
jms_pattern_to_regex([$% | Rest], Escape, Acc) ->
    %% % matches any sequence of characters (0 or more)
    jms_pattern_to_regex(Rest, Escape, [$*, $. | Acc]);
jms_pattern_to_regex([$_ | Rest], Escape, Acc) ->
    %% _ matches exactly one character
    jms_pattern_to_regex(Rest, Escape, [$. | Acc]);
jms_pattern_to_regex([Char | Rest], Escape, Acc) ->
    jms_pattern_to_regex(Rest, Escape, escape_regex_char(Char) ++ Acc).

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
check_char(C) when ?IS_CONTROL_CHAR(C) ->
    throw({invalid_pattern, {prohibited_control_character, C}});
check_char(C) ->
    C.
