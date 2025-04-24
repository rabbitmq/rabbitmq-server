%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% Evaluates parsed JMS message selector expressions.
-module(rabbit_fifo_filter_jms).

-export([eval/2]).

%% Evaluates a parsed JMS message selector expression against message metadata.
-spec eval(term(), #{atom() | binary() => atom() | binary() | number()}) ->
    boolean().
eval(Expression, Headers) ->
    case eval0(Expression, Headers) of
        true -> true;
        _ -> false
    end.

%% Literals
eval0({Type, Value}, _Headers)
  when Type =:= integer orelse
       Type =:= float orelse
       Type =:= boolean orelse
       Type =:= string ->
    Value;

%% Identifier lookup
eval0({identifier, Name}, Headers) ->
    maps:get(Name, Headers, undefined);

%% Logical operators
%%
%% Table 3-4 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'and', Expr1, Expr2}, Headers) ->
    case eval0(Expr1, Headers) of
        true ->
            case eval0(Expr2, Headers) of
                true -> true;
                false -> false;
                _ -> undefined
            end;
        false ->
            % Short-circuit
            false;
        undefined ->
            case eval0(Expr2, Headers) of
                false -> false;
                _ -> undefined
            end;
        _ ->
            undefined
    end;
%% Table 3-5 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'or', Expr1, Expr2}, Headers) ->
    case eval0(Expr1, Headers) of
        true ->
            %% Short-circuit
            true;
        false ->
            case eval0(Expr2, Headers) of
                true -> true;
                false -> false;
                _ -> undefined
            end;
        undefined ->
            case eval0(Expr2, Headers) of
                true -> true;
                _ -> undefined
            end;
        _ ->
            undefined
    end;
%% Table 3-6 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'not', Expr}, Headers) ->
    case eval0(Expr, Headers) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

%% Comparison operators
eval0({'=' = Op, Expr1, Expr2}, Headers) ->
    compare(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'<>' = Op, Expr1, Expr2}, Headers) ->
    compare(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'>' = Op, Expr1, Expr2}, Headers) ->
    compare(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'<' = Op, Expr1, Expr2}, Headers) ->
    compare(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'>=' = Op, Expr1, Expr2}, Headers) ->
    compare(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'<=' = Op, Expr1, Expr2}, Headers) ->
    compare(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));

%% Arithmetic operators
eval0({'+' = Op, Expr1, Expr2}, Headers) ->
    arithmetic(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'-' = Op, Expr1, Expr2}, Headers) ->
    arithmetic(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'*' = Op, Expr1, Expr2}, Headers) ->
    arithmetic(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'/' = Op, Expr1, Expr2}, Headers) ->
    arithmetic(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));

%% Unary operators
eval0({unary_plus, Expr}, Headers) ->
    Val = eval0(Expr, Headers),
    case is_number(Val) of
        true -> Val;
        false -> undefined
    end;
eval0({unary_minus, Expr}, Headers) ->
    Val = eval0(Expr, Headers),
    case is_number(Val) of
        true -> -Val;
        false -> undefined
    end;

%% Special operators
eval0({'between', Expr, From, To}, Headers) ->
    Value = eval0(Expr, Headers),
    FromVal = eval0(From, Headers),
    ToVal = eval0(To, Headers),
    between(Value, FromVal, ToVal);

eval0({'not_between', Expr, From, To}, Headers) ->
    case eval0({'between', Expr, From, To}, Headers) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'in', Expr, ValueList}, Headers) ->
    Value = eval0(Expr, Headers),
    is_in(Value, [eval0(Item, Headers) || Item <- ValueList]);

eval0({'not_in', Expr, ValueList}, Headers) ->
    case eval0({'in', Expr, ValueList}, Headers) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'like', Expr, Pattern, Escape}, Headers) ->
    Value = eval0(Expr, Headers),
    PatternVal = eval0(Pattern, Headers),
    EscapeVal = case Escape of
                    no_escape -> no_escape;
                    _ -> eval0(Escape, Headers)
                end,
    is_like(Value, PatternVal, EscapeVal);

eval0({'not_like', Expr, Pattern, Escape}, Headers) ->
    case eval0({'like', Expr, Pattern, Escape}, Headers) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'is_null', Expr}, Headers) ->
    eval0(Expr, Headers) =:= undefined;

eval0({'is_not_null', Expr}, Headers) ->
    eval0(Expr, Headers) =/= undefined;

%% Default case for unknown expressions
eval0(Value, _Headers)
  when is_binary(Value) orelse
       is_number(Value) orelse
       is_boolean(Value) ->
    Value;
eval0(_, _) ->
    undefined.

%% Helper functions

%% "Comparison or arithmetic with an unknown value always yields an unknown value."
compare(_, undefined, _) -> undefined;
compare(_, _, undefined) -> undefined;
compare('=', Left, Right) -> Left == Right;
compare('<>', Left, Right) -> Left /= Right;
compare('>', Left, Right) -> Left > Right;
compare('<', Left, Right) -> Left < Right;
compare('>=', Left, Right) -> Left >= Right;
compare('<=', Left, Right) -> Left =< Right.

arithmetic(_, undefined, _) ->
    undefined;
arithmetic(_, _, undefined) ->
    undefined;
arithmetic('+', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left + Right;
arithmetic('-', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left - Right;
arithmetic('*', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left * Right;
arithmetic('/', Left, Right) when Right =:= 0 andalso Left > 0 ->
    infinity;
arithmetic('/', Left, Right) when Right =:= 0 andalso Left < 0 ->
    '-infinity';
arithmetic('/', Left, Right) when Right =:= 0 andalso Left =:= 0 ->
    'NaN';
arithmetic('/', Left, Right) when is_integer(Left) andalso is_integer(Right) ->
    Left div Right;
arithmetic('/', Left, Right) when is_number(Left) andalso is_number(Right) ->
    Left / Right;
arithmetic(_, _, _) ->
    undefined.

between(Value, From, To)
  when Value =:= undefined orelse
       From =:= undefined orelse
       To =:= undefined ->
    undefined;
between(Value, From, To) ->
    From =< Value andalso Value =< To.

is_in(undefined, _) ->
    undefined;
is_in(Value, List) ->
    lists:member(Value, List).

is_like(Value, Pattern, Escape)
  when is_binary(Value) andalso
       is_binary(Pattern) ->
    RegexPattern = like_to_regex(Pattern, Escape),
    case re:run(Value, RegexPattern, [{capture, none}]) of
        match -> true;
        nomatch -> false
    end;
is_like(_, _, _) ->
    undefined.

%% Convert LIKE pattern to regex
%%
%% TODO compilation should happen when the consumer attaches.
%% Should this happen within rabbit_jms_selector_parser.yrl?
like_to_regex(Pattern, Escape) ->
    {ok, Regex} = convert_like_to_regex(binary_to_list(Pattern), Escape),
    {ok, MP} = re:compile(<<"^", Regex/binary, "$">>),
    MP.

convert_like_to_regex(Pattern, Escape) ->
    convert_like_to_regex(Pattern, [], Escape).

convert_like_to_regex([], Acc, _) ->
    {ok, iolist_to_binary(lists:reverse(Acc))};
convert_like_to_regex([Esc, Char | Rest], Acc, Esc) when Esc =/= no_escape ->
    % Escaped character - take literally
    convert_like_to_regex(Rest, [escape_regex_char(Char) | Acc], Esc);
convert_like_to_regex([$% | Rest], Acc, Esc) ->
    % % means any sequence of characters (including none)
    convert_like_to_regex(Rest, [".*" | Acc], Esc);
convert_like_to_regex([$_ | Rest], Acc, Esc) ->
    % _ means any single character
    convert_like_to_regex(Rest, [$. | Acc], Esc);
convert_like_to_regex([Char | Rest], Acc, Esc) ->
    % Regular character - escape for regex
    convert_like_to_regex(Rest, [escape_regex_char(Char) | Acc], Esc).

%% Escape special regex characters
escape_regex_char(Char)
  when Char =:= $. orelse Char =:= $* orelse Char =:= $+ orelse
       Char =:= $? orelse Char =:= $^ orelse Char =:= $$ orelse
       Char =:= $[ orelse Char =:= $] orelse Char =:= $( orelse
       Char =:= $) orelse Char =:= ${ orelse Char =:= $} orelse
       Char =:= $| orelse Char =:= $\\ ->
    [$\\, Char];
escape_regex_char(Char) ->
    Char.
