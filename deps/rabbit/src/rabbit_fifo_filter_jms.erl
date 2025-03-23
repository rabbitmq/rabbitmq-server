%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

%% Evaluates parsed JMS message selector expressions.
-module(rabbit_fifo_filter_jms).

-export([eval/2]).

%% "When used in a message selector JMSDeliveryMode is treated as having
%% the values 'PERSISTENT' and 'NON_PERSISTENT'."
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#special-notes
-define(DELIVERY_MODE_PERSISTENT, <<"PERSISTENT">>).
-define(DELIVERY_MODE_NON_PERSISTENT, <<"NON_PERSISTENT">>).
-define(IS_DELIVERY_MODE(Val),
        Val =:= ?DELIVERY_MODE_PERSISTENT orelse
        Val =:= ?DELIVERY_MODE_NON_PERSISTENT).

%% "For PERSISTENT messages, the durable field of header MUST be set to true.
%% For NON PERSISTENT messages, the durable field of header MUST be either
%% set to false or omitted."
%% amqp-bindmap-jms-v1.0-wd10
is_durable(?DELIVERY_MODE_PERSISTENT) ->
    true;
is_durable(?DELIVERY_MODE_NON_PERSISTENT) ->
    false.

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
       Type =:= string orelse
       Type =:= boolean ->
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
                _Unknown -> undefined
            end;
        false ->
            % Short-circuit
            false;
        _Unknown ->
            case eval0(Expr2, Headers) of
                false -> false;
                _ -> undefined
            end
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
                _Unknown -> undefined
            end;
        _Unknown ->
            case eval0(Expr2, Headers) of
                true -> true;
                _ -> undefined
            end
    end;
%% Table 3-6 in
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#null-values
eval0({'not', Expr}, Headers) ->
    case eval0(Expr, Headers) of
        true -> false;
        false -> true;
        _Unknown -> undefined
    end;

%% Comparison operators
eval0({'=' = Op, Expr1, Expr2}, Headers) ->
    compare_eq(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
eval0({'<>' = Op, Expr1, Expr2}, Headers) ->
    compare_eq(Op, eval0(Expr1, Headers), eval0(Expr2, Headers));
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
    is_in(Value, ValueList);

eval0({'not_in', Expr, ValueList}, Headers) ->
    case eval0({'in', Expr, ValueList}, Headers) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'like', Expr, Pattern, Escape}, Headers) ->
    Value = eval0(Expr, Headers),
    case is_binary(Value) of
        true ->
            case unicode:characters_to_list(Value) of
                L when is_list(L) ->
                    like_match(L, Pattern, Escape);
                _ ->
                    undefined
            end;
        false ->
            undefined
    end;

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

eval0(_UnexpectedExpression, _) ->
    undefined.

%% Helper functions

%% "Comparison or arithmetic with an unknown value always yields an unknown value."
compare(_, undefined, _) ->
    undefined;
compare(_, _, undefined) ->
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

compare_eq(Op, Left, Right) when is_boolean(Left) andalso ?IS_DELIVERY_MODE(Right) ->
    compare(Op, Left, is_durable(Right));
compare_eq(Op, Left, Right) when is_boolean(Right) andalso ?IS_DELIVERY_MODE(Left) ->
    compare(Op, is_durable(Left), Right);
compare_eq(Op, Left, Right) ->
    compare(Op, Left, Right).

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
    undefined;
is_in(Value, List) ->
    lists:member(Value, List).

like_match([], [], _) ->
    true;
like_match(_, [], _) ->
    false;
like_match([], [Char | _], _) when Char =/= $% ->
    false;
like_match([], [$% | PatRest], Escape) ->
    %% String is empty, pattern starts with % - % can match empty string, continue with rest
    like_match([], PatRest, Escape);
like_match(S, [Escape, Char | PatRest], Escape) when Escape =/= no_escape ->
    %% Found escape character, match the next character literally
    case S of
        [Char | StrRest] ->
            like_match(StrRest, PatRest, Escape);
        _ ->
            false
    end;
like_match([_ | StrRest], [$_ | PatRest], Escape) ->
    %% _ matches exactly 1 character
    like_match(StrRest, PatRest, Escape);
like_match(S, [$% | PatRest] = Pattern, Escape) ->
    %% % matches 0 or more characters - try two paths:
    %% 1. Skip % (match 0 characters)
    like_match(S, PatRest, Escape) orelse
    %% 2. Match current character and keep % in pattern for next iteration
    like_match(tl(S), Pattern, Escape);
like_match([Char | StrRest], [Char | PatRest], Escape) ->
    like_match(StrRest, PatRest, Escape);
like_match(_, Pattern, _) when is_list(Pattern) ->
    false.
