%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

-module(rabbit_amqp_filter_jms).

-include_lib("amqp10_common/include/amqp10_filter.hrl").

-type parsed_expression() :: {ApplicationProperties :: boolean(),
                              rabbit_jms_ast:ast()}.

-export_type([parsed_expression/0]).

-export([parse/1,
         eval/2]).

-spec parse(tuple()) ->
    {ok, parsed_expression()} | error.
parse({described, Descriptor, {utf8, JmsSelector}})
  when Descriptor =:= {symbol, ?DESCRIPTOR_NAME_SELECTOR_FILTER} orelse
       Descriptor =:= {ulong, ?DESCRIPTOR_CODE_SELECTOR_FILTER} ->
    case unicode:characters_to_list(JmsSelector) of
        String when is_list(String) ->
            case rabbit_jms_selector_lexer:string(String) of
                {ok, Tokens, _EndLocation} ->
                    case rabbit_jms_selector_parser:parse(Tokens) of
                        {ok, Ast0} ->
                            Ast = rabbit_jms_ast:map_identifiers(
                                    fun rabbit_amqp_util:section_field_name_to_atom/1,
                                    Ast0),
                            AppProps = rabbit_jms_ast:has_binary_identifier(Ast),
                            % rabbit_log:debug(
                            %   "~s:~s~nJmsSelector: ~p~nTokens: ~p~n~Ast0: ~p~n~ast: ~p~nAppProps: ~p",
                            %   [?MODULE, ?FUNCTION_NAME, JmsSelector, Tokens, Ast0, Ast, AppProps]),
                            {ok, {AppProps, Ast}};
                        {error, Reason} ->
                            rabbit_log:warning(
                              "failed to parse JMS message selector '~ts': ~p",
                              [JmsSelector, Reason]),
                            error
                    end;
                {error, {_Line, _Mod, ErrDescriptor}, _Locaction} ->
                    Reason = lists:flatten(leex:format_error(ErrDescriptor)),
                    rabbit_log:warning(
                      "failed to scan JMS message selector '~ts': ~p",
                      [JmsSelector, Reason]),
                    error
            end;
        Error ->
            rabbit_log:warning("JMS message selector '~p' is not UTF-8: ~p",
                               [JmsSelector, Error]),
            error
    end;
parse(_) ->
    error.

%% Evaluates a parsed JMS message selector expression.
-spec eval(parsed_expression(), mc:state()) -> boolean().
eval({ApplicationProperties, Ast}, Msg) ->
    State = case ApplicationProperties of
                true ->
                    %% This AST references application-properties.
                    %% Since we will probably dereference during evaluation,
                    %% for performance reasons let's parse them here only once.
                    AppProps = mc:routing_headers(Msg, []),
                    {AppProps, Msg};
                false ->
                    Msg
            end,
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
eval0({'=' = Op, Expr1, Expr2}, Msg) ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'<>' = Op, Expr1, Expr2}, Msg) ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'>' = Op, Expr1, Expr2}, Msg) ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'<' = Op, Expr1, Expr2}, Msg) ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'>=' = Op, Expr1, Expr2}, Msg) ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'<=' = Op, Expr1, Expr2}, Msg) ->
    compare(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));

%% Arithmetic operators
eval0({'+' = Op, Expr1, Expr2}, Msg) ->
    arithmetic(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'-' = Op, Expr1, Expr2}, Msg) ->
    arithmetic(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'*' = Op, Expr1, Expr2}, Msg) ->
    arithmetic(Op, eval0(Expr1, Msg), eval0(Expr2, Msg));
eval0({'/' = Op, Expr1, Expr2}, Msg) ->
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

eval0({'not_between', Expr, From, To}, Msg) ->
    case eval0({'between', Expr, From, To}, Msg) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'in', Expr, ValueList}, Msg) ->
    Value = eval0(Expr, Msg),
    is_in(Value, ValueList);

eval0({'not_in', Expr, ValueList}, Msg) ->
    case eval0({'in', Expr, ValueList}, Msg) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'like', Expr, Pattern, Escape}, Msg) ->
    Value = eval0(Expr, Msg),
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

eval0({'not_like', Expr, Pattern, Escape}, Msg) ->
    case eval0({'like', Expr, Pattern, Escape}, Msg) of
        true -> false;
        false -> true;
        _ -> undefined
    end;

eval0({'is_null', Expr}, Msg) ->
    eval0(Expr, Msg) =:= undefined;

eval0({'is_not_null', Expr}, Msg) ->
    eval0(Expr, Msg) =/= undefined;

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

%% defined in both AMQP and JMS
-define(DEFAULT_MSG_PRIORITY, 4).

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
