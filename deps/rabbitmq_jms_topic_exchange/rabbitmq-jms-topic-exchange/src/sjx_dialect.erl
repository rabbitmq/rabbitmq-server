%% -----------------------------------------------------------------------------
%% Copyright (c) 2002-2012 Tim Watson (watson.timothy@gmail.com)
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.
%% -----------------------------------------------------------------------------
%% @doc this drives the scanner and parser and checks the type validity
%% -----------------------------------------------------------------------------
-module(sjx_dialect).

-export([analyze/1]).

analyze(S) -> validate(parse(scan(S))).

scan(S) -> sjx_scanner:string(S).

parse({ok, Tokens, _}) -> sjx_parser:parse(Tokens);
parse(_) -> error.

validate({ok, AST}) -> check_types(AST);
validate(_) -> error.

check_types(AST) ->
    case check_type_bool(AST) of
        true -> AST;
        _ -> error
    end.

%% Type checking identifiers
%%
-define(IDENT_TYPE_INFO,
[ {<<"JMSDeliveryMode">>, {enum, [<<"PERSISTENT">>, <<"NON_PERSISTENT">>]}}
, {<<"JMSPriority">>, number}
, {<<"JMSMessageID">>, string}
, {<<"JMSTimestamp">>, number}
, {<<"JMSCorrelationID">>, string}
, {<<"JMSType">>, string}
]).

get_ident_type(Ident) -> proplists:get_value(Ident, ?IDENT_TYPE_INFO).

match_ident_type(Ident, Match) ->
    case get_ident_type(Ident) of
        undefined -> true;  %% presumption of innocence
        Match     -> true;
        _         -> false
    end.

%% Type checking general expressions
%%
check_type_bool( true ) -> true;
check_type_bool( false ) -> true;
check_type_bool( {'ident', Ident } ) -> match_ident_type(Ident, boolean);
check_type_bool( {'not', Exp }) -> check_type_bool(Exp);
check_type_bool( {'and', Exp1, Exp2 }) -> check_type_bool(Exp1) andalso check_type_bool(Exp2);
check_type_bool( {'or', Exp1, Exp2 }) -> check_type_bool(Exp1) andalso check_type_bool(Exp2);
check_type_bool( {'like', LHS, {regex, _RX} }) -> check_type_string(LHS);
check_type_bool( {'not_like', LHS, {regex, _RX} }) -> check_type_string(LHS);
check_type_bool( {'between', Exp1, Exp2 }) -> check_type_arith(Exp1) andalso check_type_range(Exp2);
check_type_bool( {'not_between', Exp1, Exp2 }) -> check_type_arith(Exp1) andalso check_type_range(Exp2);
check_type_bool( {'is_null', Exp }) -> check_type_ident(Exp);
check_type_bool( {'not_null', Exp }) -> check_type_ident(Exp);
check_type_bool( { Op, LHS, RHS }) ->
    ( check_eq_op(Op)
        andalso ( (check_type_arith(LHS) andalso check_type_arith(RHS))
                  orelse
                  (check_type_bool(LHS) andalso check_type_bool(RHS))
                  orelse
                  (check_type_string(LHS) andalso check_type_string(RHS))
                  orelse
                  check_type_enums(LHS, RHS)
                ) )
    orelse
    ( check_cmp_op(Op) andalso check_type_arith(LHS) andalso check_type_arith(RHS) );
check_type_bool( _Expression ) -> false.

check_type_ident( {'ident', _} ) -> true;
check_type_ident(_) -> false.

check_type_enums({'ident', LIdent}, {'ident', RIdent}) ->
    case {get_ident_type(LIdent), get_ident_type(RIdent)} of
        {undefined, _} -> true;  %% either can be undefined
        {_, undefined} -> true;
        {EType, EType} -> true;  %% or both types must match exactly
        _              -> false
    end;
check_type_enums(LHS, RHS = {'ident', _}) -> check_type_enums(RHS, LHS);
check_type_enums({'ident', Ident}, RHS) when is_binary(RHS) ->
    case get_ident_type(Ident) of
        {'enum', BinList} -> lists:member(RHS, BinList);
        _                 -> false
    end;
check_type_enums(_,_) -> false.

check_type_string( {'ident', Ident} ) -> match_ident_type(Ident, string);
check_type_string(Exp) when is_binary(Exp) -> true;
check_type_string(_) -> false.

check_type_arith( {'ident', Ident} ) -> match_ident_type(Ident, number);
check_type_arith( E ) when is_number(E) -> true;
check_type_arith( { Op, LHS, RHS }) -> check_arith_op(Op) andalso check_type_arith(LHS) andalso check_type_arith(RHS);
check_type_arith( { Op, Exp }) -> check_sign_op(Op) andalso check_type_arith(Exp);
check_type_arith(_) -> false.

check_eq_op( 'eq' ) -> true;
check_eq_op( 'neq' ) -> true;
check_eq_op(_) -> false.

check_sign_op( '+' ) -> true;
check_sign_op( '-' ) -> true;
check_sign_op(_) -> false.

check_arith_op( '+' ) -> true;
check_arith_op( '-' ) -> true;
check_arith_op( '*' ) -> true;
check_arith_op( '/' ) -> true;
check_arith_op(_) -> false.

check_cmp_op( 'lt' ) -> true;
check_cmp_op( 'gt' ) -> true;
check_cmp_op( 'lteq' ) -> true;
check_cmp_op( 'gteq' ) -> true;
check_cmp_op(_) -> false.

check_type_range({'range', From, To }) -> check_type_arith(From) andalso check_type_arith(To);
check_type_range(_) -> false.
