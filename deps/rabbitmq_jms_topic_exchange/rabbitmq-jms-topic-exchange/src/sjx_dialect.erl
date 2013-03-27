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

%% Identifier types we know about
match_type_of(<<"JMSDeliveryMode">>, Match) -> Match =:= enum;
match_type_of(<<"JMSPriority">>, Match) -> Match =:= int;
match_type_of(<<"JMSMessageID">>, Match) -> Match =:= string;
match_type_of(<<"JMSTimestamp">>, Match) -> Match =:= int;
match_type_of(<<"JMSCorrelationID">>, Match) -> Match =:= string;
match_type_of(<<"JMSType">>, Match) -> Match =:= string;
match_type_of(_, _Match) -> true.          %% presumption of innocence

check_type_bool( true ) -> true;
check_type_bool( false ) -> true;
check_type_bool( {'ident', Ident } ) -> match_type_of(Ident, bool);
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
                  (check_type_enum(LHS) andalso check_type_enum(RHS))
                  orelse
                  (check_type_string(LHS) andalso check_type_string(RHS))
                ) )
    orelse
    ( check_cmp_op(Op) andalso check_type_arith(LHS) andalso check_type_arith(RHS) );
check_type_bool( _Expression ) -> false.

check_type_ident( {'ident', _} ) -> true;
check_type_ident(_) -> false.

check_type_enum( {'ident', <<"JMSDeliveryMode">>} ) -> true;
check_type_enum(Exp) when is_binary(Exp) -> (Exp == <<"NON_PERSISTENT">> orelse Exp == <<"PERSISTENT">>);
check_type_enum(_) -> false.

check_type_string( {'ident', Ident} ) -> match_type_of(Ident, string);
check_type_string(Exp) when is_binary(Exp) -> true;
check_type_string(_) -> false.

check_type_arith( {'ident', Ident} ) -> match_type_of(Ident, int);
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
