%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2012, 2013 GoPivotal, Inc.  All rights reserved.
%% -----------------------------------------------------------------------------
%% Derived from works which were:
%% Copyright (c) 2002, 2012 Tim Watson (watson.timothy@gmail.com)
%% Copyright (c) 2012, 2013 Steve Powell (Zteve.Powell@gmail.com)
%% -----------------------------------------------------------------------------

%% Drives the scanner and parser and checks type validity

%% -----------------------------------------------------------------------------
-module(sjx_dialect).

-export([analyze/2]).

analyze(TypeInfo, S) -> validate(TypeInfo, parse(scan(S))).

scan(S) -> sjx_scanner:string(S).

parse({ok, Tokens, _}) -> sjx_parser:parse(Tokens);
parse(_) -> error.

validate(TypeInfo, {ok, AST}) -> check_types(TypeInfo, AST);
validate(_, _) -> error.

%% Validation functions

check_types(TypeInfo, AST) ->
    case check_type_bool(TypeInfo, AST) of
        true -> AST;
        _ -> error
    end.

get_ident_type(Ident, TypeInfo) ->
    case proplists:lookup(Ident, TypeInfo) of
        {_, _, Type} -> Type;
        _            -> undefined
    end.

match_ident_type(Ident, Match, TypeInfo) ->
    case get_ident_type(Ident, TypeInfo) of
        undefined -> true;  %% presumption of innocence
        Match     -> true;
        _         -> false  %% defined but not the same type
    end.

%% Type checking general expressions
%%
check_type_bool(_TypeInfo, true ) -> true;
check_type_bool(_TypeInfo, false ) -> true;
check_type_bool( TypeInfo, {'ident', Ident } ) -> match_ident_type(Ident, <<"boolean">>, TypeInfo);
check_type_bool( TypeInfo, {'not', Exp }) -> check_type_bool(TypeInfo, Exp);
check_type_bool( TypeInfo, {'and', Exp1, Exp2 }) -> check_type_bool(TypeInfo, Exp1) andalso check_type_bool(TypeInfo, Exp2);
check_type_bool( TypeInfo, {'or', Exp1, Exp2 }) -> check_type_bool(TypeInfo, Exp1) andalso check_type_bool(TypeInfo, Exp2);
check_type_bool( TypeInfo, {'like', LHS, {regex, _RX} }) -> check_type_string(TypeInfo, LHS);
check_type_bool( TypeInfo, {'not_like', LHS, {regex, _RX} }) -> check_type_string(TypeInfo, LHS);
check_type_bool( TypeInfo, {'between', Exp1, Exp2 }) -> check_type_arith(TypeInfo, Exp1) andalso check_type_range(TypeInfo, Exp2);
check_type_bool( TypeInfo, {'not_between', Exp1, Exp2 }) -> check_type_arith(TypeInfo, Exp1) andalso check_type_range(TypeInfo, Exp2);
check_type_bool(_TypeInfo, {'is_null', Exp }) -> check_type_ident(Exp);
check_type_bool(_TypeInfo, {'not_null', Exp }) -> check_type_ident(Exp);
check_type_bool( TypeInfo, {'in', LHS, RHS}) -> check_type_string(TypeInfo, LHS) andalso check_type_list(RHS);
check_type_bool( TypeInfo, {'not_in', LHS, RHS}) -> check_type_string(TypeInfo, LHS) andalso check_type_list(RHS);
check_type_bool( TypeInfo, { Op, LHS, RHS }) ->
    ( check_eq_op(Op)
        andalso ( (check_type_arith(TypeInfo, LHS) andalso check_type_arith(TypeInfo, RHS))
                orelse
                  (check_type_bool(TypeInfo, LHS) andalso check_type_bool(TypeInfo, RHS))
                orelse
                  (check_type_string(TypeInfo, LHS) andalso check_type_string(TypeInfo, RHS))
                orelse
                  check_type_enums(TypeInfo, LHS, RHS)
                )
    ) orelse
    ( check_cmp_op(Op) andalso check_type_arith(TypeInfo, LHS) andalso check_type_arith(TypeInfo, RHS) );
check_type_bool(_,_) -> false.

check_type_ident( {'ident', _} ) -> true;
check_type_ident(_) -> false.

check_type_enums( TypeInfo, {'ident', LIdent}, {'ident', RIdent}) ->
    case {get_ident_type(LIdent, TypeInfo), get_ident_type(RIdent, TypeInfo)} of
        {undefined, _} -> true;  %% either can be undefined
        {_, undefined} -> true;
        {EType, EType} -> true;  %% or both types must match exactly
        _              -> false
    end;
check_type_enums( TypeInfo, LHS, RHS = {'ident', _}) -> check_type_enums(TypeInfo, RHS, LHS);
check_type_enums( TypeInfo, {'ident', Ident}, RHS) when is_binary(RHS) ->
    case get_ident_type(Ident, TypeInfo) of
        ElemList when is_list(ElemList) -> lists:member({longstr, RHS}, ElemList);
        _                               -> false
    end;
check_type_enums(_,_,_) -> false.

check_type_string( TypeInfo, {'ident', Ident} ) -> match_ident_type(Ident, <<"string">>, TypeInfo);
check_type_string(_TypeInfo, Exp) when is_binary(Exp) -> true;
check_type_string(_,_) -> false.

check_type_arith( TypeInfo, {'ident', Ident} ) -> match_ident_type(Ident, <<"number">>, TypeInfo);
check_type_arith(_TypeInfo, E ) when is_number(E) -> true;
check_type_arith( TypeInfo, { Op, LHS, RHS }) -> check_arith_op(Op) andalso check_type_arith(TypeInfo, LHS) andalso check_type_arith(TypeInfo, RHS);
check_type_arith( TypeInfo, { Op, Exp }) -> check_sign_op(Op) andalso check_type_arith(TypeInfo, Exp);
check_type_arith(_,_) -> false.

check_eq_op( '='  ) -> true;
check_eq_op( '<>' ) -> true;
check_eq_op(_) -> false.

check_sign_op( '+' ) -> true;
check_sign_op( '-' ) -> true;
check_sign_op(_) -> false.

check_arith_op( '+' ) -> true;
check_arith_op( '-' ) -> true;
check_arith_op( '*' ) -> true;
check_arith_op( '/' ) -> true;
check_arith_op(_) -> false.

check_cmp_op( '<'  ) -> true;
check_cmp_op( '>'  ) -> true;
check_cmp_op( '<=' ) -> true;
check_cmp_op( '>=' ) -> true;
check_cmp_op(_) -> false.

check_type_range( TypeInfo, {'range', From, To }) -> check_type_arith(TypeInfo, From) andalso check_type_arith(TypeInfo, To);
check_type_range(_,_) -> false.

check_type_list( [] ) -> false;
check_type_list( [ _hd | _tl ] ) -> true.