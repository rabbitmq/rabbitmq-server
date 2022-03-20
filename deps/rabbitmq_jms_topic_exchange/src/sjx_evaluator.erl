%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2012-2022 VMware, Inc. or its affiliates.  All rights reserved.
%% -----------------------------------------------------------------------------
%% Derived from works which were:
%% Copyright (c) 2002, 2012 Tim Watson (watson.timothy@gmail.com)
%% Copyright (c) 2012, 2013 Steve Powell (Zteve.Powell@gmail.com)
%% -----------------------------------------------------------------------------

%% Evaluate an SQL expression for filtering purposes

%% -----------------------------------------------------------------------------

-module(sjx_evaluator).

-export([evaluate/2]).
%% Evaluation function
%%
%%   Given Headers (a list of keyed typed values), and a
%%   parsed SQL string, evaluate the truth or falsity of the expression.
%%
%%   If an identifier is absent from Headers, or the types do not match the comparisons, the
%%   expression will evaluate to false.

-type itemname() :: binary().
-type itemtype() ::
      'longstr' | 'signedint' | 'byte' | 'double' | 'float' | 'long' | 'short' | 'bool'.
-type itemvalue() :: any().

-type tableitem() :: { itemname(), itemtype(), itemvalue() }.
-type table() :: list(tableitem()).

-type expression() :: any().

-spec evaluate(expression(), table()) -> true | false | error.


evaluate( true,                           _Headers ) -> true;
evaluate( false,                          _Headers ) -> false;

evaluate( {'not', Exp },                   Headers ) -> not3(evaluate(Exp, Headers));
evaluate( {'ident', Ident },               Headers ) -> lookup_value(Headers, Ident);
evaluate( {'is_null', Exp },               Headers ) -> val_of(Exp, Headers) =:= undefined;
evaluate( {'not_null', Exp },              Headers ) -> val_of(Exp, Headers) =/= undefined;
evaluate( { Op, Exp },                     Headers ) -> do_una_op(Op, evaluate(Exp, Headers));

evaluate( {'and', Exp1, Exp2 },            Headers ) -> and3(evaluate(Exp1, Headers), evaluate(Exp2, Headers));
evaluate( {'or', Exp1, Exp2 },             Headers ) -> or3(evaluate(Exp1, Headers), evaluate(Exp2, Headers));
evaluate( {'like', LHS, Patt, Esc },       Headers ) -> isLike(val_of(LHS, Headers), {Patt, Esc});
evaluate( {'not_like', LHS, Patt, Esc },   Headers ) -> not3(isLike(val_of(LHS, Headers), {Patt, Esc}));
evaluate( { Op, Exp, {range, From, To} },  Headers ) -> evaluate({ Op, Exp, From, To }, Headers);
evaluate( {'between', Exp, From, To},           Hs ) -> between(evaluate(Exp, Hs), evaluate(From, Hs), evaluate(To, Hs));
evaluate( {'not_between', Exp, From, To},       Hs ) -> not3(between(evaluate(Exp, Hs), evaluate(From, Hs), evaluate(To, Hs)));
evaluate( { Op, LHS, RHS },                Headers ) -> do_bin_op(Op, evaluate(LHS, Headers), evaluate(RHS, Headers));

evaluate( Value,                          _Headers ) -> Value.

not3(true ) -> false;
not3(false) -> true;
not3(_    ) -> undefined.

and3(true,  true ) -> true;
and3(false, _    ) -> false;
and3(_,     false) -> false;
and3(_,     _    ) -> undefined.

or3(false, false) -> false;
or3(true,  _    ) -> true;
or3(_,     true ) -> true;
or3(_,     _    ) -> undefined.

do_una_op(_, undefined)  -> undefined;
do_una_op('-', E) -> -E;
do_una_op('+', E) -> +E;
do_una_op(_,   _) -> error.

do_bin_op(_, undefined, _)  -> undefined;
do_bin_op(_, _, undefined ) -> undefined;
do_bin_op('=' , L, R) -> L == R;
do_bin_op('<>', L, R) -> L /= R;
do_bin_op('>' , L, R) -> L > R;
do_bin_op('<' , L, R) -> L < R;
do_bin_op('>=', L, R) -> L >= R;
do_bin_op('<=', L, R) -> L =< R;
do_bin_op('in', L, R) -> isIn(L, R);
do_bin_op('not_in', L, R) -> not isIn(L, R);
do_bin_op('+' , L, R) -> L + R;
do_bin_op('-' , L, R) -> L - R;
do_bin_op('*' , L, R) -> L * R;
do_bin_op('/' , L, R) when R /= 0 -> L / R;
do_bin_op('/' , L, R) when L > 0 andalso R == 0 -> plus_infinity;
do_bin_op('/' , L, R) when L < 0 andalso R == 0 -> minus_infinity;
do_bin_op('/' , L, R) when L == 0 andalso R == 0 -> nan;
do_bin_op(_,_,_) -> error.

isLike(undefined, _Patt) -> undefined;
isLike(L, {regex, MP}) -> patt_match(L, MP);
isLike(L, {Patt, Esc}) -> patt_match(L, pattern_of(Patt, Esc)).

patt_match(L, MP) ->
  BS = byte_size(L),
  case re:run(L, MP, [{capture, first}]) of
    {match, [{0, BS}]} -> true;
    _                  -> false
  end.

isIn(_L, []   ) -> false;
isIn( L, [L|_]) -> true;
isIn( L, [_|R]) -> isIn(L,R).

val_of({'ident', Ident}, Hs) -> lookup_value(Hs, Ident);
val_of(Value,           _Hs) -> Value.

between(E, F, T) when E =:= undefined orelse F =:= undefined orelse T =:= undefined -> undefined;
between(Value, Lo, Hi) -> Lo =< Value andalso Value =< Hi.

lookup_value(Table, Key) ->
  case lists:keyfind(Key, 1, Table) of
    {_, longstr,   Value} -> Value;
    {_, signedint, Value} -> Value;
    {_, float,     Value} -> Value;
    {_, double,    Value} -> Value;
    {_, byte,      Value} -> Value;
    {_, short,     Value} -> Value;
    {_, long,      Value} -> Value;
    {_, bool,      Value} -> Value;
    false                 -> undefined
  end.

pattern_of(S, Esc) -> compile_re(gen_re(binary_to_list(S), Esc)).

gen_re(S, <<Ch>>   ) -> convert(S, [], Ch       );
gen_re(S, no_escape) -> convert(S, [], no_escape);
gen_re(_,_) -> error.

convert([],               Acc, _Esc) -> lists:reverse(Acc);
convert([Esc, Ch | Rest], Acc,  Esc) -> convert(Rest, [escape(Ch) | Acc], Esc);
convert([$_ | Rest],      Acc,  Esc) -> convert(Rest, [$.         | Acc], Esc);
convert([$% | Rest],      Acc,  Esc) -> convert(Rest, [".*"       | Acc], Esc);
convert([Ch | Rest],      Acc,  Esc) -> convert(Rest, [escape(Ch) | Acc], Esc).

escape($.)  -> "\\.";
escape($*)  -> "\\*";
escape($+)  -> "\\+";
escape($?)  -> "\\?";
escape($^)  -> "\\^";
escape($=)  -> "\\=";
escape($!)  -> "\\!";
escape($:)  -> "\\:";
escape($$)  -> "\\$";
escape(${)  -> "\\{";
escape($})  -> "\\}";
escape($()  -> "\\(";
escape($))  -> "\\)";
escape($|)  -> "\\|";
escape($[)  -> "\\[";
escape($])  -> "\\]";
escape($/)  -> "\\/";
escape($\\) -> "\\\\";
escape(Ch)  -> Ch.

compile_re(error) -> error;
compile_re(MatchMany) ->
    case re:compile(MatchMany)
    of  {ok, Rx} -> Rx;
        _        -> error
    end.
