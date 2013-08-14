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

%% This is the grammar definition for the JMS Topic Selector parser.

%% -----------------------------------------------------------------------------
%%
%% Notes:
%%
%%   Parsing rules take the form
%%      item -> RHS : builder
%%
%%   The builder expression takes parameters '$1', '$2', &c corresponding to the
%%   items in RHS, implicitly numbered from left to right, starting at 1.
%%
%%   Terminal items deliver the lexer scanned tokens as the parameters.
%%
%%   Nonterminal items deliver the result of the builder on the item definition.
%%
%%   Tokens from the lexical scanner are pairs or triples, denoting {Terminal, Line}
%%   or {Terminal, Line, Value}.  Value is omitted when this is irrelevant, for example
%%   for the COMMA or PARENS delimiters, whose Terminal is in fact their value.
%%
%% -----------------------------------------------------------------------------

Nonterminals
expression
or_expr
and_expr
not_expr
cmp_expr
arith_expr
plus_expr
mult_expr
sign_expr
simple
bool_literal
identifier
stringlist
strings
string
pattern
number      .

Terminals
'(' ')' ',' op_like op_in op_and op_or op_not op_null op_between escape true
false op_cmp op_plus op_mult ident lit_string lit_flt lit_int lit_hex.

Rootsymbol expression.

expression   -> or_expr                                            : '$1'.

or_expr      -> and_expr                                           : '$1'.
or_expr      -> and_expr op_or or_expr                             : disjunction('$1', '$3').

and_expr     -> not_expr                                           : '$1'.
and_expr     -> not_expr op_and and_expr                           : conjunction('$1', '$3').

not_expr     -> cmp_expr                                           : '$1'.
not_expr     -> op_not not_expr                                    : negation('$2').

cmp_expr     -> arith_expr                                         : '$1'.
cmp_expr     -> arith_expr op_cmp arith_expr                       : binary_op(value_of('$2'), '$1', '$3').
cmp_expr     -> arith_expr op_between arith_expr op_and arith_expr : binary_op(value_of('$2'), '$1', to_range('$3', '$5')).

arith_expr   -> plus_expr                                          : '$1'.

plus_expr    -> mult_expr                                          : '$1'.
plus_expr    -> mult_expr op_plus plus_expr                        : binary_op(value_of('$2'), '$1', '$3').

mult_expr    -> sign_expr                                          : '$1'.
mult_expr    -> sign_expr op_mult mult_expr                        : binary_op(value_of('$2'), '$1', '$3').

sign_expr    -> simple                                             : '$1'.
sign_expr    -> op_plus sign_expr                                  : unary_op(value_of('$1'), '$2').

simple       -> '(' expression ')'                                 : '$2'.
simple       -> bool_literal                                       : '$1'.
simple       -> string                                             : '$1'.
simple       -> number                                             : '$1'.
simple       -> identifier                                         : '$1'.
simple       -> identifier op_null                                 : unary_op(value_of('$2'), '$1').
simple       -> identifier op_in stringlist                        : binary_op(value_of('$2'), '$1', '$3').
simple       -> identifier op_like pattern                         : binary_op(value_of('$2'), '$1', '$3').

bool_literal -> true                                               : true.
bool_literal -> false                                              : false.

identifier   -> ident                                              : {'ident', bin_value_of('$1')}.

stringlist   -> '(' strings ')'                                    : '$2'.

strings      -> string                                             : [value_of('$1')].
strings      -> string ',' strings                                 : [value_of('$1') | '$3'].

string       -> lit_string                                         : bin_value_of('$1').

pattern      -> string                                             : pattern_of('$1').
pattern      -> string escape string                               : pattern_of('$1', '$3').

number       -> lit_hex                                            : value_of('$1').
number       -> lit_flt                                            : value_of('$1').
number       -> lit_int                                            : value_of('$1').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Erlang code.
%% -----------------------------------------------------------------------------
%% Copyright (c) 2002-2012 Tim Watson (watson.timothy@gmail.com)
%% Copyright (c) 2012-2013 Steve Powell (Zteve.Powell@gmail.com)
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
%%
%% NB: This file was generated by yecc - DO NOT MODIFY BY HAND.
%%

conjunction( true,   R     ) -> R;
conjunction( false, _R     ) -> false;
conjunction( L,      true  ) -> L;
conjunction(_L,      false ) -> false;
conjunction( L,      R     ) -> {'and', L, R}.

disjunction( true,  _R     ) -> true;
disjunction( false,  R     ) -> R;
disjunction(_L,      true  ) -> true;
disjunction( L,      false ) -> L;
disjunction( L,      R     ) -> {'or', L, R}.

negation( true       ) -> false;
negation( false      ) -> true;
negation( {'not', E} ) -> E;
negation( E          ) -> {'not', E}.

value_of({_,_,V}) -> V.

bin_value_of(Token) -> list_to_binary(value_of(Token)).

unary_op(Op, Ident) -> {Op, Ident}.

binary_op(Op, Lhs, Rhs) -> {Op, Lhs, Rhs}.

to_range(Low, High) -> {range, Low, High}.

pattern_of(S) -> pattern_of(S, no_escape).

pattern_of(S, Esc) -> compile_re(gen_re(binary_to_list(S), Esc)).

gen_re(S, <<Ch>>) -> convert(S, [], Ch);
gen_re(S, no_escape) -> convert(S, [], no_escape);
gen_re(_,_) -> error.

convert([], Acc, _Esc) -> lists:reverse(Acc);
convert([Esc, Ch | Rest], Acc, Esc) -> convert(Rest, [escape(Ch) | Acc], Esc);
convert([$_ | Rest], Acc, Esc) -> convert(Rest, [$. | Acc], Esc);
convert([$% | Rest], Acc, Esc) -> convert(Rest, [".*" | Acc], Esc);
convert([Ch | Rest], Acc, Esc) -> convert(Rest, [escape(Ch) | Acc], Esc).

escape($.) -> "\\.";
escape($*) -> "\\*";
escape($+) -> "\\+";
escape($?) -> "\\?";
escape($^) -> "\\^";
escape($=) -> "\\=";
escape($!) -> "\\!";
escape($:) -> "\\:";
escape($$) -> "\\$";
escape(${) -> "\\{";
escape($}) -> "\\}";
escape($() -> "\\(";
escape($)) -> "\\)";
escape($|) -> "\\|";
escape($[) -> "\\[";
escape($]) -> "\\]";
escape($/) -> "\\/";
escape($\\) -> "\\\\";
escape(Ch) -> Ch.

compile_re(error) -> error;
compile_re(MatchMany) ->
    case re:compile(MatchMany)
    of  {ok, Rx} -> {regex, Rx};
        _        -> error
    end.
