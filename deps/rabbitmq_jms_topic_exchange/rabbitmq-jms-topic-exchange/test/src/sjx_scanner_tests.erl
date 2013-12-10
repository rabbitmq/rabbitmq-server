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

%% Unit Tests for sjx_scanner.

%% -----------------------------------------------------------------------------
-module(sjx_scanner_tests).
-include_lib("eunit/include/eunit.hrl").

basic_scan_test_() ->
    [ ?_assertEqual( [t("car"), t('='), t("blue")]  ,  scan("'car' = 'blue'"))
    , ?_assertEqual( [i("car"), t('>'), t("blue")]  ,  scan("car > 'blue'"))
    , ?_assertEqual( [i("car"), t(not_between), t(1), t('and'), t(21.5)]  ,  scan("car not   between 1   and 21.5"))
    , ?_assertEqual( [i("car"), t(in), t('('), t("volvo"), t(')')]  ,  scan("car in ('volvo')"))
    , ?_assertEqual( [i("car"), t(not_in), t('('), t("volvo"), t(')')]  ,  scan("car   not  in ( 'volvo')  "))
    , ?_assertEqual( [t(4.0e2), t(0.0), t(','), t(1), t(3.0), h(57005)]  ,  scan(" 4e2  0e-1,1 3. 0xdead   "))
    , ?_assertEqual( [t(','), t('('), t(')')]  ,  scan("   ,   (  \t  )   \r"))
    , ?_assertEqual( [t('-'), t('-'), t(1.0), t('='), t(1.0)]  ,  scan(" --1.0 = 1.0"))
    , ?_assertEqual( [t(4.0), t(4.0), t(4.1)],  scan(" 4f 4.D 4.1f "))
    ].

% Produces a list of tokens from a string
scan(S) ->
    case sjx_scanner:string(S) of
        {ok, TokenList, _} -> TokenList;
        _                  -> undefined
    end.

% token shorthand for tests
t(like       ) -> tl(op_like,    1, like       );
t(not_like   ) -> tl(op_like,    1, not_like   );
t(in         ) -> tl(op_in,      1, in         );
t(not_in     ) -> tl(op_in,      1, not_in     );
t('and'      ) -> tl(op_and,     1, conjunction);
t('or'       ) -> tl(op_or,      1, disjunction);
t('not'      ) -> tl(op_not,     1, negation   );
t(is_null    ) -> tl(op_null,    1, is_null    );
t(not_null   ) -> tl(op_null,    1, not_null   );
t(between    ) -> tl(op_between, 1, between    );
t(not_between) -> tl(op_between, 1, not_between);
t(escape     ) -> tl(escape,     1, escape     );
t('='        ) -> tl(op_cmp,     1, '='        );
t('<>'       ) -> tl(op_cmp,     1, '<>'       );
t('>='       ) -> tl(op_cmp,     1, '>='       );
t('<='       ) -> tl(op_cmp,     1, '<='       );
t('<'        ) -> tl(op_cmp,     1, '<'        );
t('>'        ) -> tl(op_cmp,     1, '>'        );
t('+'        ) -> tl(op_plus,    1, '+'        );
t('-'        ) -> tl(op_plus,    1, '-'        );
t('*'        ) -> tl(op_mult,    1, '*'        );
t('/'        ) -> tl(op_mult,    1, '/'        );
t(true       ) -> tl(true,       1);
t(false      ) -> tl(false,      1);
t(','        ) -> tl(',',        1);
t('('        ) -> tl('(',        1);
t(')'        ) -> tl(')',        1);
t(L) when is_list(L)    -> tl(lit_string, 1, L);
t(F) when is_float(F)   -> tl(lit_flt,    1, F);
t(I) when is_integer(I) -> tl(lit_int,    1, I).

tl(Atom, TLine) -> {Atom, TLine}.

tl(TType, TLine, TVal) -> {TType, TLine, TVal}.

i(L) -> tl(ident, 1, L).     % identifier

h(I) -> tl(lit_hex, 1, I).   % hex integer
