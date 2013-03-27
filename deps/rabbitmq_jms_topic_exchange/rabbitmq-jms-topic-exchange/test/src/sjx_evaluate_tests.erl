%% -----------------------------------------------------------------------------
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
-module(sjx_evaluate_tests).
-include_lib("eunit/include/eunit.hrl").

-import(sjx_dialect, [analyze/1]).
-import(sjx_evaluator, [evaluate/2]).

eval(Hs, S) -> evaluate(analyze(S),Hs).

basic_evaluate_test_() ->
    Hs = [{<<"JMSType">>, longstr, <<"car">>},
          {<<"colour">>, longstr, <<"blue">>},
          {<<"altcol">>, longstr, <<"'blue">>},
          {<<"likevar">>, longstr, <<"bl_ue">>},
          {<<"weight">>, signedint, 2501},
          {<<"WeIgHt">>, signedint, 2},
          {<<"afloat">>, float, 3.0e-2},
          {<<"abool">>, bool, false}],

    [ ?_assert(    eval(Hs, " JMSType = 'car'                                  "))
    , ?_assert(not eval(Hs, " abool                                            "))
    , ?_assert(    eval(Hs, " not abool                                        "))
    , ?_assert(    eval(Hs, " colour = 'blue'                                  "))
    , ?_assert(    eval(Hs, " weight = 2501                                    "))
    , ?_assert(    eval(Hs, " WeIgHt = 2                                       "))
    , ?_assert(    eval(Hs, " 2501 = weight                                    "))
    , ?_assert(    eval(Hs, " afloat = 3.0e-02                                 "))
    , ?_assert(    eval(Hs, " afloat = 3.e-2                                   "))
    , ?_assert(    eval(Hs, " afloat = 3e-2                                    "))
    , ?_assert(    eval(Hs, " weight > 2500                                    "))
    , ?_assert(    eval(Hs, " weight < 2502                                    "))
    , ?_assert(    eval(Hs, " weight >= 2500                                   "))
    , ?_assert(    eval(Hs, " weight <= 2502                                   "))
    , ?_assert(    eval(Hs, " weight >= 2501                                   "))
    , ?_assert(    eval(Hs, " weight <= 2501                                   "))
    , ?_assert(    eval(Hs, " weight between 0 and 2501                        "))
    , ?_assert(    eval(Hs, " weight between 2500 and 2501                     "))
    , ?_assert(    eval(Hs, " 17 between 17 and 18                             "))
    , ?_assert(    eval(Hs, " 16 not between 17 and 18                         "))
    , ?_assert(    eval(Hs, " 2500 < weight                                    "))
    , ?_assert(    eval(Hs, " 2502 > weight                                    "))
    , ?_assert(    eval(Hs, " 2500 <= weight                                   "))
    , ?_assert(    eval(Hs, " 2502 >= weight                                   "))
    , ?_assert(    eval(Hs, " 2501 <= weight                                   "))
    , ?_assert(    eval(Hs, " 2501 >= weight                                   "))
    , ?_assert(    eval(Hs, " colour LIKE 'bl%'                                "))
    , ?_assert(    eval(Hs, " likevar like 'b_!_ue' escape '!'                 "))
    , ?_assert(    eval(Hs, " colour like 'bl_e'                               "))
    , ?_assert(    eval(Hs, " colour not like 'l%'                             "))
    , ?_assert(    eval(Hs, " altcol not like 'bl%'                            "))
    , ?_assert(    eval(Hs, " altcol     like '''bl%'                          "))
    , ?_assert(    eval(Hs, " altcol not like 'bl%'                            "))
    , ?_assert(    eval(Hs, " colour LIKE 'bl%' and weight > 2500 or false     "))
    , ?_assert(not eval(Hs, " weight <= 2500                                   "))
    , ?_assert(not eval(Hs, " colour not like 'bl%'                            "))
    , ?_assert(undefined =:= eval(Hs, " missing <= 2500                        "))
    ].