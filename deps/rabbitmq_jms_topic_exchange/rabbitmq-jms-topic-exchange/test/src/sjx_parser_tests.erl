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
-module(sjx_parser_tests).
-include_lib("eunit/include/eunit.hrl").

-import(sjx_dialect, [analyze/1]).

basic_parse_test_() ->
    [
     ?_assertMatch( {eq, <<"car">>, <<"blue">>},                      analyze("'car' = 'blue'               ")),
     ?_assertMatch( error,                                            analyze("'car' > 'blue'               ")),
     ?_assertMatch( {eq, 3.2, 3.3},                                   analyze("3.2 = 3.3                    ")),
     ?_assertMatch( {eq, 3.0, 3.3},                                   analyze("3. = 3.3                     ")),
     ?_assertMatch( {eq, 3.0, 3.3},                                   analyze("3d = 3.3                     ")),
     ?_assertMatch( {eq, 3.0, 3.3},                                   analyze("3D = 3.3                     ")),
     ?_assertMatch( {eq, 3.0, 3.3},                                   analyze("3f = 3.3                     ")),
     ?_assertMatch( {eq, 3.0, 3.3},                                   analyze("3F = 3.3                     ")),
     ?_assertMatch( {eq, {'ident', <<"JMSType">>}, <<"car">>},        analyze("JMSType = 'car'              ")),
     ?_assertMatch( {eq, {'ident', <<"colour">>}, <<"blue">>},        analyze("colour = 'blue'              ")),
     ?_assertMatch( false,                                            analyze("false                        ")),
     ?_assertMatch( false,                                            analyze("not true                     ")),
     ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze("weight >  2500               ")),
     ?_assertMatch( {lt, {'ident', <<"weight">>}, 2500},              analyze("weight <  2500               ")),
     ?_assertMatch( {gteq, {'ident', <<"weight">>}, 2500},            analyze("weight >= 2500               ")),
     ?_assertMatch( {lteq, {'ident', <<"weight">>}, 2500},            analyze("weight <= 2500               ")),
     ?_assertMatch( {gt, 2500, {'ident', <<"weight">>}},              analyze("2500 >  weight               ")),
     ?_assertMatch( {lt, 2500, {'ident', <<"weight">>}},              analyze("2500 <  weight               ")),
     ?_assertMatch( {gteq, 2500, {'ident', <<"weight">>}},            analyze("2500 >= weight               ")),
     ?_assertMatch( {lteq, 2500, {'ident', <<"weight">>}},            analyze("2500 <= weight               ")),
     ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze("true and weight > 2500       ")),
     ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze("not false and weight > 2500  ")),
     ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze("weight > 2500 and true       ")),
     ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze("weight > 2500 and not false  ")),
     ?_assertMatch( {gt, 2500, 2500},                                 analyze("2500 > 2500                  ")),
     ?_assertMatch( {gt, 2500, {'ident', <<"weight">>}},              analyze("2500 > weight                ")),
     ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze("weight > 0x9C4               ")),
     ?_assertMatch( {is_null, {'ident', <<"weight">>}},               analyze("weight is null               ")),
     ?_assertMatch( {not_null, {'ident', <<"weight">>}},              analyze("weight IS not NULL           ")),
     ?_assertMatch( {between, {'ident', <<"weight">>}, {range, 1, 10}}, analyze("weight between 1 and 10    ")),
     ?_assertMatch( {between, 2, {range, 1, 10}},                     analyze("2 between 1 and 10           ")),
     ?_assertMatch( {not_between, 2, {range, 1, 10}},                 analyze("2 not between 1 and 10       ")),
     ?_assertMatch( {like, {'ident', <<"colour">>}, {regex, _}},      analyze("colour like 'abc'            ")),
     ?_assertMatch( {like, {'ident', <<"colour">>}, {regex, _}},      analyze("colour like 'ab!__c' escape '!' ")),
     ?_assertMatch( {not_like, {'ident', <<"colour">>}, {regex, _}},  analyze("colour NOT LIKE 'abc'        ")),
     ?_assertMatch( {like, {'ident', <<"altcol">>}, {regex, _}},      analyze(" altcol     like '''bl%'     ")),
     ?_assertMatch( error,                analyze("JMSType LIKE 'car' AND colour <= 'blue' and weight > 2500")),

     ?_assertMatch( error,                                            analyze("1.0 <> false                 ")),
     ?_assertMatch( error,                                            analyze("1 <> false                   ")),
     ?_assertMatch( error,                                            analyze("-dummy <> false              ")),

     ?_assertMatch( {'ident', <<"dummy">> },                          analyze("true and dummy               ")),
     ?_assertMatch( false,                                            analyze("false and dummy              ")),
     ?_assertMatch( {'ident', <<"dummy">> },                          analyze("false or dummy               ")),
     ?_assertMatch( true,                                             analyze("true or dummy                ")),

     ?_assertMatch(
    {'and',{eq,{'ident',<<"JMSType">>},<<"car">>},{'and',{eq,{'ident',<<"colour">>},<<"blue">>}, {gt,{'ident',<<"weight">>},2500}}},
     analyze("JMSType = 'car' AND colour = 'blue' AND weight > 2500")),

     ?_assertMatch(
    {'or',{eq,{'ident',<<"JMSType">>},<<"car">>},{'or',{eq,{'ident',<<"colour">>},<<"blue">>},{gt,{'ident',<<"weight">>},2500}}},
     analyze("JMSType = 'car' OR colour = 'blue' OR weight > 2500")),

     ?_assertMatch(
    {'or',{'or',{eq,{'ident',<<"JMSType">>},<<"car">>},{eq,{'ident',<<"colour">>},<<"blue">>}},{gt,{'ident',<<"weight">>},2500}},
     analyze("(JMSType = 'car' OR colour = 'blue') OR weight > 2500")),

     ?_assertMatch(
    {'or',{eq,{'ident',<<"JMSType">>},<<"car">>},{'and',{eq,{'ident',<<"colour">>},<<"blue">>},{gt,{'ident',<<"weight">>},2500}}},
     analyze("JMSType = 'car' OR colour = 'blue' AND weight > 2500"))].
