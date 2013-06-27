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

%% Unit Tests for sjx_parser and, indirectly, sjx_dialect.

%% -----------------------------------------------------------------------------
-module(sjx_parser_tests).
-include_lib("eunit/include/eunit.hrl").

-import(sjx_dialect, [analyze/2]).

%% Fixed type info for identifiers
%%
-define(TEST_TYPE_INFO,
[ {<<"JMSType">>,          longstr, <<"string">>}
, {<<"JMSCorrelationID">>, longstr, <<"string">>}
, {<<"JMSMessageID">>,     longstr, <<"string">>}
, {<<"JMSDeliveryMode">>,  array,
     [{longstr, <<"PERSISTENT">>}, {longstr, <<"NON_PERSISTENT">>}]}
, {<<"JMSPriority">>,      longstr, <<"number">>}
, {<<"JMSTimestamp">>,     longstr, <<"number">>}
, {<<"TestBoolean">>,      longstr, <<"boolean">>}
]).

basic_parse_test_() ->
    [ ?_assertMatch( {eq, <<"car">>, <<"blue">>},                      analyze(?TEST_TYPE_INFO, "'car' = 'blue'               "))
    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "'car' > 'blue'               "))
    , ?_assertMatch( {eq, 3.2, 3.3},                                   analyze(?TEST_TYPE_INFO, "3.2 = 3.3                    "))
    , ?_assertMatch( {eq, 3.0, 3.3},                                   analyze(?TEST_TYPE_INFO, "3. = 3.3                     "))
    , ?_assertMatch( {eq, 3.0, 3.3},                                   analyze(?TEST_TYPE_INFO, "3d = 3.3                     "))
    , ?_assertMatch( {eq, 3.0, 3.3},                                   analyze(?TEST_TYPE_INFO, "3D = 3.3                     "))
    , ?_assertMatch( {eq, 3.0, 3.3},                                   analyze(?TEST_TYPE_INFO, "3f = 3.3                     "))
    , ?_assertMatch( {eq, 3.0, 3.3},                                   analyze(?TEST_TYPE_INFO, "3F = 3.3                     "))
    , ?_assertMatch( {eq, {'ident', <<"JMSType">>}, <<"car">>},        analyze(?TEST_TYPE_INFO, "JMSType = 'car'              "))
    , ?_assertMatch( {eq, {'ident', <<"colour">>}, <<"blue">>},        analyze(?TEST_TYPE_INFO, "colour = 'blue'              "))
    , ?_assertMatch( false,                                            analyze(?TEST_TYPE_INFO, "false                        "))
    , ?_assertMatch( false,                                            analyze(?TEST_TYPE_INFO, "not true                     "))
    , ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "weight >  2500               "))
    , ?_assertMatch( {lt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "weight <  2500               "))
    , ?_assertMatch( {gteq, {'ident', <<"weight">>}, 2500},            analyze(?TEST_TYPE_INFO, "weight >= 2500               "))
    , ?_assertMatch( {lteq, {'ident', <<"weight">>}, 2500},            analyze(?TEST_TYPE_INFO, "weight <= 2500               "))
    , ?_assertMatch( {gt, 2500, {'ident', <<"weight">>}},              analyze(?TEST_TYPE_INFO, "2500 >  weight               "))
    , ?_assertMatch( {lt, 2500, {'ident', <<"weight">>}},              analyze(?TEST_TYPE_INFO, "2500 <  weight               "))
    , ?_assertMatch( {gteq, 2500, {'ident', <<"weight">>}},            analyze(?TEST_TYPE_INFO, "2500 >= weight               "))
    , ?_assertMatch( {lteq, 2500, {'ident', <<"weight">>}},            analyze(?TEST_TYPE_INFO, "2500 <= weight               "))
    , ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "true and weight > 2500       "))
    , ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "not false and weight > 2500  "))
    , ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "weight > 2500 and true       "))
    , ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "weight > 2500 and not false  "))
    , ?_assertMatch( {gt, 2500, 2500},                                 analyze(?TEST_TYPE_INFO, "2500 > 2500                  "))
    , ?_assertMatch( {gt, 2500, {'ident', <<"weight">>}},              analyze(?TEST_TYPE_INFO, "2500 > weight                "))
    , ?_assertMatch( {gt, {'ident', <<"weight">>}, 2500},              analyze(?TEST_TYPE_INFO, "weight > 0x9C4               "))
    , ?_assertMatch( {is_null, {'ident', <<"weight">>}},               analyze(?TEST_TYPE_INFO, "weight is null               "))
    , ?_assertMatch( {not_null, {'ident', <<"weight">>}},              analyze(?TEST_TYPE_INFO, "weight IS not NULL           "))
    , ?_assertMatch( {between, {'ident', <<"weight">>}, {range, 1, 10}}, analyze(?TEST_TYPE_INFO, "weight between 1 and 10    "))
    , ?_assertMatch( {between, 2, {range, 1, 10}},                     analyze(?TEST_TYPE_INFO, "2 between 1 and 10           "))
    , ?_assertMatch( {not_between, 2, {range, 1, 10}},                 analyze(?TEST_TYPE_INFO, "2 not between 1 and 10       "))
    , ?_assertMatch( {like, {'ident', <<"colour">>}, {regex, _}},      analyze(?TEST_TYPE_INFO, "colour like 'abc'            "))
    , ?_assertMatch( {like, {'ident', <<"colour">>}, {regex, _}},      analyze(?TEST_TYPE_INFO, "colour like 'ab!__c' escape '!' "))
    , ?_assertMatch( {not_like, {'ident', <<"colour">>}, {regex, _}},  analyze(?TEST_TYPE_INFO, "colour NOT LIKE 'abc'        "))
    , ?_assertMatch( {like, {'ident', <<"altcol">>}, {regex, _}},      analyze(?TEST_TYPE_INFO, " altcol     like '''bl%'     "))
    , ?_assertMatch( error,                analyze(?TEST_TYPE_INFO, "JMSType LIKE 'car' AND colour <= 'blue' and weight > 2500"))
    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "1.0 <> false                 "))
    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "1 <> false                   "))
    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "-dummy <> false              "))

    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "-TestBoolean = 0             "))
    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "TestBoolean >= 0             "))
    , ?_assertMatch( error,                                            analyze(?TEST_TYPE_INFO, "not JMSPriority              "))
    , ?_assertMatch( {'not', {'ident', <<"TestBoolean">>}},            analyze(?TEST_TYPE_INFO, "not TestBoolean              "))

    , ?_assertMatch( {'ident', <<"dummy">> },                          analyze(?TEST_TYPE_INFO, "true and dummy               "))
    , ?_assertMatch( false,                                            analyze(?TEST_TYPE_INFO, "false and dummy              "))
    , ?_assertMatch( {'ident', <<"dummy">> },                          analyze(?TEST_TYPE_INFO, "false or dummy               "))
    , ?_assertMatch( true,                                             analyze(?TEST_TYPE_INFO, "true or dummy                "))
    , ?_assertMatch(
        {'and',{eq,{'ident',<<"JMSType">>},<<"car">>},{'and',{eq,{'ident',<<"colour">>},<<"blue">>}, {gt,{'ident',<<"weight">>},2500}}}
      , analyze(?TEST_TYPE_INFO, "JMSType = 'car' AND colour = 'blue' AND weight > 2500")
      )
    , ?_assertMatch(
        {'or',{eq,{'ident',<<"JMSType">>},<<"car">>},{'or',{eq,{'ident',<<"colour">>},<<"blue">>},{gt,{'ident',<<"weight">>},2500}}}
      , analyze(?TEST_TYPE_INFO, "JMSType = 'car' OR colour = 'blue' OR weight > 2500")
      )
    , ?_assertMatch(
        {'or',{'or',{eq,{'ident',<<"JMSType">>},<<"car">>},{eq,{'ident',<<"colour">>},<<"blue">>}},{gt,{'ident',<<"weight">>},2500}}
      , analyze(?TEST_TYPE_INFO, "(JMSType = 'car' OR colour = 'blue') OR weight > 2500")
      )
    , ?_assertMatch(
        {'or',{eq,{'ident',<<"JMSType">>},<<"car">>},{'and',{eq,{'ident',<<"colour">>},<<"blue">>},{gt,{'ident',<<"weight">>},2500}}}
      , analyze(?TEST_TYPE_INFO, "JMSType = 'car' OR colour = 'blue' AND weight > 2500")
      )
    , ?_assertMatch({eq,{ident,<<"JMSDeliveryMode">>}, <<"PERSISTENT">>},              analyze(?TEST_TYPE_INFO, "JMSDeliveryMode = 'PERSISTENT'    "))
    , ?_assertMatch({eq,<<"PERSISTENT">>, {ident,<<"JMSDeliveryMode">>}},              analyze(?TEST_TYPE_INFO, "'PERSISTENT'    = JMSDeliveryMode "))
    , ?_assertMatch({eq,{ident,<<"JMSDeliveryMode">>}, {ident,<<"JMSDeliveryMode">>}}, analyze(?TEST_TYPE_INFO, "JMSDeliveryMode = JMSDeliveryMode "))
    , ?_assertMatch({eq,{ident,<<"id.with.periods.in">>}, <<"TEST">>},                 analyze(?TEST_TYPE_INFO, "id.with.periods.in = 'TEST'       "))
    , ?_assertMatch(error,                                                             analyze(?TEST_TYPE_INFO, "JMSDeliveryMode = 'non_persistent'"))
    ].
