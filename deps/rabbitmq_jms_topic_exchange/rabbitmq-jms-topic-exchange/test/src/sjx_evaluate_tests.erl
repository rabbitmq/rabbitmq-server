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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2012, 2013 VMware, Inc.  All rights reserved.
%% -----------------------------------------------------------------------------
%% Derived from works which were:
%% Copyright (c) 2012, 2013 Steve Powell (Zteve.Powell@gmail.com)
%% -----------------------------------------------------------------------------

%% Tests for sjx_evaluator and, indirectly, sjx_dialect

%% -----------------------------------------------------------------------------
-module(sjx_evaluate_tests).
-include_lib("eunit/include/eunit.hrl").

-import(sjx_dialect, [analyze/2]).
-import(sjx_evaluator, [evaluate/2]).

%% Fixed type info for identifiers
%%
-define(TEST_TYPE_INFO,
[ {<<"JMSDeliveryMode">>, [<<"PERSISTENT">>, <<"NON_PERSISTENT">>]}
, {<<"JMSPriority">>, number}
, {<<"JMSMessageID">>, string}
, {<<"JMSTimestamp">>, number}
, {<<"JMSCorrelationID">>, string}
, {<<"JMSType">>, string}
]).

eval(Hs, S) -> evaluate(analyze(?TEST_TYPE_INFO, S),Hs).

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