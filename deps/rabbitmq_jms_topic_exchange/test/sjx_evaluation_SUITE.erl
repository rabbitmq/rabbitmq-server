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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2012, 2013 Pivotal Software, Inc.  All rights reserved.
%% -----------------------------------------------------------------------------
%% Derived from works which were:
%% Copyright (c) 2012, 2013 Steve Powell (Zteve.Powell@gmail.com)
%% -----------------------------------------------------------------------------

%% Tests for sjx_evaluator

%% -----------------------------------------------------------------------------
-module(sjx_evaluation_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(sjx_evaluator, [evaluate/2]).

%% Fixed type info for identifiers
%%
-define(TEST_TYPE_INFO,
[ {<<"JMSType">>,          longstr, <<"string">>}
, {<<"JMSCorrelationID">>, longstr, <<"string">>}
, {<<"JMSMessageID">>,     longstr, <<"string">>}
, {<<"JMSDeliveryMode">>,  longstr, <<"string">>}
, {<<"JMSPriority">>,      longstr, <<"number">>}
, {<<"JMSTimestamp">>,     longstr, <<"number">>}
]).


all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                basic_evaluate_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

basic_evaluate_test(_Config) ->
    Hs = [{<<"JMSType">>, longstr, <<"car">>},
          {<<"colour">>, longstr, <<"blue">>},
          {<<"altcol">>, longstr, <<"'blue">>},
          {<<"likevar">>, longstr, <<"bl_ue">>},
          {<<"weight">>, signedint, 2501},
          {<<"WeIgHt">>, signedint, 2},
          {<<"afloat">>, float, 3.0e-2},
          {<<"abool">>, bool, false}],

    [ ?_assert(    eval(Hs, {'=', {'ident', <<"JMSType">>}, <<"car">>}                      ))
    , ?_assert(not eval(Hs, {'ident', <<"abool">>}                                          ))
    , ?_assert(    eval(Hs, {'not', {'ident', <<"abool">>}}                                 ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"colour">>}, <<"blue">>}                      ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"weight">>}, 2501}                            ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"WeIgHt">>}, 2}                               ))
    , ?_assert(    eval(Hs, {'=', 2501, {'ident', <<"weight">>}}                            ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"afloat">>}, 3.0e-2}                          ))
    , ?_assert(    eval(Hs, {'>', {'ident', <<"weight">>}, 2500}                            ))
    , ?_assert(    eval(Hs, {'<', {'ident', <<"weight">>}, 2502}                            ))
    , ?_assert(    eval(Hs, {'>=', {'ident', <<"weight">>}, 2501}                           ))
    , ?_assert(    eval(Hs, {'<=', {'ident', <<"weight">>}, 2501}                           ))
    , ?_assert(not eval(Hs, {'<=', {'ident', <<"weight">>}, 2500}                           ))
    , ?_assert(    eval(Hs, {'between', {'ident', <<"weight">>}, {'range', 0, 2501}}        ))
    , ?_assert(    eval(Hs, {'between', {'ident', <<"weight">>}, {'range', 2500, 2501}}     ))
    , ?_assert(    eval(Hs, {'between', 17, {'range', 17, 18}}                              ))
    , ?_assert(    eval(Hs, {'between', 17, {'range', 17, 17}}                              ))
    , ?_assert(    eval(Hs, {'not_between', 16, {'range', 17, 18}}                          ))
    , ?_assert(    eval(Hs, {'<', 2500, {'ident', <<"weight">>}}                            ))
    , ?_assert(    eval(Hs, {'>', 2502, {'ident', <<"weight">>}}                            ))
    , ?_assert(    eval(Hs, {'<=', 2500, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'>=', 2502, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'<=', 2501, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'>=', 2501, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"colour">>}, {<<"bl%">>, 'no_escape'}}     ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"likevar">>}, {<<"b_!_ue">>, <<"!">>}}     ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"colour">>}, {<<"bl_e">>, 'no_escape'}}    ))
    , ?_assert(    eval(Hs, {'not_like', {'ident', <<"colour">>}, {<<"l%">>, 'no_escape'}}  ))
    , ?_assert(not eval(Hs, {'not_like', {'ident', <<"colour">>}, {<<"bl%">>, 'no_escape'}} ))
    , ?_assert(    eval(Hs, {'in', {'ident', <<"colour">>}, [<<"blue">>, <<"green">>]}      ))
    , ?_assert(not eval(Hs, {'not_in', {'ident', <<"colour">>}, [<<"green">>, <<"blue">>]}  ))
    , ?_assert(not eval(Hs, {'in', {'ident', <<"colour">>}, [<<"bleen">>, <<"grue">>]}      ))
    , ?_assert(    eval(Hs, {'not_in', {'ident', <<"colour">>}, [<<"grue">>, <<"bleen">>]}  ))
    , ?_assert(    eval(Hs, {'not_like', {'ident', <<"altcol">>}, {<<"bl%">>, 'no_escape'}} ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"altcol">>}, {<<"'bl%">>, 'no_escape'}}    ))
    , ?_assert(    eval(Hs, {'or', {'and', {'like', {'ident', <<"colour">>}, {<<"bl%">>, 'no_escape'}}
                                         , {'>', {'ident', <<"weight">>}, 2500}}
                                 , false}                                                   ))
    , ?_assert(undefined =:= eval(Hs, {'<=', {'ident', <<"missing">>}, 2500}                ))
    , ?_assert(undefined =:= eval(Hs, {'in', {'ident', <<"missing">>}, [<<"blue">>]}        ))
    ].

eval(Hs, S) -> evaluate(S, Hs).
