%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2012-2023 VMware, Inc. or its affiliates.  All rights reserved.
%% -----------------------------------------------------------------------------

%% Unit test file for RJMS Topic Selector plugin

%% -----------------------------------------------------------------------------

-module(rjms_topic_selector_unit_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_jms_topic_exchange.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_jms_topic_exchange, [ description/0
                                   , serialise_events/0
                                   , validate/1
                                   , validate_binding/2 ]).


all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
                                    description_test,
                                    serialise_events_test,
                                    validate_test,
                                    validate_binding_test
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

description_test(_Config) ->
  ?assertMatch([{name, _}, {description, _}], description()).

serialise_events_test(_Config) ->
  ?assertMatch(false, serialise_events()).

validate_test(_Config) ->
  ?assertEqual(ok, validate(dummy_exchange())).

validate_binding_test(_Config) ->
  ?assertEqual(ok, validate_binding(dummy_exchange(), dummy_binding())).

dummy_exchange() ->
  #exchange{name = <<"XName">>, arguments = []}.

dummy_binding() ->
  #binding{ key = <<"BindingKey">>
          , destination = #resource{name = <<"DName">>}
          , args = [{?RJMS_COMPILED_SELECTOR_ARG, longstr, <<"<<\"false\">>.">>}]}.
