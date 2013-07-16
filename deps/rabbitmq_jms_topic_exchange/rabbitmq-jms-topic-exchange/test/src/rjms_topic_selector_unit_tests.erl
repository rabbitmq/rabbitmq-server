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

%% Unit test file for RJMS Topic Selector plugin

%% -----------------------------------------------------------------------------

-module(rjms_topic_selector_unit_tests).

-include_lib("eunit/include/eunit.hrl").

-import(rabbit_jms_topic_exchange, [ description/0
                                   , serialise_events/0
                                   , route/2
                                   , validate/1
                                   , create/2
                                   , delete/3
                                   , validate_binding/2
                                   , add_binding/3
                                   , remove_bindings/3
                                   , assert_args_equivalence/2
                                   , policy_changed/3 ]).

description_test() ->
  ?assertMatch([{name, _}, {description, _}], description()).

serialise_events_test() ->
  ?assertMatch(false, serialise_events()).

validate_test() ->
  ?assertEqual(ok, validate(any_exchange)).

create_test() ->
  ?assertEqual(ok, create(none, any_exchange)).

delete_test() ->
  ?assertEqual(ok, delete(none, any_exchange, any_bindings)).

validate_binding_test() ->
  ?assertEqual(ok, validate_binding(any_exchange, any_bindings)).

