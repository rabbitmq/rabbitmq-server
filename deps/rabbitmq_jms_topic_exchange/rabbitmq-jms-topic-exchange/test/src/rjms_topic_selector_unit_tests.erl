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
                                   , add_binding/3
                                   , remove_bindings/3
                                   , assert_args_equivalence/2]).

dummy_test() ->
    io:format("~n~ntest_dummy run~n~n"),
    ?assertEqual(ok,ok).
