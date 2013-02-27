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
%% The Original Code is rabbitmq_jms_topic_exchange.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2013 VMware, Inc; Eduard Sergeev.
%% All rights reserved.
%%

-module(rjms_topic_selector_tests).

-import(rabbit_jms_topic_exchange, [ description/0
                                   , serialise_events/0
                                   , route/2
                                   , validate/1
                                   , create/2
                                   , delete/3
                                   , add_binding/3
                                   , remove_bindings/3
                                   , assert_args_equivalence/2]).

-compile(export_all).

test_dummy() ->
    io:format("~n~ntest_dummy run~n~n").

test() ->
    test:test([{?MODULE, [test_dummy]}],
              [report, {name, ?MODULE}]).
