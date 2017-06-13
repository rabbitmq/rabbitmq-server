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
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%


-module(term_to_binary_compat_prop_SUITE).

-compile(export_all).

-include("rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

all() ->
    %% The test should run on OTP < 20 (erts < 9)
    case erts_gt_8() of
        true ->
            [string_and_binary_tuple_2_to_binary];
        false ->
            [queue_name_to_binary,
             string_and_binary_tuple_2_to_binary]
    end.

erts_gt_8() ->
    Vsn = erlang:system_info(version),
    [Maj|_] = string:tokens(Vsn, "."),
    list_to_integer(Maj) > 8.

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

string_and_binary_tuple_2_to_binary(Config) ->
    Fun = fun() -> prop_string_and_binary_tuple_2_to_binary(Config) end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 10000).

queue_name_to_binary(Config) ->
    Fun = fun () -> prop_queue_name_to_binary(Config) end,
    rabbit_ct_proper_helpers:run_proper(Fun, [], 10000).


prop_queue_name_to_binary(_Config) ->
    ?FORALL({Vhost, QName}, {binary(), binary()},
            begin
                Resource = rabbit_misc:r(Vhost, queue, QName),
                Legacy = term_to_binary_compat:queue_name_to_binary(Resource),
                Current = term_to_binary(Resource),
                Current =:= Legacy
            end).

prop_string_and_binary_tuple_2_to_binary(_Config) ->
    ?FORALL({First, Second}, {union([string(), binary()]), union([string(), binary()])},
            begin
                Tuple = {First, Second},
                Legacy = term_to_binary_compat:string_and_binary_tuple_2_to_binary(Tuple),
                Current = term_to_binary(Tuple),
                Current =:= Legacy
            end).