%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Unit tests for what the Native STOMP processor logs when a SUBSCRIBE fails
%% after declaring a new queue.
-module(unit_subscribe_failure_log_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SECRET, <<"message body and passcode">>).

all() ->
    [reason_keeps_atoms_and_integers,
     reason_drops_everything_else,
     stacktrace_drops_arguments].

reason_keeps_atoms_and_integers(_Config) ->
    ?assertEqual({case_clause, {error, timeout}},
                 rabbit_stomp_processor:failure_reason({case_clause, {error, timeout}})),
    ?assertEqual({timeout, {gen_server, call, '_'}},
                 rabbit_stomp_processor:failure_reason(
                   {timeout, {gen_server, call, [self(), ping, 5000]}})).

reason_drops_everything_else(_Config) ->
    State = {state, #{default_passcode => ?SECRET}},
    ?assertEqual({badmatch, {error, '_', '_', {state, '_'}}},
                 rabbit_stomp_processor:failure_reason(
                   {badmatch, {error, "Failed to consume", "refused", State}})),
    ?assertEqual({{badmatch, '_'}, '_'},
                 rabbit_stomp_processor:failure_reason({{badmatch, ?SECRET}, [frame]})),
    ?assertEqual('_', rabbit_stomp_processor:failure_reason(list_to_tuple(lists:seq(1, 9)))),
    ?assertEqual({a, {b, {c, {d, '_'}}}},
                 rabbit_stomp_processor:failure_reason({a, {b, {c, {d, {e, f}}}}})).

stacktrace_drops_arguments(_Config) ->
    Location = [{file, "rabbit_stomp_processor.erl"}, {line, 1},
                {error_info, #{cause => ?SECRET}}],
    ?assertEqual([{rabbit_stomp_processor, do_subscribe, 2,
                   [{file, "rabbit_stomp_processor.erl"}, {line, 1}]},
                  {rabbit_binding, add, 3, [{file, "rabbit_binding.erl"}, {line, 2}]}],
                 rabbit_stomp_processor:failure_stacktrace(
                   [{rabbit_stomp_processor, do_subscribe, [frame, ?SECRET], Location},
                    {rabbit_binding, add, 3, [{file, "rabbit_binding.erl"}, {line, 2}]}])).
