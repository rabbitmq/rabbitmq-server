%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_volatile_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

all() ->
    [
     single_target_correlation,
     single_target_no_correlation,
     forged_suffixes_one_pid_cast_once_correlation,
     forged_suffixes_one_pid_cast_once_no_correlation,
     distinct_pids_not_deduplicated
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    Counter = counters:new(1, []),
    ok = meck:new(delegate, [passthrough]),
    ok = meck:expect(delegate, invoke_no_result,
                     fun(_Pid, _MFA) ->
                             counters:add(Counter, 1, 1),
                             ok
                     end),
    [{counter, Counter} | Config].

end_per_testcase(_Testcase, _Config) ->
    ok = meck:unload(delegate).

single_target_correlation(Config) ->
    Counter = ?config(counter, Config),
    [Target] = targets(self(), 1),
    {[], Actions} = rabbit_volatile_queue:deliver(
                      [{Target, stateless}], msg, #{correlation => corr}),
    ?assertEqual(1, counters:get(Counter, 1)),
    ?assertMatch([{settled, _QName, [corr]}], Actions).

single_target_no_correlation(Config) ->
    Counter = ?config(counter, Config),
    [Target] = targets(self(), 1),
    ?assertEqual({[], []},
                 rabbit_volatile_queue:deliver(
                   [{Target, stateless}], msg, #{})),
    ?assertEqual(1, counters:get(Counter, 1)).

forged_suffixes_one_pid_cast_once_correlation(Config) ->
    Counter = ?config(counter, Config),
    N = 1000,
    Qs = [{T, stateless} || T <- targets(self(), N)],
    {[], Actions} = rabbit_volatile_queue:deliver(Qs, msg, #{correlation => corr}),
    ?assertEqual(1, counters:get(Counter, 1)),
    ?assertEqual(N, length(Actions)),
    ?assert(lists:all(fun({settled, _QName, [corr]}) -> true;
                         (_) -> false
                      end, Actions)),
    ?assertEqual(N, length(lists:usort([QName || {settled, QName, _} <- Actions]))).

forged_suffixes_one_pid_cast_once_no_correlation(Config) ->
    Counter = ?config(counter, Config),
    Qs = [{T, stateless} || T <- targets(self(), 1000)],
    ?assertEqual({[], []},
                 rabbit_volatile_queue:deliver(Qs, msg, #{})),
    ?assertEqual(1, counters:get(Counter, 1)).

distinct_pids_not_deduplicated(Config) ->
    Counter = ?config(counter, Config),
    N = 5,
    Pids = [spawn(fun() -> receive stop -> ok end end) || _ <- lists:seq(1, N)],
    Qs = [{target(Pid, I), stateless}
          || {Pid, I} <- lists:zip(Pids, lists:seq(1, N))],
    {[], Actions} = rabbit_volatile_queue:deliver(Qs, msg, #{correlation => corr}),
    ?assertEqual(N, counters:get(Counter, 1)),
    ?assertEqual(N, length(Actions)),
    [exit(Pid, kill) || Pid <- Pids],
    ok.

targets(Pid, N) ->
    [target(Pid, I) || I <- lists:seq(1, N)].

target(Pid, I) ->
    NameBin = iolist_to_binary(
                ["amq.rabbitmq.reply-to.PID.forged-", integer_to_list(I)]),
    Name = rabbit_misc:r(<<"/">>, queue, NameBin),
    amqqueue:new_target(Name, {rabbit_volatile_queue, Pid, none}).
