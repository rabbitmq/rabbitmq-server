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
     distinct_pids_not_deduplicated,
     new_name_has_reply_to_prefix,
     new_name_calls_gen_secure_not_gen,
     new_name_values_differ_under_stub,
     new_name_key_not_derivable_from_predecessor
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

%% `gen_secure/0` needs the `rabbit_guid` gen_server, which this suite
%% doesn't start, so stub it with a real random source instead.
with_stubbed_gen_secure(Fun) ->
    ok = meck:new(rabbit_guid, [passthrough]),
    meck:expect(rabbit_guid, gen_secure, fun() -> crypto:strong_rand_bytes(16) end),
    try
        Fun()
    after
        meck:unload(rabbit_guid)
    end.

new_name_has_reply_to_prefix(_Config) ->
    with_stubbed_gen_secure(
      fun() ->
              Name = rabbit_volatile_queue:new_name(),
              ?assertMatch(<<"amq.rabbitmq.reply-to.", _/binary>>, Name)
      end).

%% `gen/0`'s own docstring calls it predictable after the first call.
new_name_calls_gen_secure_not_gen(_Config) ->
    with_stubbed_gen_secure(
      fun() ->
              _ = rabbit_volatile_queue:new_name(),
              ?assert(meck:called(rabbit_guid, gen_secure, [])),
              ?assertNot(meck:called(rabbit_guid, gen, []))
      end).

%% A smoke test, not a security check: `gen/0` never repeats a value
%% either, so distinctness alone passes under both generators.
new_name_values_differ_under_stub(_Config) ->
    with_stubbed_gen_secure(
      fun() ->
              N = 1000,
              Names = [rabbit_volatile_queue:new_name() || _ <- lists:seq(1, N)],
              ?assertEqual(N, length(lists:usort(Names)))
      end).

%% Needs the real generator: the property is specific to `gen/0`'s XOR
%% chaining, so block 4 of one output combined with block 1 of the next
%% reproduces block 2 of the first, regardless of the hash
%% `advance_blocks/2` picks. `gen_secure/0` has no such relation. Passing
%% rules out a regression to `gen/0`; it says nothing about how hard
%% `gen_secure/0` is to guess on its own.
new_name_key_not_derivable_from_predecessor(_Config) ->
    %% Both generators seed from the `rabbit_guid` server on first use;
    %% `init([Serial])` takes it directly, bypassing the on-disk file.
    {ok, Server} = gen_server:start({local, rabbit_guid}, rabbit_guid, [0], []),
    try
        Key1 = reply_to_suffix(rabbit_volatile_queue:new_name()),
        Key2 = reply_to_suffix(rabbit_volatile_queue:new_name()),
        ?assertNotEqual(Key1, Key2),
        <<_:32, K1B2:32, K1B3:32, K1B4:32>> = Key1,
        <<K2B1:32, K2B2:32, K2B3:32, K2B4:32>> = Key2,
        ?assertNotEqual(K1B2, K2B1 bxor K2B4),
        ?assertNotEqual(K1B3, K2B2 bxor K2B4),
        ?assertNotEqual(K1B4, K2B3 bxor K2B4)
    after
        gen_server:stop(Server)
    end.

reply_to_suffix(<<"amq.rabbitmq.reply-to.", Rest/binary>>) ->
    [_EncodedPid, EncodedKey] = binary:split(Rest, <<".">>),
    Key = base64:decode(EncodedKey),
    ?assertEqual(16, byte_size(Key)),
    Key.

targets(Pid, N) ->
    [target(Pid, I) || I <- lists:seq(1, N)].

target(Pid, I) ->
    NameBin = iolist_to_binary(
                ["amq.rabbitmq.reply-to.PID.forged-", integer_to_list(I)]),
    Name = rabbit_misc:r(<<"/">>, queue, NameBin),
    amqqueue:new_target(Name, {rabbit_volatile_queue, Pid, none}).
