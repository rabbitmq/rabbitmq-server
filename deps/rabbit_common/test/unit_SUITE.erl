%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_memory.hrl").
-include("rabbit.hrl").

-compile(export_all).

%% This cipher is listed as supported, but doesn't actually work.
%% OTP bug: https://bugs.erlang.org/browse/ERL-1478
-define(SKIPPED_CIPHERS, [aes_ige256]).

all() ->
    [
        {group, parallel_tests},
        {group, parse_mem_limit},
        {group, gen_server2}
    ].

groups() ->
    [
        {parallel_tests, [parallel], [
            data_coercion_to_proplist,
            data_coercion_to_list,
            data_coercion_to_map,
            data_coercion_atomize_keys_proplist,
            data_coercion_atomize_keys_map,
            pget,
            encrypt_decrypt,
            encrypt_decrypt_term,
            version_equivalence,
            pid_decompose_compose,
            platform_and_version,
            frame_encoding_does_not_fail_with_empty_binary_payload,
            amqp_table_conversion,
            name_type,
            get_erl_path,
            date_time_parse_duration
        ]},
        {parse_mem_limit, [parallel], [
            parse_mem_limit_relative_exactly_max,
            parse_mem_relative_above_max,
            parse_mem_relative_integer,
            parse_mem_relative_invalid
        ]},
        {gen_server2, [parallel], [
            stats_timer_is_working,
            stats_timer_writes_gen_server2_metrics_if_core_metrics_ets_exists,
            stop_stats_timer_on_hibernation,
            stop_stats_timer_on_backoff,
            stop_stats_timer_on_backoff_when_backoff_less_than_stats_timeout,
            gen_server2_stop
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(_, Config) -> Config.

end_per_testcase(stats_timer_is_working, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stop_stats_timer_on_hibernation, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stop_stats_timer_on_backoff, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stop_stats_timer_on_backoff_when_backoff_less_than_stats_timeout, Config) ->
    reset_stats_interval(),
    Config;
end_per_testcase(stats_timer_writes_gen_server2_metrics_if_core_metrics_ets_exists, Config) ->
    rabbit_core_metrics:terminate(),
    reset_stats_interval(),
    Config;
end_per_testcase(_, Config) -> Config.

stats_timer_is_working(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    {ok, TestServer} = gen_server2_test_server:start_link(count_stats),
    %% Start the emission
    % TestServer ! emit_gen_server2_stats,

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount = gen_server2_test_server:stats_count(TestServer),
    ?assertEqual(4, StatsCount).

stats_timer_writes_gen_server2_metrics_if_core_metrics_ets_exists(_) ->
    rabbit_core_metrics:init(),

    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    {ok, TestServer} = gen_server2_test_server:start_link(),
    timer:sleep(StatsInterval * 4),

    %% No messages in the buffer
    ?assertEqual(0, rabbit_core_metrics:get_gen_server2_stats(TestServer)),

    %% Sleep to accumulate messages
    gen_server2:cast(TestServer, {sleep, StatsInterval + 100}),

    %% Sleep to get results
    gen_server2:cast(TestServer, {sleep, 1000}),
    gen_server2:cast(TestServer, ignore),
    gen_server2:cast(TestServer, ignore),
    gen_server2:cast(TestServer, ignore),

    timer:sleep(StatsInterval + 150),
    ?assertEqual(4, rabbit_core_metrics:get_gen_server2_stats(TestServer)).

stop_stats_timer_on_hibernation(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    %% No backoff configured
    {ok, TestServer} = gen_server2_test_server:start_link(count_stats),

    ?assertEqual(ok, gen_server2:call(TestServer, hibernate)),

    timer:sleep(50),

    ?assertEqual({current_function,{erlang, hibernate, 3}},
                 erlang:process_info(TestServer, current_function)),

    timer:sleep(StatsInterval * 6 + 100),
    StatsCount1 = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    ?assertEqual(1, StatsCount1),

    %% A message will wake up the process
    gen_server2:call(TestServer, wake_up),
    gen_server2:call(TestServer, wake_up),
    gen_server2:call(TestServer, wake_up),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount5 = gen_server2_test_server:stats_count(TestServer),
    ?assertEqual(5, StatsCount5),
    ?assertEqual(ok, gen_server2:call(TestServer, hibernate)),

    timer:sleep(50),

    {current_function,{erlang,hibernate,3}} =
        erlang:process_info(TestServer, current_function),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount6 = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    6 = StatsCount6.

stop_stats_timer_on_backoff(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    Backoff = 1000,
    {ok, TestServer} =
        gen_server2_test_server:start_link(
            count_stats,
            {backoff, Backoff, Backoff, 10000}),

    ok = gen_server2:call(TestServer, hibernate),

    {current_function,{gen_server2,process_next_msg,1}} =
        erlang:process_info(TestServer, current_function),

    %% Receiving messages during backoff period does not emit stats
    timer:sleep(Backoff div 2),
    ok = gen_server2:call(TestServer, hibernate),

    timer:sleep(Backoff div 2 + 50),
    ?assertEqual({current_function,{gen_server2,process_next_msg,1}},
                 erlang:process_info(TestServer, current_function)),

    %% Hibernate after backoff time after last message
    timer:sleep(Backoff div 2),
    ?assertEqual({current_function,{erlang,hibernate,3}},
                 erlang:process_info(TestServer, current_function)),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    ?assertEqual(1, StatsCount),

    %% A message will wake up the process
    gen_server2:call(TestServer, wake_up),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount5 = gen_server2_test_server:stats_count(TestServer),
    ?assertEqual(5, StatsCount5).

stop_stats_timer_on_backoff_when_backoff_less_than_stats_timeout(_) ->
    StatsInterval = 300,
    set_stats_interval(StatsInterval),

    Backoff = 200,
    {ok, TestServer} =
        gen_server2_test_server:start_link(
            count_stats,
            {backoff, Backoff, Backoff, 10000}),

    ?assertEqual(ok, gen_server2:call(TestServer, hibernate)),

    ?assertEqual({current_function, {gen_server2, process_next_msg, 1}},
        erlang:process_info(TestServer, current_function)),

    timer:sleep(Backoff + 50),

    ?assertEqual({current_function, {erlang, hibernate, 3}},
                 erlang:process_info(TestServer, current_function)),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount = gen_server2_test_server:stats_count(TestServer),
    %% The timer was stopped. No stats collected
    %% The count is 1 because hibernation emits stats
    ?assertEqual(1, StatsCount),

    %% A message will wake up the process
    gen_server2:call(TestServer, wake_up),

    timer:sleep(StatsInterval * 4 + 100),
    StatsCount5 = gen_server2_test_server:stats_count(TestServer),
    ?assertEqual(5, StatsCount5).

gen_server2_stop(_) ->
    {ok, TestServer} = gen_server2_test_server:start_link(),
    ?assertEqual(ok, gen_server2:stop(TestServer)),
    ?assertEqual(false, erlang:is_process_alive(TestServer)),
    ?assertEqual({'EXIT', noproc}, (catch gen_server:stop(TestServer))),
    ok.

parse_mem_limit_relative_exactly_max(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit(1.0),
    case MemLimit of
        ?MAX_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?MAX_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

parse_mem_relative_above_max(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit(1.01),
    case MemLimit of
        ?MAX_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?MAX_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

parse_mem_relative_integer(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit(1),
    case MemLimit of
        ?MAX_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?MAX_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

parse_mem_relative_invalid(_Config) ->
    MemLimit = vm_memory_monitor:parse_mem_limit([255]),
    case MemLimit of
        ?DEFAULT_VM_MEMORY_HIGH_WATERMARK -> ok;
        _ ->    ct:fail(
                    "Expected memory limit to be ~p, but it was ~p",
                    [?DEFAULT_VM_MEMORY_HIGH_WATERMARK, MemLimit]
                )
    end.

platform_and_version(_Config) ->
    MajorVersion = erlang:system_info(otp_release),
    Result = rabbit_misc:platform_and_version(),
    RegExp = "^Erlang/OTP\s" ++ MajorVersion,
    case re:run(Result, RegExp) of
        nomatch -> ct:fail("~p does not match ~p", [Result, RegExp]);
        {error, ErrType} -> ct:fail("~p", [ErrType]);
        _ -> ok
    end.

data_coercion_to_map(_Config) ->
    ?assertEqual(#{a => 1}, rabbit_data_coercion:to_map([{a, 1}])),
    ?assertEqual(#{a => 1}, rabbit_data_coercion:to_map(#{a => 1})).

data_coercion_to_proplist(_Config) ->
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_proplist([{a, 1}])),
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_proplist(#{a => 1})).

data_coercion_atomize_keys_map(_Config) ->
    A = #{a => 1, b => 2, c => 3},
    B = rabbit_data_coercion:atomize_keys(#{a => 1, "b" => 2, <<"c">> => 3}),
    ?assertEqual(A, B).

data_coercion_atomize_keys_proplist(_Config) ->
    A = [{a, 1}, {b, 2}, {c, 3}],
    B = rabbit_data_coercion:atomize_keys([{a, 1}, {"b", 2}, {<<"c">>, 3}]),
    ?assertEqual(lists:usort(A), lists:usort(B)).

data_coercion_to_list(_Config) ->
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_list([{a, 1}])),
    ?assertEqual([{a, 1}], rabbit_data_coercion:to_list(#{a => 1})).

pget(_Config) ->
    ?assertEqual(1, rabbit_misc:pget(a, [{a, 1}])),
    ?assertEqual(undefined, rabbit_misc:pget(b, [{a, 1}])),

    ?assertEqual(1, rabbit_misc:pget(a, #{a => 1})),
    ?assertEqual(undefined, rabbit_misc:pget(b, #{a => 1})).

pid_decompose_compose(_Config) ->
    Pid = self(),
    {Node, Cre, Id, Ser} = rabbit_misc:decompose_pid(Pid),
    Node = node(Pid),
    Pid = rabbit_misc:compose_pid(Node, Cre, Id, Ser),
    OtherNode = 'some_node@localhost',
    PidOnOtherNode = rabbit_misc:pid_change_node(Pid, OtherNode),
    {OtherNode, Cre, Id, Ser} = rabbit_misc:decompose_pid(PidOnOtherNode).

encrypt_decrypt(_Config) ->
    %% Take all available block ciphers.
    Hashes = rabbit_pbe:supported_hashes(),
    Ciphers = rabbit_pbe:supported_ciphers() -- ?SKIPPED_CIPHERS,
    %% For each cipher, try to encrypt and decrypt data sizes from 0 to 64 bytes
    %% with a random passphrase.
    _ = [begin
             PassPhrase = crypto:strong_rand_bytes(16),
             Iterations = rand:uniform(100),
             Data = crypto:strong_rand_bytes(64),
             [begin
                  Expected = binary:part(Data, 0, Len),
                  Enc = rabbit_pbe:encrypt(C, H, Iterations, PassPhrase, Expected),
                  Expected = iolist_to_binary(rabbit_pbe:decrypt(C, H, Iterations, PassPhrase, Enc))
              end || Len <- lists:seq(0, byte_size(Data))]
         end || H <- Hashes, C <- Ciphers],
    ok.

encrypt_decrypt_term(_Config) ->
    %% Take all available block ciphers.
    Hashes = rabbit_pbe:supported_hashes(),
    Ciphers = rabbit_pbe:supported_ciphers() -- ?SKIPPED_CIPHERS,
    %% Different Erlang terms to try encrypting.
    DataSet = [
        10000,
        [5672],
        [{"127.0.0.1", 5672},
            {"::1",       5672}],
        [{connection, info}, {channel, info}],
        [{cacertfile,           "/path/to/testca/cacert.pem"},
            {certfile,             "/path/to/server/cert.pem"},
            {keyfile,              "/path/to/server/key.pem"},
            {verify,               verify_peer},
            {fail_if_no_peer_cert, false}],
        [<<".*">>, <<".*">>, <<".*">>]
    ],
    _ = [begin
             PassPhrase = crypto:strong_rand_bytes(16),
             Iterations = rand:uniform(100),
             Enc = rabbit_pbe:encrypt_term(C, H, Iterations, PassPhrase, Data),
             Data = rabbit_pbe:decrypt_term(C, H, Iterations, PassPhrase, Enc)
         end || H <- Hashes, C <- Ciphers, Data <- DataSet],
    ok.

version_equivalence(_Config) ->
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.0"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.1"),
    true = rabbit_misc:version_minor_equivalent("%%VSN%%", "%%VSN%%"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.0.1"),
    true = rabbit_misc:version_minor_equivalent("3.0.0.1", "3.0.0.3"),
    true = rabbit_misc:version_minor_equivalent("3.0.0.1", "3.0.1.3"),
    true = rabbit_misc:version_minor_equivalent("3.0.0", "3.0.foo"),
    false = rabbit_misc:version_minor_equivalent("3.0.0", "3.1.0"),
    false = rabbit_misc:version_minor_equivalent("3.0.0.1", "3.1.0.1"),

    false = rabbit_misc:version_minor_equivalent("3.5.7", "3.6.7"),
    false = rabbit_misc:version_minor_equivalent("3.6.5", "3.6.6"),
    false = rabbit_misc:version_minor_equivalent("3.6.6", "3.7.0"),
    true = rabbit_misc:version_minor_equivalent("3.6.7", "3.6.6"),

    %% Starting with RabbitMQ 3.7.x and feature flags introduced in
    %% RabbitMQ 3.8.x, versions are considered equivalent and the actual
    %% check is deferred to the feature flags module.
    false = rabbit_misc:version_minor_equivalent("3.6.0", "3.8.0"),
    true = rabbit_misc:version_minor_equivalent("3.7.0", "3.8.0"),
    true = rabbit_misc:version_minor_equivalent("3.7.0", "3.10.0"),

    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0.0">>),
    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0.1">>),
    true = rabbit_misc:version_minor_equivalent(<<"%%VSN%%">>, <<"%%VSN%%">>),
    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0">>),
    true = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.0.0.1">>),
    false = rabbit_misc:version_minor_equivalent(<<"3.0.0">>, <<"3.1.0">>),
    false = rabbit_misc:version_minor_equivalent(<<"3.0.0.1">>, <<"3.1.0.1">>).

frame_encoding_does_not_fail_with_empty_binary_payload(_Config) ->
    [begin
         Content = #content{
             class_id = 60, properties = none, properties_bin = <<0,0>>, protocol = rabbit_framing_amqp_0_9_1,
             payload_fragments_rev = P
         },
         ExpectedFrames = rabbit_binary_generator:build_simple_content_frames(1, Content, 0, rabbit_framing_amqp_0_9_1)
     end || {P, ExpectedFrames} <- [
                    {[], [[<<2,0,1,0,0,0,14>>,[<<0,60,0,0,0,0,0,0,0,0,0,0>>,<<0,0>>],206]]},
                    {[<<>>], [[<<2,0,1,0,0,0,14>>,[<<0,60,0,0,0,0,0,0,0,0,0,0>>,<<0,0>>],206]]},
                    {[<<"payload">>], [[<<2,0,1,0,0,0,14>>,[<<0,60,0,0,0,0,0,0,0,0,0,7>>,<<0,0>>],206],
                                       [<<3,0,1,0,0,0,7>>,[<<"payload">>],206]]}
                    ]],
    ok.

amqp_table_conversion(_Config) ->
    assert_table(#{}, []),
    assert_table(#{<<"x-expires">> => 1000},
                 [{<<"x-expires">>, long, 1000}]),
    assert_table(#{<<"x-forwarding">> =>
                   [#{<<"uri">> => <<"amqp://localhost/%2F/upstream">>}]},
                 [{<<"x-forwarding">>, array,
                   [{table, [{<<"uri">>, longstr,
                              <<"amqp://localhost/%2F/upstream">>}]}]}]).

assert_table(JSON, AMQP) ->
    ?assertEqual(JSON, rabbit_misc:amqp_table(AMQP)),
    ?assertEqual(AMQP, rabbit_misc:to_amqp_table(JSON)).


set_stats_interval(Interval) ->
    application:set_env(rabbit, collect_statistics, coarse),
    application:set_env(rabbit, collect_statistics_interval, Interval).

reset_stats_interval() ->
    application:unset_env(rabbit, collect_statistics),
    application:unset_env(rabbit, collect_statistics_interval).

name_type(_) ->
    ?assertEqual(shortnames, rabbit_nodes_common:name_type(rabbit)),
    ?assertEqual(shortnames, rabbit_nodes_common:name_type(rabbit@localhost)),
    ?assertEqual(longnames, rabbit_nodes_common:name_type('rabbit@localhost.example.com')),
    ok.

get_erl_path(_) ->
    Exe = rabbit_runtime:get_erl_path(),
    case os:type() of
        {win32, _} ->
            ?assertNotMatch(nomatch, string:find(Exe, "erl.exe"));
        _ ->
            ?assertNotMatch(nomatch, string:find(Exe, "erl"))
    end,
    ok.

date_time_parse_duration(_) ->
    ?assertEqual(
        {ok, [{sign, "+"}, {years, 6}, {months, 3}, {days, 1}, {hours, 1}, {minutes, 1}, {seconds, 1}]},
        rabbit_date_time:parse_duration("+P6Y3M1DT1H1M1.1S")
    ),
    ?assertEqual(
        {ok, [{sign, []}, {years, 0}, {months, 0}, {days, 0}, {hours, 0}, {minutes, 6}, {seconds, 0}]},
        rabbit_date_time:parse_duration("PT6M")
    ),
    ?assertEqual(
        {ok, [{sign, []}, {years, 0}, {months, 0}, {days, 0}, {hours, 0}, {minutes, 10}, {seconds, 30}]},
        rabbit_date_time:parse_duration("PT10M30S")
    ),
    ?assertEqual(
        {ok, [{sign, []}, {years, 0}, {months, 0}, {days, 5}, {hours, 8}, {minutes, 0}, {seconds, 0}]},
        rabbit_date_time:parse_duration("P5DT8H")
    ),
    ?assertEqual(error, rabbit_date_time:parse_duration("foo")),
    ok.