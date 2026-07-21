%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(binary_parser_buffer_budget_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%% Mirrors ?MAX_BUFFER_BUDGET in amqp10_binary_parser.
-define(BUDGET, 100_000).

all() ->
    [{group, tests}].

all_tests() ->
    [
     many_arrays_exceed_budget,
     single_array_at_budget,
     single_array_over_budget,
     oversized_count_rejected_before_allocation,
     budget_boundary,
     budget_resets_between_parses,
     budget_shared_within_parse_many,
     budget_shared_across_nested_lists,
     described_zero_width_array_counted,
     byte_backed_array_not_limited,
     allocation_bounded_under_oversized_input
    ].

groups() ->
    [{tests, [parallel], all_tests()}].

%% One zero-width null array declaring Count elements.
zw_array(Count) ->
    <<16#f0, 5:32, Count:32, 16#40>>.

list32(N, Inner) ->
    <<16#d0, (4 + byte_size(Inner)):32, N:32, Inner/binary>>.

zw_arrays_in_list(Counts) ->
    Inner = iolist_to_binary([zw_array(C) || C <- Counts]),
    list32(length(Counts), Inner).

many_arrays_exceed_budget(_Config) ->
    Input = zw_arrays_in_list(lists:duplicate(13000, 10000)),
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, _, _},
       amqp10_binary_parser:parse(Input)).

single_array_at_budget(_Config) ->
    {{array, null, Elems}, _} = amqp10_binary_parser:parse(zw_array(?BUDGET)),
    ?assertEqual(?BUDGET, length(Elems)).

single_array_over_budget(_Config) ->
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, _, _},
       amqp10_binary_parser:parse(zw_array(?BUDGET + 1))).

oversized_count_rejected_before_allocation(_Config) ->
    %% A 4-billion count is rejected without materializing the list
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, 16#FFFFFFFF, _},
       amqp10_binary_parser:parse(zw_array(16#FFFFFFFF))).

budget_boundary(_Config) ->
    AtBudget = zw_arrays_in_list(lists:duplicate(10, 10000)),
    {{list, _}, _} = amqp10_binary_parser:parse(AtBudget),
    OverBudget = zw_arrays_in_list(lists:duplicate(10, 10000) ++ [1]),
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, _, _},
       amqp10_binary_parser:parse(OverBudget)).

budget_resets_between_parses(_Config) ->
    Input = zw_array(?BUDGET),
    {{array, null, _}, _} = amqp10_binary_parser:parse(Input),
    {{array, null, Elems}, _} = amqp10_binary_parser:parse(Input),
    ?assertEqual(?BUDGET, length(Elems)).

budget_shared_within_parse_many(_Config) ->
    Half = zw_array(?BUDGET div 2),
    ?assertMatch([{array, null, _}, {array, null, _}],
                 amqp10_binary_parser:parse_many(<<Half/binary, Half/binary>>, [])),
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, _, _},
       amqp10_binary_parser:parse_many(<<Half/binary, Half/binary, Half/binary>>, [])).

budget_shared_across_nested_lists(_Config) ->
    Inner = list32(1, zw_array(60000)),
    Outer = list32(2, <<Inner/binary, Inner/binary>>),
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, _, _},
       amqp10_binary_parser:parse(Outer)).

described_zero_width_array_counted(_Config) ->
    Descriptor = <<16#a1, 1, "x">>,
    Body = <<16#00, Descriptor/binary, 16#40>>,
    Bin = <<16#f0, (4 + byte_size(Body)):32, (?BUDGET + 1):32, Body/binary>>,
    ?assertExit(
       {failed_to_parse_array_buffer_budget_exceeded, _, _},
       amqp10_binary_parser:parse(Bin)).

byte_backed_array_not_limited(_Config) ->
    %% A ubyte array larger than the budget still parses: its count is bounded by input bytes
    N = ?BUDGET + 50_000,
    Data = <<0:(N * 8)>>,
    Payload = <<N:32, 16#50, Data/binary>>,
    Bin = <<16#f0, (byte_size(Payload)):32, Payload/binary>>,
    {{array, ubyte, Elems}, _} = amqp10_binary_parser:parse(Bin),
    ?assertEqual(N, length(Elems)).

allocation_bounded_under_oversized_input(_Config) ->
    %% Parse in a process capped at 64 MiB; unbounded allocation would kill it
    %% before any result is returned.
    Input = zw_arrays_in_list(lists:duplicate(200000, 10000)),
    Self = self(),
    Words = 64 * 1024 * 1024 div erlang:system_info(wordsize),
    spawn(fun() ->
                  process_flag(max_heap_size,
                               #{size => Words, kill => true, error_logger => false}),
                  R = try amqp10_binary_parser:parse(Input)
                      catch C:E -> {C, E}
                      end,
                  Self ! {result, R}
          end),
    receive
        {result, R} ->
            ?assertMatch({exit, {failed_to_parse_array_buffer_budget_exceeded, _, _}}, R)
    after 30000 ->
              ct:fail(no_result)
    end.
