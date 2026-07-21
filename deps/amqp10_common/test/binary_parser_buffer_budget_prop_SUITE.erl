%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(binary_parser_buffer_budget_prop_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").

-import(rabbit_ct_proper_helpers, [run_proper/3]).

%% Mirrors ?MAX_BUFFER_BUDGET in amqp10_binary_parser.
-define(BUDGET, 100_000).

all() ->
    [{group, tests}].

groups() ->
    [{tests, [parallel],
      [
       prop_element_count_matches_budget,
       prop_allocation_bounded
      ]}].

%% A sequence of zero-width arrays parses exactly when its total element count
%% fits the budget, and then materializes exactly that many elements.
prop_element_count_matches_budget(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 Counts, list(integer(0, 20000)),
                 begin
                     Total = lists:sum(Counts),
                     case catch amqp10_binary_parser:parse(zw_arrays_in_list(Counts)) of
                         {'EXIT', {failed_to_parse_array_buffer_budget_exceeded, _, _}} ->
                             Total > ?BUDGET;
                         {{list, Arrays}, _} ->
                             Materialized = lists:sum([length(E) || {array, _, E} <- Arrays]),
                             Total =< ?BUDGET andalso Materialized =:= Total
                     end
                 end)
      end, [], 300).

%% Under an oversized per-array count, parsing in a heap-capped process still
%% terminates normally rather than being killed for exceeding the heap.
prop_allocation_bounded(_Config) ->
    run_proper(
      fun() ->
              ?FORALL(
                 Counts,
                 non_empty(list(oneof([integer(0, 20000), 16#FFFFFFFF]))),
                 parses_within_heap(zw_arrays_in_list(Counts), 64)
              )
      end, [], 300).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

zw_array(Count) ->
    <<16#f0, 5:32, Count:32, 16#40>>.

zw_arrays_in_list(Counts) ->
    Inner = iolist_to_binary([zw_array(C) || C <- Counts]),
    <<16#d0, (4 + byte_size(Inner)):32, (length(Counts)):32, Inner/binary>>.

parses_within_heap(Bin, MiB) ->
    Self = self(),
    Words = MiB * 1024 * 1024 div erlang:system_info(wordsize),
    {Pid, Ref} = spawn_monitor(
                   fun() ->
                           process_flag(max_heap_size,
                                        #{size => Words, kill => true, error_logger => false}),
                           _ = catch amqp10_binary_parser:parse(Bin),
                           Self ! {done, self()}
                   end),
    receive
        {done, Pid} ->
            erlang:demonitor(Ref, [flush]),
            true;
        {'DOWN', Ref, process, Pid, _Reason} ->
            false
    after 30000 ->
              false
    end.
