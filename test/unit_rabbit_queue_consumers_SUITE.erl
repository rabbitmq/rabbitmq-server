%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_rabbit_queue_consumers_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-import(rabbit_queue_consumers, [subtract_acks/2, subtract_acks/4, subtract_acks_impl/3]).

all() ->
    [
        {group, default_tests},
        {group, performance_tests}
    ].

groups() ->
    [
        {default_tests, [], [
            subtract_acks_single_start, subtract_acks_single_middle,
            subtract_acks_multiple_start, subtract_acks_multiple_middle, subtract_acks_multiple_several_ctags,
            ack_fifo, ack_multiple, ack_middle, ack_lifo
        ]},
        {performance_tests, [], [
            performance_queue_map
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

subtract_acks_single_start(_Config) ->
    compare(
        {ctag_count(1), ack_tags_list(2, 10)},
        subtract_acks([1], shuffle(ack_tags_list(1, 10)))
    ),
    ok.

subtract_acks_single_middle(_Config) ->
    compare(
        {ctag_count(1), ack_tags_list(1, 10, [3])},
        subtract_acks([3], shuffle(ack_tags_list(1, 10)))
    ),
    ok.

subtract_acks_multiple_start(_Config) ->
    compare(
        {ctag_count(4), [{5, <<"ctag">>}, {6, <<"ctag">>}]},
        subtract_acks([1, 2, 3, 4], shuffle(ack_tags_list(1, 6)))
    ),
    ok.

subtract_acks_multiple_middle(_Config) ->
    compare(
        {ctag_count(4), [{5, <<"ctag">>}, {6, <<"ctag">>}, {7, <<"ctag">>}, {8, <<"ctag">>}, {9, <<"ctag">>}, {10, <<"ctag">>}]},
        subtract_acks([3, 4], shuffle(ack_tags_list(1, 10)))
    ),
    ok.

subtract_acks_multiple_several_ctags(_Config) ->
    compare(
        {#{<<"ctag1">> => 2, <<"ctag2">> => 3},
            [{6, <<"ctag2">>}]},
        subtract_acks([1, 2, 3, 4, 5], [{1, <<"ctag2">>}, {2, <<"ctag1">>}, {3, <<"ctag2">>}, {4, <<"ctag1">>}, {5, <<"ctag2">>}, {6, <<"ctag2">>}])
    ),
    ok.

ack_fifo(_Config) ->
    compare({ctag_count(1), ack_tags_queue(1, 2)}, subtract_acks([0], [], maps:new(), ack_tags_queue(2))),
    ok.

ack_multiple(_Config) ->
    compare({ctag_count(4), ack_tags_queue(4, 9)}, subtract_acks([0, 1, 2, 3], [], maps:new(), ack_tags_queue(9))),
    ok.

ack_middle(_Config) ->
    compare({ctag_count(1), ack_tags_queue(0, 9, [4])}, subtract_acks([4], [], maps:new(), ack_tags_queue(9))),
    ok.

ack_lifo(_Config) ->
    compare({ctag_count(1), ack_tags_queue(0, 8)}, subtract_acks([9], [], maps:new(), ack_tags_queue(9))),
    ok.

performance_queue_map(_Config) ->
    IterationCount = 1000,
    AckCount = 2000,
    run(fun (_) -> lists:foldl(
                    fun (Tag, Acc) -> {_, AckQ} = subtract_acks([Tag], [], maps:new(), Acc), AckQ end,
                        ack_tags_queue(AckCount),
                        lists:seq(0, AckCount))
                    end, IterationCount, "Queue single ack fifo"),
    run(fun (_) -> lists:foldl(
                    fun (Tag, Acc) -> {_, AckQ} = subtract_acks_impl([Tag], maps:new(), Acc), AckQ end,
                        ack_tags_map(AckCount),
                        lists:seq(0, AckCount))
                    end, IterationCount, "Map single ack fifo"),
    run(fun (_) -> lists:foldl(
                    fun (Tag, Acc) -> {_, AckQ} = subtract_acks([Tag], [], maps:new(), Acc), AckQ end,
                        ack_tags_queue(AckCount),
                        lists:reverse(lists:seq(0, AckCount)))
                    end, IterationCount, "Queue single ack lifo"),
    run(fun (_) -> lists:foldl(
                    fun (Tag, Acc) -> {_, AckQ} = subtract_acks_impl([Tag], maps:new(), Acc), AckQ end,
                        ack_tags_map(AckCount),
                        lists:reverse(lists:seq(0, AckCount)))
                    end, IterationCount, "Map single ack lifo"),
    run(fun (_) -> subtract_acks(lists:reverse(lists:seq(0, AckCount)), [], maps:new(), ack_tags_queue(AckCount)) end,
        IterationCount, "Queue multiple ack lifo"),
    run(fun (_) -> subtract_acks_impl(lists:reverse(lists:seq(0, AckCount)), maps:new(), ack_tags_map(AckCount)) end,
        IterationCount, "Map multiple ack lifo"),
    run(fun (_) -> subtract_acks(lists:reverse(lists:seq(0, AckCount div 2)), [], maps:new(), ack_tags_queue(AckCount)) end,
        IterationCount, "Queue multiple ack in the middle"),
    run(fun (_) -> subtract_acks_impl(lists:reverse(lists:seq(0, AckCount div 2)), maps:new(), ack_tags_map(AckCount)) end,
        IterationCount, "Map multiple ack in the middle"),
    ok.

run(F, IterationCount, Msg) ->
    S = erlang:system_time(millisecond),
    lists:foreach(F, lists:seq(0, IterationCount)),
    ct:pal(Msg ++ " ~p ms~n", [erlang:system_time(millisecond) - S]).

compare({ExpectedCTagsCount, ExpectedAckQ}, Result) when is_list(ExpectedAckQ) ->
    {ExpectedCTagsCount, ExpectedAckQ} = Result;
compare({ExpectedCTagsCount, ExpectedAckQ}, Result) ->
    {CTagsCount, AckQ} = Result,
    compare({ExpectedCTagsCount, queue:to_list(ExpectedAckQ)}, {CTagsCount, queue:to_list(AckQ)}).

ack_tags_list(To) ->
    ack_tags_list(0, To).

ack_tags_list(From, To) ->
    ack_tags_list(From, To, []).

ack_tags_list(From, To, Excluded) ->
    [{Value, <<"ctag">>} || Value <- lists:filter(fun (X) -> not lists:member(X, Excluded) end, lists:seq(From, To))].

ack_tags_queue(To) ->
    ack_tags_queue(0, To).

ack_tags_queue(From, To) ->
    ack_tags_queue(From, To, []).

ack_tags_queue(From, To, Excluded) ->
    queue:from_list(ack_tags_list(From, To, Excluded)).

ack_tags_map(To) ->
    ack_tags_map(0, To).

ack_tags_map(From, To) ->
    ack_tags_map(From, To, []).

ack_tags_map(From, To, Excluded) ->
    maps:from_list(ack_tags_list(From, To, Excluded)).

ctag_count(Count) ->
    #{ <<"ctag">> => Count}.

shuffle(L) ->
    [Y || {_,Y} <- lists:sort([ {rand:uniform(), N} || N <- L])].


