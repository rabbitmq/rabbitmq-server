%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2012 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_test_db_unit).

-include("rabbit_mgmt.hrl").
-include_lib("eunit/include/eunit.hrl").

gc_test() ->
    T = fun (Before, After) ->
                ?assertEqual(After, unstats(
                                      rabbit_mgmt_stats:gc(
                                        cutoff(), stats(Before))))
        end,
    %% Cut off old sample, move to base
    T({[{8999, 123}, {9000, 456}], 0},
      {[{9000, 456}], 123}),
    %% Amalgamate old samples to rounder one
    T({[{9001, 100}, {9010, 020}, {10000, 003}], 0},
      {[{10000, 123}], 0}),
    %% The same, but a bit less
    T({[{9000, 100}, {9901, 020}, {9910, 003}], 0},
      {[{9000, 100}, {9910, 023}], 0}),
    %% Nothing needs to be done
    T({[{9000, 100}, {9990, 020}, {9991, 003}], 0},
      {[{9000, 100}, {9990, 020}, {9991, 003}], 0}),
    %% Invent a newer sample that's acceptable
    T({[{9001, 10}, {9010, 02}], 0},
      {[{9100, 12}], 0}),
    %% ...but don't if it's too old
    T({[{8001, 10}, {8010, 02}], 0},
      {[], 12}),
    ok.

format_test() ->
    Interval = 10,
    T = fun ({First, Last, Incr}, Stats, Results) ->
                ?assertEqual(format(Results),
                             rabbit_mgmt_stats:format(
                               #range{first = First * 1000,
                                      last  = Last * 1000,
                                      incr  = Incr * 1000},
                               stats(Stats),
                               Interval * 1000))
        end,

    %% Just three samples, all of which we format. Note the
    %% instantaneous rate is taken from the penultimate sample.
    T({10, 30, 10}, {[{10, 10}, {20, 20}, {30, 30}], 1},
      {[{30, 61}, {20, 31}, {10, 11}], 2.0, 2.5, 103/3, 61}),

    %% Skip over the second (and ditto).
    T({10, 30, 20}, {[{10, 10}, {20, 20}, {30, 30}], 1},
      {[{30, 61}, {10, 11}], 2.0, 2.5, 36.0, 61}),

    %% Skip over some and invent some. Note that the instantaneous
    %% rate drops to 0 since the last event is now in the past.
    T({0, 40, 20}, {[{10, 10}, {20, 20}, {30, 30}], 1},
      {[{40, 61}, {20, 31}, {0, 1}], 0.0, 1.5, 31.0, 61}),

    %% And a case where the range starts after the samples
    T({20, 40, 10}, {[{10, 10}, {20, 20}, {30, 30}], 1},
      {[{40, 61}, {30, 61}, {20, 31}], 0.0, 1.5, 51.0, 61}),

    %% A single sample - which should lead to some bits not getting generated
    T({10, 10, 10}, {[{10, 10}, {20, 20}, {30, 30}], 1},
      {[{10, 11}], 0.0, 11}),

    %% No samples - which should also lead to some bits not getting generated
    T({10, 0, 10}, {[{10, 10}, {20, 20}, {30, 30}], 1},
      {[], 0.0, 1}),

    %% TODO more?
    ok.

format_no_range_test() ->
    Interval = 10,
    T = fun (Stats, Results) ->
                ?assertEqual(format(Results),
                             rabbit_mgmt_stats:format(
                               no_range, stats(Stats), Interval * 1000))
        end,

    %% Just three samples
    T({[{10, 10}, {20, 20}, {30, 30}], 1},
      {0.0, 61}),
    ok.


%%--------------------------------------------------------------------

cutoff() ->
    {[{10, 1}, {100, 10}, {1000, 100}], %% Sec
     10000000}. %% Millis

stats({Diffs, Base}) ->
    #stats{diffs = gb_trees:from_orddict(secs_to_millis(Diffs)), base = Base}.

unstats(#stats{diffs = Diffs, base = Base}) ->
    {millis_to_secs(gb_trees:to_list(Diffs)), Base}.

secs_to_millis(L) -> [{TS * 1000, S} || {TS, S} <- L].
millis_to_secs(L) -> [{TS div 1000, S} || {TS, S} <- L].

format({Rate, Count}) ->
    {[{rate,     Rate}],
     Count};

format({Samples, Rate, Count}) ->
    {[{rate,     Rate},
      {samples,  format_samples(Samples)}],
     Count};

format({Samples, Rate, AvgRate, Avg, Count}) ->
    {[{rate,     Rate},
      {samples,  format_samples(Samples)},
      {avg_rate, AvgRate},
      {avg,      Avg}],
     Count}.

format_samples(Samples) ->
    [[{sample, S}, {timestamp, TS * 1000}] || {TS, S} <- Samples].
