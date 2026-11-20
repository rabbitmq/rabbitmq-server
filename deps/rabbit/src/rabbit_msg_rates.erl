%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_msg_rates).

%% Smoothed point-in-time message rates, built on `ra_li`.

-export([new/0,
         new/1,
         new_counters/0,
         count/3,
         totals/1,
         observe/4,
         observe/5,
         observe_deltas/4,
         snapshot/1,
         snapshot/2,
         format/1,
         format/2,
         decay_ms/0]).

-define(DEFAULT_DECAY_MS, 20_000).
-define(MAX_INTERVAL_MS, 10_000).
-define(MIN_ELAPSED_MS, 1000).
%% A snapshot is not aged until it is older than this, so that a rate that is
%% refreshed every few seconds reads steady.
-define(FRESH_MS, 15_000).
-define(METRICS, [publish, deliver, redeliver, ack, reject]).

-type metric() :: publish | deliver | redeliver | ack | reject.
-type totals() :: #{metric() => non_neg_integer()}.
-opaque state() :: #{start := integer(), metric() => ra_li:state()}.
-type snapshot() :: #{timestamp := integer(),
                      decay_ms := pos_integer(),
                      metric() => float()}.

-export_type([state/0, snapshot/0, metric/0, totals/0]).

-spec decay_ms() -> pos_integer().
decay_ms() ->
    persistent_term:get(rabbit_msg_rates_decay_ms, ?DEFAULT_DECAY_MS).

-spec new() -> state().
new() ->
    new(ts()).

%% `Start' is the monotonic time from which messages are counted.
-spec new(Start :: integer()) -> state().
new(Start) ->
    Li = ra_li:new(decay_ms()),
    Rates = maps:from_keys(?METRICS, Li),
    Rates#{start => Start}.

%% Counters that a process bumps cheaply on every message, for another
%% process to read.
-spec new_counters() -> counters:counters_ref().
new_counters() ->
    counters:new(length(?METRICS), []).

-spec count(metric(), non_neg_integer(), counters:counters_ref()) -> ok.
count(Metric, Count, Counters) ->
    counters:add(Counters, index(Metric), Count).

-spec totals(counters:counters_ref()) -> totals().
totals(Counters) ->
    maps:from_list([{Metric, counters:get(Counters, index(Metric))}
                    || Metric <- ?METRICS]).

%% Records the counts of every metric, spread evenly over the last `IntervalMs'.
-spec observe_deltas(#{metric() => non_neg_integer()}, pos_integer(), integer(),
                     state()) -> state().
observe_deltas(Deltas, IntervalMs, Ts, State) ->
    maps:fold(fun(Metric, Count, Acc) ->
                      observe(Metric, Count, IntervalMs, Ts, Acc)
              end, State, maps:with(?METRICS, Deltas)).

%% Records `Count' messages spread evenly over the last `IntervalMs'.
-spec observe(metric(), non_neg_integer(), pos_integer(), state()) -> state().
observe(Metric, Count, IntervalMs, State) ->
    observe(Metric, Count, IntervalMs, ts(), State).

-spec observe(metric(), non_neg_integer(), pos_integer(), integer(), state()) ->
    state().
observe(Metric, Count, IntervalMs, Ts, State) ->
    #{Metric := Li} = State,
    DecayMs = decay_ms(),
    %% After a long gap there is no telling when the messages were counted.
    Interval = min(IntervalMs, ?MAX_INTERVAL_MS),
    Amount = Count * DecayMs / Interval *
             (1 - math:exp(-Interval / DecayMs)),
    State#{Metric := ra_li:update(Amount, Ts, Li)}.

-spec snapshot(state()) -> snapshot().
snapshot(State) ->
    snapshot(State, ts()).

%% Stamped with the wall clock so that a reader on another node can age it.
-spec snapshot(state(), integer()) -> snapshot().
snapshot(#{start := Start} = State, Ts) ->
    DecayMs = decay_ms(),
    %% The integrator starts at zero, so it under-reads until it has been
    %% fed for a few decay times.
    Elapsed = max(Ts - Start, ?MIN_ELAPSED_MS),
    Correction = 1 / (1 - math:exp(-Elapsed / DecayMs)),
    lists:foldl(fun(Metric, Acc) ->
                        #{Metric := Li} = State,
                        Acc#{Metric => ra_li:rate(Ts, Li) * Correction}
                end, #{timestamp => erlang:system_time(millisecond),
                       decay_ms => DecayMs}, ?METRICS).

-spec format(snapshot()) -> [{metric(), float()}].
format(Snapshot) ->
    format(Snapshot, erlang:system_time(millisecond)).

%% Decays a snapshot that has not been refreshed, so that it reaches zero.
-spec format(snapshot(), integer()) -> [{metric(), float()}].
format(#{timestamp := Timestamp, decay_ms := DecayMs} = Snapshot, Now) ->
    Age = max(0, Now - Timestamp - ?FRESH_MS),
    Factor = math:exp(-Age / DecayMs),
    [{Metric, round_rate(maps:get(Metric, Snapshot, 0.0) * Factor)}
     || Metric <- ?METRICS].

index(publish) -> 1;
index(deliver) -> 2;
index(redeliver) -> 3;
index(ack) -> 4;
index(reject) -> 5.

round_rate(Rate) ->
    round(Rate * 100) / 100.

ts() ->
    erlang:monotonic_time(millisecond).
