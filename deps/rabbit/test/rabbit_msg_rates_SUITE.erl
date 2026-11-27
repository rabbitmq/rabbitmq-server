%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_msg_rates_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].

groups() ->
    [{tests, [shuffle],
      [
       idle,
       steady_rate,
       reads_steady_while_warming_up,
       does_not_oscillate,
       decays_when_idle,
       follows_a_change,
       counters_and_deltas,
       format_ages_stale_snapshot,
       metrics_are_independent
      ]}].

idle(_) ->
    S = rabbit_msg_rates:new(0),
    Snapshot = rabbit_msg_rates:snapshot(S, 5000),
    [?assert(maps:get(Metric, Snapshot) == 0)
     || Metric <- [publish, deliver, redeliver, ack, reject]].

steady_rate(_) ->
    %% 200 messages per second observed every 5 seconds
    S = observe_every_5s(deliver, 1000, 0, 120_000, rabbit_msg_rates:new(0)),
    ?assert(abs(rate(deliver, 120_000, S) - 200) < 2).

reads_steady_while_warming_up(_) ->
    %% 1 message per second from the start
    S0 = rabbit_msg_rates:new(0),
    S1 = rabbit_msg_rates:observe(publish, 5, 5000, 5000, S0),
    ?assert(abs(rate(publish, 5000, S1) - 1) < 0.05),
    S2 = observe_every_5s(publish, 5, 0, 30_000, S0),
    ?assert(abs(rate(publish, 30_000, S2) - 1) < 0.05).

does_not_oscillate(_) ->
    S0 = observe_every_5s(ack, 500, 0, 120_000, rabbit_msg_rates:new(0)),
    S1 = rabbit_msg_rates:observe(ack, 500, 5000, 125_000, S0),
    ?assert(abs(rate(ack, 120_000, S0) - rate(ack, 125_000, S1)) < 0.5).

decays_when_idle(_) ->
    S0 = observe_every_5s(publish, 500, 0, 60_000, rabbit_msg_rates:new(0)),
    S1 = observe_every_5s(publish, 0, 65_000, 260_000, S0),
    ?assert(rate(publish, 60_000, S0) > 90),
    ?assert(rate(publish, 260_000, S1) < 1).

follows_a_change(_) ->
    %% from 100 to 10 messages per second
    S0 = observe_every_5s(deliver, 500, 0, 120_000, rabbit_msg_rates:new(0)),
    S1 = observe_every_5s(deliver, 50, 125_000, 300_000, S0),
    ?assert(abs(rate(deliver, 300_000, S1) - 10) < 1).

format_ages_stale_snapshot(_) ->
    Snapshot = #{timestamp => 1000, decay_ms => 30_000,
                 publish => 100.0, deliver => 50.0, ack => 0.0},
    Steady = [{publish, 100.0}, {deliver, 50.0}, {redeliver, 0.0},
              {ack, 0.0}, {reject, 0.0}],
    ?assertEqual(Steady, rabbit_msg_rates:format(Snapshot, 1000)),
    %% not aged while it can still be refreshed
    ?assertEqual(Steady, rabbit_msg_rates:format(Snapshot, 16_000)),
    ?assertEqual([{publish, 36.79}, {deliver, 18.39}, {redeliver, 0.0},
                  {ack, 0.0}, {reject, 0.0}],
                 rabbit_msg_rates:format(Snapshot, 46_000)),
    %% a snapshot from the future is not amplified
    ?assertEqual(Steady, rabbit_msg_rates:format(Snapshot, 0)).

counters_and_deltas(_) ->
    Counters = rabbit_msg_rates:new_counters(),
    ok = rabbit_msg_rates:count(reject, 5, Counters),
    ok = rabbit_msg_rates:count(reject, 5, Counters),
    ok = rabbit_msg_rates:count(redeliver, 1, Counters),
    ?assertMatch(#{reject := 10, redeliver := 1, publish := 0, deliver := 0, ack := 0},
                 rabbit_msg_rates:totals(Counters)),
    S = rabbit_msg_rates:observe_deltas(#{reject => 50}, 5000, 5000,
                                        rabbit_msg_rates:new(0)),
    ?assert(abs(rate(reject, 5000, S) - 10) < 0.5),
    ?assertEqual(0.0, rate(ack, 5000, S)).

metrics_are_independent(_) ->
    S = rabbit_msg_rates:observe(publish, 10, 5000, 5000,
                                 rabbit_msg_rates:new(0)),
    ?assert(rate(publish, 5000, S) > 0),
    ?assertEqual(0.0, rate(deliver, 5000, S)),
    ?assertEqual(0.0, rate(ack, 5000, S)).

observe_every_5s(Metric, Count, From, To, State) ->
    lists:foldl(fun(Ts, Acc) ->
                        rabbit_msg_rates:observe(Metric, Count, 5000, Ts, Acc)
                end, State, lists:seq(From + 5000, To, 5000)).

rate(Metric, Ts, State) ->
    maps:get(Metric, rabbit_msg_rates:snapshot(State, Ts)).
