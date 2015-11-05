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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2012 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_stats).

-include("rabbit_mgmt.hrl").

-export([blank/0, is_blank/1, record/3, format/3, sum/1, gc/2]).

-import(rabbit_misc, [pget/2]).

%%----------------------------------------------------------------------------

blank() ->
    Table = ets:new(rabbit_mgmt_stats, [ordered_set, public]),
    true = ets:insert(Table, {base, 0}),
    %% Total is only updated on event-time, so it can be part of the event
    %% server.
    #stats{diffs = Table, total = 0}.

is_blank(#stats{diffs = Diffs}) -> 0 == get_base(Diffs)
                                       andalso ets:info(Diffs, size) == 1.

%%----------------------------------------------------------------------------
%% Event-time
%%----------------------------------------------------------------------------

record(TS, Diff, Stats = #stats{diffs = Diffs, total = TreeTotal}) ->
    ets_update(Diffs, TS, Diff),
    Stats#stats{total = Diff + TreeTotal}.

%%----------------------------------------------------------------------------
%% Query-time
%%----------------------------------------------------------------------------

format(no_range, #stats{diffs = Diffs, total = Count}, Interval) ->
    Now = time_compat:os_system_time(milli_seconds),
    RangePoint = ((Now div Interval) * Interval) - Interval,
    {[{rate, format_rate(
               Diffs, RangePoint, Interval, Interval)}], Count};

format(Range, #stats{diffs = Diffs}, Interval) ->
    Base = get_base(Diffs),
    RangePoint = Range#range.last - Interval,
    {Samples, Count} = extract_samples(Range, Base, Diffs, []),
    Part1 = [{rate,    format_rate(
                         Diffs, RangePoint, Range#range.incr, Interval)},
             {samples, Samples}],
    Length = length(Samples),
    Part2 = case Length > 1 of
                true  -> [{sample, S2}, {timestamp, T2}] = hd(Samples),
                         [{sample, S1}, {timestamp, T1}] = lists:last(Samples),
                         Total = lists:sum([pget(sample, I) || I <- Samples]),
                         [{avg_rate, (S2 - S1) * 1000 / (T2 - T1)},
                          {avg,      Total / Length}];
                false -> []
            end,
    {Part1 ++ Part2, Count}.

format_rate(Diffs, RangePoint, Incr, Interval) ->
    case second_largest(Diffs) of
        []   -> 0.0;
        [{TS, S}] -> case TS - RangePoint of %% [0]
                         D when D =< Incr andalso D >= 0 -> S * 1000 / Interval;
                         _                               -> 0.0
                     end
    end.

%% [0] Only display the rate if it's live - i.e. ((the end of the
%% range) - interval) corresponds to the second to last data point we
%% have. If the end of the range is earlier we have gone silent, if
%% it's later we have been asked for a range back in time (in which
%% case showing the correct instantaneous rate would be quite a faff,
%% and probably unwanted). Why the second to last? Because data is
%% still arriving for the last...
second_largest(Table) ->
    %% 'base' is always largest than any timestamp (integer)
    case ets:prev(Table, base) of
        '$end_of_table' ->
            [];
        Key ->
            case ets:prev(Table, Key) of
                '$end_of_table' -> [];
                Key2 -> ets:lookup(Table, Key2)
            end
    end.

%% What we want to do here is: given the #range{}, provide a set of
%% samples such that we definitely provide a set of samples which
%% covers the exact range requested, despite the fact that we might
%% not have it. We need to spin up over the entire range of the
%% samples we *do* have since they are diff-based (and we convert to
%% absolute values here).
extract_samples(Range, Base, Table, Samples) ->
    %% There is always at least one element, the base
    {List, Cont} = ets:select(Table, [{'_', [], ['$_']}], 5),
    extract_samples(Range, Base, List, Cont, Samples).


extract_samples(Range = #range{first = Next}, Base, [], '$end_of_table' = Cont,
                Samples) ->
    %% [3] Empty or finished table
    extract_samples1(Range, Base, [{Next, 0}], Cont, Samples);
extract_samples(Range = #range{first = Next}, Base, [], Cont, Samples) ->
    case ets:select(Cont) of
        '$end_of_table' = Cont0 ->
            extract_samples1(Range, Base, [{Next, 0}], Cont0, Samples);
        {List, Cont1} ->
            extract_samples1(Range, Base, List, Cont1, Samples)
    end;
extract_samples(Range, Base, [{base, _} | T], Cont, Samples) ->
    extract_samples(Range, Base, T, Cont, Samples);
extract_samples(Range, Base, List, Cont, Samples) ->
    extract_samples1(Range, Base, List, Cont, Samples).

extract_samples1(Range = #range{first = Next, last = Last, incr = Incr},
                 Base, [{TS, S} | T] = List, Cont, Samples) ->
    if
        %% We've gone over the range. Terminate.
        Next > Last ->
            {Samples, Base};
        %% We've hit bang on a sample. Record it and move to the next.
        Next =:= TS ->
            extract_samples(Range#range{first = Next + Incr}, Base + S,
                            T, Cont,
                            append(Base + S, Next, Samples));
        %% We haven't yet hit the beginning of our range.
        Next > TS ->
            extract_samples(Range, Base + S, T, Cont, Samples);
        %% We have a valid sample, but we haven't used it up
        %% yet. Append it and loop around.
        Next < TS ->
            extract_samples1(Range#range{first = Next + Incr}, Base,
                             List, Cont, append(Base, Next, Samples))
    end.

append(S, TS, Samples) -> [[{sample, S}, {timestamp, TS}] | Samples].

sum([]) -> blank();

sum([#stats{diffs = D, total = T} | StatsN]) ->
    #stats{diffs = Table} = Stats = blank(),
    ets:insert(Table, ets:tab2list(D)),
    sum(StatsN, Stats#stats{total = T}).

sum(StatsN, Stats) ->
    lists:foldl(
      fun (#stats{diffs = D1, total = T1},
           #stats{diffs = D2, total = T2} = Acc) ->
              List = ets:tab2list(D1),
              [ets_update(D2, K, V) || {K, V} <- List],
              Acc#stats{total = T1 + T2}
      end, Stats, StatsN).

%%----------------------------------------------------------------------------
%% Event-GCing
%%----------------------------------------------------------------------------

gc(Cutoff, #stats{diffs = Diffs}) ->
    %% There is always at least one element, the base
    {List, Cont} = ets:select_reverse(Diffs, [{'_', [], ['$_']}], 5),
    gc(Cutoff, List, Cont, Diffs, undefined).

%% Go through the list, amalgamating all too-old samples with the next
%% newest keepable one [0] (we move samples forward in time since the
%% semantics of a sample is "we had this many x by this time"). If the
%% sample is too old, but would not be too old if moved to a rounder
%% timestamp which does not exist then invent one and move it there
%% [1]. But if it's just outright too old, move it to the base [2].
gc(_Cutoff, [], '$end_of_table', _Table, _Keep) ->
    ok;
gc(Cutoff, [], Cont, Table, Keep) ->
    case ets:select(Cont) of
        '$end_of_table' -> ok;
        {List, Cont1} -> gc(Cutoff, List, Cont1, Table, Keep)
    end;
gc(Cutoff, [{base, _} | T], Cont, Table, Keep) ->
    gc(Cutoff, T, Cont, Table, Keep);
gc(Cutoff, [{TS, S} | T], Cont, Table, Keep) ->
    %% TODO GC should have an independent process that removes the 0's
    %% This function won't do it! -> traverse the table deleting {Key, 0}
    Keep1 = case keep(Cutoff, TS) of
                keep ->
                    TS;
                drop -> %% [2]
                    ets:update_counter(Table, base, S),
                    ets_delete_value(Table, TS, S),
                    Keep;
                {move, D} when Keep =:= undefined -> %% [1]
                    ets_update(Table, TS + D, S),
                    ets_delete_value(Table, TS, S),
                    TS + D;
                {move, _} -> %% [0]
                    ets_update(Table, Keep, S),
                    ets_delete_value(Table, TS, S),
                    Keep
            end,
    gc(Cutoff, T, Cont, Table, Keep1).

keep({Policy, Now}, TS) ->
    lists:foldl(fun ({AgeSec, DivisorSec}, Action) ->
                        prefer_action(
                          Action,
                          case (Now - TS) =< (AgeSec * 1000) of
                              true  -> DivisorMillis = DivisorSec * 1000,
                                       case TS rem DivisorMillis of
                                           0   -> keep;
                                           Rem -> {move, DivisorMillis - Rem}
                                       end;
                              false -> drop
                          end)
                end, drop, Policy).

prefer_action(keep,              _) -> keep;
prefer_action(_,              keep) -> keep;
prefer_action({move, A}, {move, B}) -> {move, lists:min([A, B])};
prefer_action({move, A},      drop) -> {move, A};
prefer_action(drop,      {move, A}) -> {move, A};
prefer_action(drop,           drop) -> drop.

ets_update(Table, K, V) ->
    try
        ets:update_counter(Table, K, V)
    catch
        _:_ ->
            ets:insert(Table, {K, V})
    end.

get_base(Table) ->
    [{base, Base}] = ets:lookup(Table, base),
    Base.

ets_delete_value(Table, TS, S) ->
    case ets:select_delete(Table, [{{'$1','$2'}, [{'==', TS, '$1'},
                                                  {'==', S, '$2'}],[true]}]) of
        1 -> ok;
        0 -> ets:update_counter(Table, TS, -S)
    end.
