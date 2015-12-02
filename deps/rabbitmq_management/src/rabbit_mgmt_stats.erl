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

-export([blank/1, is_blank/2, record/3, format/4, sum/1, gc/3,
         free/1, delete_stats/2, get_keys/2, get_keys_count/1,
         get_first_key/1]).

-import(rabbit_misc, [pget/2]).

%%----------------------------------------------------------------------------

blank(Name) ->
    ets:new(Name, [ordered_set, public, named_table]).

blank() ->
    ets:new(rabbit_mgmt_stats, [ordered_set, public]).

is_blank(Table, Id) ->
    ets:lookup(Table, {Id, total}) == [].

%%----------------------------------------------------------------------------
free(Table) ->
    ets:delete(Table).

delete_stats(Table, Id) ->
    ets:select_delete(Table, match_spec_delete_id(Id)).

%%----------------------------------------------------------------------------
get_keys(Table, Id0) ->
    ets:select(Table, match_spec_keys(Id0)).

get_keys_count(Table) ->
    ets:select_count(Table, match_spec_keys()).

get_first_key(Table) ->
    ets:select(Table, match_spec_keys(), 1).

%%----------------------------------------------------------------------------
%% Event-time
%%----------------------------------------------------------------------------

record({Id, _TS} = Key, Diff, Table) ->
    ets_update(Table, Key, Diff),
    ets_update(Table, {Id, total}, Diff).

%%----------------------------------------------------------------------------
%% Query-time
%%----------------------------------------------------------------------------

format(no_range, Table, Id, Interval) ->
    Count = get_value(Table, Id, total),
    Now = time_compat:os_system_time(milli_seconds),
    RangePoint = ((Now div Interval) * Interval) - Interval,
    {[{rate, format_rate(
               Table, Id, RangePoint, Interval, Interval)}], Count};

format(Range, Table, Id, Interval) ->
    Base = get_value(Table, Id, base),
    RangePoint = Range#range.last - Interval,
    {Samples, Count} = extract_samples(Range, Base, Table, Id, []),
    Part1 = [{rate,    format_rate(
                         Table, Id, RangePoint, Range#range.incr, Interval)},
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

format_rate(Table, Id, RangePoint, Incr, Interval) ->
    case second_largest(Table, Id) of
        unknown   ->
            0.0;
        {{_, TS}, S} ->
            case TS - RangePoint of %% [0]
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
second_largest(Table, Id) ->
    case ets:select(Table, match_spec_second_largest(Id), 2) of
        {Match, _} when length(Match) == 2->
            lists:last(Match);
        _ ->
            unknown
    end.

%% What we want to do here is: given the #range{}, provide a set of
%% samples such that we definitely provide a set of samples which
%% covers the exact range requested, despite the fact that we might
%% not have it. We need to spin up over the entire range of the
%% samples we *do* have since they are diff-based (and we convert to
%% absolute values here).
extract_samples(Range, Base, Table, Id, Samples) ->
    case ets:select(Table, match_spec_id(Id), 5) of
        {List, Cont} ->
            extract_samples0(Range, Base, List, Cont, Samples);
        '$end_of_table' ->
            extract_samples0(Range, Base, [], '$end_of_table', Samples)
    end.

extract_samples0(Range = #range{first = Next}, Base, [], '$end_of_table' = Cont,
                Samples) ->
    %% [3] Empty or finished table
    extract_samples1(Range, Base, [{{unused_id, Next}, 0}], Cont, Samples);
extract_samples0(Range = #range{first = Next}, Base, [], Cont, Samples) ->
    case ets:select(Cont) of
        '$end_of_table' = Cont0 ->
            extract_samples1(Range, Base, [{{unused_id, Next}, 0}], Cont0, Samples);
        {List, Cont1} ->
            extract_samples1(Range, Base, List, Cont1, Samples)
    end;
extract_samples0(Range, Base, [{{_, base}, _} | T], Cont, Samples) ->
    extract_samples0(Range, Base, T, Cont, Samples);
extract_samples0(Range, Base, [{{_, total}, _} | T], Cont, Samples) ->
    extract_samples0(Range, Base, T, Cont, Samples);
extract_samples0(Range, Base, List, Cont, Samples) ->
    extract_samples1(Range, Base, List, Cont, Samples).

extract_samples1(Range = #range{first = Next, last = Last, incr = Incr},
                 Base, [{{_, TS}, S} | T] = List, Cont, Samples) ->
    if
        %% We've gone over the range. Terminate.
        Next > Last ->
            {Samples, Base};
        %% We've hit bang on a sample. Record it and move to the next.
        Next =:= TS ->
            extract_samples0(Range#range{first = Next + Incr}, Base + S,
                            T, Cont,
                            append(Base + S, Next, Samples));
        %% We haven't yet hit the beginning of our range.
        Next > TS ->
            extract_samples0(Range, Base + S, T, Cont, Samples);
        %% We have a valid sample, but we haven't used it up
        %% yet. Append it and loop around.
        Next < TS ->
            extract_samples1(Range#range{first = Next + Incr}, Base,
                             List, Cont, append(Base, Next, Samples))
    end.

append(S, TS, Samples) -> [[{sample, S}, {timestamp, TS}] | Samples].

sum([]) -> blank();

sum([{T1, {_, Id} = TId} | StatsN]) ->
    Table = blank(),
    All = select_by_id(T1, TId),
    lists:foreach(fun({{_, TS}, V}) ->
                          ets:insert(Table, {{{all, Id}, TS}, V})
                  end, All),
    sum(StatsN, Table).

sum(StatsN, Table) ->
    lists:foreach(
      fun ({T1, {_, Id} = TId}) ->
              All = select_by_id(T1, TId),
              lists:foreach(fun({{_, TS}, V}) ->
                                    ets_update(Table, {{all, Id}, TS}, V)
                            end, All)
      end, StatsN),
    Table.

%%----------------------------------------------------------------------------
%% Event-GCing
%%----------------------------------------------------------------------------

gc(Cutoff, Table, Id) ->
    case ets:select_reverse(Table, match_spec_id(Id), 5) of
        '$end_of_table' ->
            ok;
        {List, Cont} ->
            gc(Cutoff, List, Cont, Table, undefined)
    end.

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
gc(Cutoff, [{{_, base}, _} | T], Cont, Table, Keep) ->
    gc(Cutoff, T, Cont, Table, Keep);
gc(Cutoff, [{{_, total}, _} | T], Cont, Table, Keep) ->
    gc(Cutoff, T, Cont, Table, Keep);
gc(Cutoff, [{{Id, TS} = Key, S} | T], Cont, Table, Keep) ->
    %% TODO GC should have an independent process that removes the 0's
    %% This function won't do it! -> traverse the table deleting {Key, 0}
    Keep1 = case keep(Cutoff, TS) of
                keep ->
                    TS;
                drop -> %% [2]
                    ets_update(Table, {Id, base}, S),
                    ets_delete_value(Table, Key, S),
                    Keep;
                {move, D} when Keep =:= undefined -> %% [1]
                    ets_update(Table, {Id, TS + D}, S),
                    ets_delete_value(Table, Key, S),
                    TS + D;
                {move, _} -> %% [0]
                    ets_update(Table, {Id, Keep}, S),
                    ets_delete_value(Table, Key, S),
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

get_value(Table, Id, Tag) ->
    try
        ets:lookup_element(Table, {Id, Tag}, 2)
    catch
        error:badarg ->
            0
    end.

ets_delete_value(Table, Key, S) ->
    case ets:select_delete(Table, match_spec_delete_value(Key, S)) of
        1 -> ok;
        0 -> ets:update_counter(Table, Key, -S)
    end.

select_by_id(Table, Id) ->
    ets:select(Table, match_spec_id(Id)).

match_spec_id({{Id0, Id1}, Id2}) when is_tuple(Id0), is_tuple(Id1) ->
    [{{{'$1', '_'}, '_'}, [{'==', {{{{{Id0}, {Id1}}}, Id2}}, '$1'}], ['$_']}];
match_spec_id({{Id0, Id1}, Id2}) when is_tuple(Id0) ->
    [{{{'$1', '_'}, '_'}, [{'==', {{{{{Id0}, Id1}}, Id2}}, '$1'}], ['$_']}];
match_spec_id({{Id0, Id1}, Id2}) when is_tuple(Id1) ->
    [{{{'$1', '_'}, '_'}, [{'==', {{{{Id0, {Id1}}}, Id2}}, '$1'}], ['$_']}];
match_spec_id({Id0, Id1}) when is_tuple(Id0) ->
    [{{{'$1', '_'}, '_'}, [{'==', {{{Id0}, Id1}}, '$1'}], ['$_']}];
match_spec_id(Id) ->
    [{{{'$1', '_'}, '_'}, [{'==', {Id}, '$1'}], ['$_']}].

match_spec_second_largest({{Id0, Id1}, Id2}) when is_tuple(Id0), is_tuple(Id1) ->
    [{{{'$1', '$2'}, '_'}, [{'==', {{{{{Id0}, {Id1}}}, Id2}}, '$1'},
                            {'is_integer', '$2'}], ['$_']}];
match_spec_second_largest({{Id0, Id1}, Id2}) when is_tuple(Id0) ->
    [{{{'$1', '$2'}, '_'}, [{'==', {{{{{Id0}, Id1}}, Id2}}, '$1'},
                            {'is_integer', '$2'}], ['$_']}];
match_spec_second_largest({{Id0, Id1}, Id2}) when is_tuple(Id1) ->
    [{{{'$1', '$2'}, '_'}, [{'==', {{{{Id0, {Id1}}}, Id2}}, '$1'},
                            {'is_integer', '$2'}], ['$_']}];
match_spec_second_largest({Id0, Id1}) when is_tuple(Id0) ->
    [{{{'$1', '$2'}, '_'}, [{'==', {{{Id0}, Id1}}, '$1'},
                            {'is_integer', '$2'}], ['$_']}];
match_spec_second_largest(Id) ->
    [{{{'$1', '$2'}, '_'}, [{'==', {Id}, '$1'},
                            {'is_integer', '$2'}], ['$_']}].

match_spec_delete_id({Id0, '_'}) when is_tuple(Id0) ->
    [{{{{{'$1', '_'}, '_'}, '_'}, '_'}, [{'==', {Id0}, '$1'}],[true]}];
match_spec_delete_id({'_', Id1}) when is_tuple(Id1) ->
    [{{{{{'_', '$1'}, '_'}, '_'}, '_'}, [{'==', {Id1}, '$1'}],[true]}];
match_spec_delete_id({Id0, '_'}) ->
    [{{{{{'$1', '_'}, '_'}, '_'}, '_'}, [{'==', Id0, '$1'}],[true]}];
match_spec_delete_id({'_', Id1}) ->
    [{{{{{'_', '$1'}, '_'}, '_'}, '_'}, [{'==', Id1, '$1'}],[true]}];
match_spec_delete_id(Id) when is_tuple(Id) ->
    [{{{{'$1', '_'}, '_'}, '_'}, [{'==', {Id}, '$1'}],[true]}];
match_spec_delete_id(Id) ->
    [{{{{'$1', '_'}, '_'}, '_'}, [{'==', Id, '$1'}],[true]}].

match_spec_delete_value({Id, TS}, S) when is_tuple(Id) ->
    [{'$1', [{'==', {{{{{Id}, TS}}, S}}, '$1'}], [true]}];
match_spec_delete_value({Id, TS}, S) ->
    [{'$1', [{'==', {{{{Id, TS}}, S}}, '$1'}], [true]}].


match_spec_keys({'_', Id1}) when is_tuple(Id1) ->
    [{{{{{'$1', '$2'}, '$3'}, '$4'}, '_'}, [{'==', {Id1}, '$2'},
                                            {'==', total, '$4'}], ['$$']}];
match_spec_keys({'_', Id1}) ->
    [{{{{{'$1', '$2'}, '$3'}, '$4'}, '_'}, [{'==', Id1, '$2'},
                                            {'==', total, '$4'}], ['$$']}];
match_spec_keys({Id0, '_'}) when is_tuple(Id0) ->
    [{{{{{'$1', '$2'}, '$3'}, '$4'}, '_'}, [{'==', {Id0}, '$1'},
                                            {'==', total, '$4'}], ['$$']}];
match_spec_keys({Id0, '_'}) ->
    [{{{{{'$1', '$2'}, '$3'}, '$4'}, '_'}, [{'==', Id0, '$1'},
                                            {'==', total, '$4'}], ['$$']}];
match_spec_keys(Id) when is_tuple(Id) ->
    [{{{{'$1', '$2'}, '$3'}, '_'}, [{'==', {Id}, '$1'},
                                    {'==', total, '$3'}], ['$$']}];
match_spec_keys(Id) ->
    [{{{{'$1', '$2'}, '$3'}, '_'}, [{'==', Id, '$1'},
                                    {'==', total, '$3'}], ['$$']}].

match_spec_keys() ->
    [{{{'$1', '$2'}, '_'}, [{'==', total, '$2'}], ['$1']}].
