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
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_stats).

-include("rabbit_mgmt.hrl").

-export([blank/0, is_blank/1, record/3, format/3, sum/1, gc/2]).

-import(rabbit_misc, [pget/2]).

%%----------------------------------------------------------------------------

blank() -> #stats{diffs = gb_trees:empty(), base = 0}.

is_blank(#stats{diffs = Diffs, base = 0}) -> gb_trees:is_empty(Diffs);
is_blank(#stats{}) ->                        false.

%%----------------------------------------------------------------------------
%% Event-time
%%----------------------------------------------------------------------------

record(TS, Diff, Stats = #stats{diffs = Diffs}) ->
    Diffs2 = case gb_trees:lookup(TS, Diffs) of
                 {value, Total} -> gb_trees:update(TS, Diff + Total, Diffs);
                 none           -> gb_trees:insert(TS, Diff, Diffs)
             end,
    Stats#stats{diffs = Diffs2}.

%%----------------------------------------------------------------------------
%% Query-time
%%----------------------------------------------------------------------------

format(no_range, #stats{diffs = Diffs, base = Base}, Interval) ->
    Now = rabbit_mgmt_format:timestamp_ms(erlang:now()),
    RangePoint = ((Now div Interval) * Interval) - Interval,
    Count = sum_entire_tree(gb_trees:iterator(Diffs), Base),
    {[{rate, format_rate(
               Diffs, RangePoint, Interval, Interval)}], Count};

format(Range, #stats{diffs = Diffs, base = Base}, Interval) ->
    RangePoint = Range#range.last - Interval,
    {Samples, Count} = extract_samples(
                         Range, Base, gb_trees:iterator(Diffs), []),
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
    case nth_largest(Diffs, 2) of
        false   -> 0.0;
        {TS, S} -> case TS - RangePoint of %% [0]
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
nth_largest(Tree, N) ->
    case gb_trees:is_empty(Tree) of
        true              -> false;
        false when N == 1 -> gb_trees:largest(Tree);
        false             -> {_, _, Tree2} = gb_trees:take_largest(Tree),
                             nth_largest(Tree2, N - 1)
    end.

sum_entire_tree(Iter, Acc) ->
    case gb_trees:next(Iter) of
        none            -> Acc;
        {_TS, S, Iter2} -> sum_entire_tree(Iter2, Acc + S)
    end.

%% What we want to do here is: given the #range{}, provide a set of
%% samples such that we definitely provide a set of samples which
%% covers the exact range requested, despite the fact that we might
%% not have it. We need to spin up over the entire range of the
%% samples we *do* have since they are diff-based (and we convert to
%% absolute values here).
extract_samples(Range = #range{first = Next}, Base, It, Samples) ->
    case gb_trees:next(It) of
        {TS, S, It2} -> extract_samples1(Range, Base, TS,   S, It2, Samples);
        none         -> extract_samples1(Range, Base, Next, 0, It,  Samples)
    end.

extract_samples1(Range = #range{first = Next, last = Last, incr = Incr},
                 Base, TS, S, It, Samples) ->
    if
        %% We've gone over the range. Terminate.
        Next > Last ->
            {Samples, Base};
        %% We've hit bang on a sample. Record it and move to the next.
        Next =:= TS ->
            extract_samples(Range#range{first = Next + Incr}, Base + S, It,
                            append(Base + S, Next, Samples));
        %% We haven't yet hit the beginning of our range.
        Next > TS ->
            extract_samples(Range, Base + S, It, Samples);
        %% We have a valid sample, but we haven't used it up
        %% yet. Append it and loop around.
        Next < TS ->
            extract_samples1(Range#range{first = Next + Incr}, Base, TS, S, It,
                             append(Base, Next, Samples))
    end.

append(S, TS, Samples) -> [[{sample, S}, {timestamp, TS}] | Samples].

sum([]) -> blank();

sum([Stats | StatsN]) ->
    lists:foldl(
      fun (#stats{diffs = D1, base = B1}, #stats{diffs = D2, base = B2}) ->
              #stats{diffs = add_trees(D1, gb_trees:iterator(D2)),
                     base  = B1 + B2}
      end, Stats, StatsN).

add_trees(Tree, It) ->
    case gb_trees:next(It) of
        none        -> Tree;
        {K, V, It2} -> add_trees(
                         case gb_trees:lookup(K, Tree) of
                             {value, V2} -> gb_trees:update(K, V + V2, Tree);
                             none        -> gb_trees:insert(K, V, Tree)
                         end, It2)
    end.

%%----------------------------------------------------------------------------
%% Event-GCing
%%----------------------------------------------------------------------------

gc(Cutoff, #stats{diffs = Diffs, base = Base}) ->
    List = lists:reverse(gb_trees:to_list(Diffs)),
    gc(Cutoff, List, [], Base).

%% Go through the list, amalgamating all too-old samples with the next
%% newest keepable one [0] (we move samples forward in time since the
%% semantics of a sample is "we had this many x by this time"). If the
%% sample is too old, but would not be too old if moved to a rounder
%% timestamp which does not exist then invent one and move it there
%% [1]. But if it's just outright too old, move it to the base [2].
gc(_Cutoff, [], Keep, Base) ->
    #stats{diffs = gb_trees:from_orddict(Keep), base = Base};
gc(Cutoff, [H = {TS, S} | T], Keep, Base) ->
    {NewKeep, NewBase} =
        case keep(Cutoff, TS) of
            keep                       -> {[H | Keep],           Base};
            drop                       -> {Keep,             S + Base}; %% [2]
            {move, D} when Keep =:= [] -> {[{TS + D, S}],        Base}; %% [1]
            {move, _}                  -> [{KTS, KS} | KT] = Keep,
                                          {[{KTS, KS + S} | KT], Base}  %% [0]
        end,
    gc(Cutoff, T, NewKeep, NewBase).

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
