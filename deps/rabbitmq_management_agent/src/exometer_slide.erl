%% This file is a copy of exometer_slide.erl from https://github.com/Feuerlabs/exometer_core,
%% with the following modifications:
%%
%% 1) The elements are tuples of numbers
%%
%% 2) Only one element for each expected interval point is added, intermediate values
%%    are discarded. Thus, if we have a window of 60s and interval of 5s, at max 12 elements
%%    are stored.
%%
%% 3) Additions can be provided as increments to the last value stored
%%
%% 4) sum/1 implements the sum of several slides, generating a new timestamp sequence based
%%    on the given intervals. Elements on each window are added to the closest interval point.
%%
%% Original commit: https://github.com/Feuerlabs/exometer_core/commit/2759edc804211b5245867b32c9a20c8fe1d93441
%%
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%%   This Source Code Form is subject to the terms of the Mozilla Public
%%   License, v. 2.0. If a copy of the MPL was not distributed with this
%%   file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% -------------------------------------------------------------------
%%
%% @author Tony Rogvall <tony@rogvall.se>
%% @author Ulf Wiger <ulf@feuerlabs.com>
%% @author Magnus Feuer <magnus@feuerlabs.com>
%%
%% @doc Efficient sliding-window buffer
%%
%% Initial implementation: 29 Sep 2009 by Tony Rogvall
%%
%% This module implements an efficient sliding window, maintaining
%% two lists - a primary and a secondary. Values are paired with a
%% timestamp (millisecond resolution, see `timestamp/0')
%% and prepended to the primary list. When the time span between the oldest
%% and the newest entry in the primary list exceeds the given window size,
%% the primary list is shifted into the secondary list position, and the
%% new entry is added to a new (empty) primary list.
%%
%% The window can be converted to a list using `to_list/1'.
%% @end
%%
%%
%% All modifications are (C) 2007-2022 VMware, Inc. or its affiliates. All rights reserved.
%% The Initial Developer of the Original Code is Basho Technologies, Inc.

-module(exometer_slide).

-export([new/2, new/3,
         reset/1,
         add_element/3,
         to_list/2,
         to_list/3,
         foldl/5,
         map/2,
         to_normalized_list/5]).

-export([timestamp/0,
         last_two/1,
         last/1]).

-export([sum/1,
         sum/2,
         sum/5,
         optimize/1]).

%% For testing
-export([buffer/1]).

-compile(inline).
-compile(inline_list_funcs).


-type value() :: tuple().
-type internal_value() :: tuple() | drop.
-type timestamp() :: non_neg_integer().

-type fold_acc() :: any().
-type fold_fun() :: fun(({timestamp(), internal_value()}, fold_acc()) -> fold_acc()).

%% Fixed size event buffer
-record(slide, {size = 0 :: integer(),  % ms window
                n = 0 :: integer(),  % number of elements in buf1
                max_n :: infinity | integer(),  % max no of elements
                incremental = false :: boolean(),
                interval :: integer(),
                last = 0 :: integer(), % millisecond timestamp
                first = undefined :: undefined | integer(), % millisecond timestamp
                buf1 = [] :: [internal_value()],
                buf2 = [] :: [internal_value()],
                total :: undefined | value()}).

-opaque slide() :: #slide{}.

-export_type([slide/0, timestamp/0]).

-spec timestamp() -> timestamp().
%% @doc Generate a millisecond-resolution timestamp.
%%
%% This timestamp format is used e.g. by the `exometer_slide' and
%% `exometer_histogram' implementations.
%% @end
timestamp() ->
    os:system_time(milli_seconds).

-spec new(_Size::integer(), _Options::list()) -> slide().
%% @doc Create a new sliding-window buffer.
%%
%% `Size' determines the size in milliseconds of the sliding window.
%% The implementation prepends values into a primary list until the oldest
%% element in the list is `Size' ms older than the current value. It then
%% swaps the primary list into a secondary list, and starts prepending to
%% a new primary list. This means that more data than fits inside the window
%% will be kept - upwards of twice as much. On the other hand, updating the
%% buffer is very cheap.
%% @end
new(Size, Opts) -> new(timestamp(), Size, Opts).

-spec new(Timestamp :: timestamp(), Size::integer(), Options::list()) -> slide().
new(TS, Size, Opts) ->
    #slide{size = Size,
           max_n = proplists:get_value(max_n, Opts, infinity),
           interval = proplists:get_value(interval, Opts, infinity),
           last = TS,
           first = undefined,
           incremental = proplists:get_value(incremental, Opts, false),
           buf1 = [],
           buf2 = []}.

-spec reset(slide()) -> slide().

%% @doc Empty the buffer
%%
reset(Slide) ->
    Slide#slide{n = 0, buf1 = [], buf2 = [], last = 0, first = undefined}.

%% @doc Add an element to the buffer, tagged with the given timestamp.
%%
%% Apart from the specified timestamp, this function works just like
%% {@link add_element/2}.
%% @end
-spec add_element(timestamp(), value(), slide()) -> slide().
add_element(_TS, _Evt, Slide) when Slide#slide.size == 0 ->
    Slide;
add_element(TS, Evt, #slide{last = Last, interval = Interval, total = Total0,
                            incremental = true} = Slide)
  when (TS - Last) < Interval ->
    Total = add_to_total(Evt, Total0),
    Slide#slide{total = Total};
add_element(TS, Evt, #slide{last = Last, interval = Interval} = Slide)
  when (TS - Last) < Interval ->
    Slide#slide{total = Evt};
add_element(TS, Evt, #slide{last = Last, size = Sz, incremental = true,
                            n = N, max_n = MaxN, total = Total0,
                            buf1 = Buf1} = Slide) ->
    N1 = N+1,
    Total = add_to_total(Evt, Total0),
    %% Total could be the same as the last sample, by adding and substracting
    %% the same amout to the totals. That is not strictly a drop, but should
    %% generate new samples.
    %% I.e. 0, 0, -14, 14 (total = 0, samples = 14, -14, 0, drop)
    case {is_zeros(Evt), Buf1} of
        {_, []} ->
            Slide#slide{n = N1, first = TS, buf1 = [{TS, Total} | Buf1],
                        last = TS, total = Total};
        _ when TS - Last > Sz; N1 > MaxN ->
            %% swap
            Slide#slide{last = TS, n = 1, buf1 = [{TS, Total}],
                        buf2 = Buf1, total = Total};
        {true, [{_, Total}, {_, drop} = Drop | Tail]} ->
            %% Memory optimisation
            Slide#slide{buf1 = [{TS, Total}, Drop | Tail],
                        n = N1, last = TS};
        {true, [{DropTS, Total} | Tail]} ->
            %% Memory optimisation
            Slide#slide{buf1 = [{TS, Total}, {DropTS, drop} | Tail],
                        n = N1, last = TS};
        _ ->
            Slide#slide{n = N1, buf1 = [{TS, Total} | Buf1],
                        last = TS, total = Total}
    end;
add_element(TS, Evt, #slide{last = Last, size = Sz, n = N, max_n = MaxN,
                            buf1 = Buf1} = Slide)
    when TS - Last > Sz; N + 1 > MaxN ->
            Slide#slide{last = TS, n = 1, buf1 = [{TS, Evt}],
                        buf2 = Buf1, total = Evt};
add_element(TS, Evt, #slide{buf1 = [{_, Evt}, {_, drop} = Drop | Tail],
                            n = N} = Slide) ->
    %% Memory optimisation
    Slide#slide{buf1 = [{TS, Evt}, Drop | Tail], n = N + 1, last = TS};
add_element(TS, Evt, #slide{buf1 = [{DropTS, Evt} | Tail], n = N} = Slide) ->
    %% Memory optimisation
    Slide#slide{buf1 = [{TS, Evt}, {DropTS, drop} | Tail],
                n = N + 1, last = TS};
add_element(TS, Evt, #slide{n = N, buf1 = Buf1} = Slide) ->
    N1 = N+1,
    case Buf1 of
        [] ->
            Slide#slide{n = N1, buf1 = [{TS, Evt} | Buf1],
                        last = TS, first = TS, total = Evt};
       _ ->
            Slide#slide{n = N1, buf1 = [{TS, Evt} | Buf1],
                        last = TS, total = Evt}
    end.

add_to_total(Evt, undefined) ->
    Evt;
add_to_total({A0}, {B0}) ->
    {B0 + A0};
add_to_total({A0, A1}, {B0, B1}) ->
    {B0 + A0, B1 + A1};
add_to_total({A0, A1, A2}, {B0, B1, B2}) ->
    {B0 + A0, B1 + A1, B2 + A2};
add_to_total({A0, A1, A2, A3}, {B0, B1, B2, B3}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3};
add_to_total({A0, A1, A2, A3, A4}, {B0, B1, B2, B3, B4}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4};
add_to_total({A0, A1, A2, A3, A4, A5}, {B0, B1, B2, B3, B4, B5}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5};
add_to_total({A0, A1, A2, A3, A4, A5, A6}, {B0, B1, B2, B3, B4, B5, B6}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6};
add_to_total({A0, A1, A2, A3, A4, A5, A6, A7}, {B0, B1, B2, B3, B4, B5, B6, B7}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6, B7 + A7};
add_to_total({A0, A1, A2, A3, A4, A5, A6, A7, A8}, {B0, B1, B2, B3, B4, B5, B6, B7, B8}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6, B7 + A7, B8 + A8};
add_to_total({A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14,
          A15, A16, A17, A18, A19},
         {B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14,
          B15, B16, B17, B18, B19}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6, B7 + A7, B8 + A8,
     B9 + A9, B10 + A10, B11 + A11, B12 + A12, B13 + A13, B14 + A14, B15 + A15, B16 + A16,
     B17 + A17, B18 + A18, B19 + A19}.

is_zeros({0}) ->
    true;
is_zeros({0, 0}) ->
    true;
is_zeros({0, 0, 0}) ->
    true;
is_zeros({0, 0, 0, 0}) ->
    true;
is_zeros({0, 0, 0, 0, 0}) ->
    true;
is_zeros({0, 0, 0, 0, 0, 0}) ->
    true;
is_zeros({0, 0, 0, 0, 0, 0, 0}) ->
    true;
is_zeros({0, 0, 0, 0, 0, 0, 0, 0, 0}) ->
    true;
is_zeros({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) ->
    true;
is_zeros(_) ->
    false.

-spec optimize(slide()) -> slide().
optimize(#slide{buf2 = []} = Slide) ->
    Slide;
optimize(#slide{buf1 = Buf1, buf2 = Buf2, max_n = MaxN, n = N} = Slide)
  when is_integer(MaxN) andalso length(Buf1) < MaxN ->
    Slide#slide{buf1 = Buf1,
                buf2 = lists:sublist(Buf2, n_diff(MaxN, N) + 1)};
optimize(Slide) -> Slide.

snd(T) when is_tuple(T) ->
    element(2, T).


-spec to_list(timestamp(), slide()) -> [{timestamp(), value()}].
%% @doc Convert the sliding window into a list of timestamped values.
%% @end
to_list(_Now, #slide{size = Sz}) when Sz == 0 ->
    [];
to_list(Now, #slide{size = Sz} = Slide) ->
    snd(to_list_from(Now, Now - Sz, Slide)).

to_list(Now, Start, Slide) ->
    snd(to_list_from(Now, Start, Slide)).

to_list_from(Now, Start0, #slide{max_n = MaxN, buf2 = Buf2, first = FirstTS,
                                 interval = Interval} = Slide) ->

    {NewN, Buf1} = maybe_add_last_sample(Now, Slide),
    Start = first_max(FirstTS, Start0),
    {Prev0, Buf1_1} = take_since(Buf1, Now, Start, first_max(MaxN, NewN), [], Interval),
    case take_since(Buf2, Now, Start, first_max(MaxN, NewN), Buf1_1, Interval) of
        {undefined, Buf1_1} ->
            {Prev0, Buf1_1};
        {_Prev, Buf1_1} = Res ->
            case Prev0 of
                undefined ->
                    Res;
                _ ->
                    %% If take_since returns the same buffer, that means we don't
                    %% need Buf2 at all. We might be returning a too old sample
                    %% in previous, so we must use the one from Buf1
                    {Prev0, Buf1_1}
            end;
        Res ->
            Res
    end.

first_max(F, X) when is_integer(F) -> max(F, X);
first_max(_, X) -> X.

-spec last_two(slide()) -> [{timestamp(), value()}].
%% @doc Returns the newest 2 elements on the sample
last_two(#slide{buf1 = [{TS, Evt} = H1, {_, drop} | _], interval = Interval}) ->
    [H1, {TS - Interval, Evt}];
last_two(#slide{buf1 = [H1, H2_0 | _], interval = Interval}) ->
    H2 = adjust_timestamp(H1, H2_0, Interval),
    [H1, H2];
last_two(#slide{buf1 = [H1], buf2 = [H2_0 | _],
                interval = Interval}) ->
    H2 = adjust_timestamp(H1, H2_0, Interval),
    [H1, H2];
last_two(#slide{buf1 = [H1], buf2 = []}) ->
    [H1];
last_two(_) ->
    [].

adjust_timestamp({TS1, _}, {TS2, V2}, Interval) ->
    case TS1 - TS2 > Interval of
        true -> {TS1 - Interval, V2};
        false -> {TS2, V2}
    end.

-spec last(slide()) -> value() | undefined.
last(#slide{total = T}) when T =/= undefined ->
    T;
last(#slide{buf1 = [{_TS, T} | _]}) ->
    T;
last(#slide{buf2 = [{_TS, T} | _]}) ->
    T;
last(_) ->
    undefined.

-spec foldl(timestamp(), timestamp(), fold_fun(), fold_acc(), slide()) -> fold_acc().
%% @doc Fold over the sliding window, starting from `Timestamp'.
%% Now provides a reference point to evaluate whether to include
%% partial, unrealised sample values in the sequence. Unrealised values will be
%% appended to the sequence when Now >= LastTS + Interval
%%
%% The fun should as `fun({Timestamp, Value}, Acc) -> NewAcc'.
%% The values are processed in order from oldest to newest.
%% @end
foldl(_Now, _Timestamp, _Fun, _Acc, #slide{size = Sz}) when Sz == 0 ->
    [];
foldl(Now, Start0, Fun, Acc, #slide{max_n = _MaxN, buf2 = _Buf2,
                                    interval = _Interval} = Slide) ->
    lists:foldl(Fun, Acc, element(2, to_list_from(Now, Start0, Slide)) ++ [last]).

map(Fun, #slide{buf1 = Buf1, buf2 = Buf2, total = Total} = Slide) ->
    BufFun = fun({Timestamp, Value}) ->
                     {Timestamp, Fun(Value)}
             end,
    MappedBuf1 = lists:map(BufFun, Buf1),
    MappedBuf2 = lists:map(BufFun, Buf2),
    MappedTotal = Fun(Total),
    Slide#slide{buf1 = MappedBuf1, buf2 = MappedBuf2, total = MappedTotal}.

maybe_add_last_sample(_Now, #slide{total = T, n = N,
                                   buf1 = [{_, T} | _] = Buf1}) ->
    {N, Buf1};
maybe_add_last_sample(Now, #slide{total = T,
                                  n = N,
                                  last = Last,
                                  interval = I,
                                  buf1 = Buf1})
  when T =/= undefined andalso Now >= Last + I  ->
    {N + 1, [{Last + I, T} | Buf1]};
maybe_add_last_sample(_Now, #slide{buf1 = Buf1, n = N}) ->
    {N, Buf1}.


create_normalized_lookup(Start, Interval, RoundFun, Samples) ->
    lists:foldl(fun({TS, Value}, Acc) when TS - Start >= 0 ->
                        NewTS = map_timestamp(TS, Start, Interval, RoundFun),
                        maps:update_with(NewTS,
                                         fun({T, V}) when T > TS ->
                                                 {T, V};
                                            (_) ->
                                                 {TS, Value}
                                         end, {TS, Value}, Acc);
                   (_, Acc) ->
                        Acc
                end, #{}, Samples).

-spec to_normalized_list(timestamp(), timestamp(), integer(), slide(),
                         no_pad | tuple()) -> [tuple()].
to_normalized_list(Now, Start, Interval, Slide, Empty) ->
    to_normalized_list(Now, Start, Interval, Slide, Empty, fun ceil/1).

to_normalized_list(Now, Start, Interval, #slide{first = FirstTS0,
                                                total = Total} = Slide,
                   Empty, RoundFun) ->

    RoundTSFun = fun (TS) -> map_timestamp(TS, Start, Interval, RoundFun) end,

    % add interval as we don't want to miss a sample due to rounding
    {Prev, Samples} = to_list_from(Now + Interval, Start, Slide),
    Lookup = create_normalized_lookup(Start, Interval, RoundFun, Samples),

    NowRound = RoundTSFun(Now),

    Pad = case Samples of
              _ when Empty =:= no_pad ->
                  [];
              [{TS, _} | _] when Prev =/= undefined, Start =< TS ->
                  [{T, snd(Prev)}
                   || T <- lists:seq(RoundTSFun(TS) - Interval, Start,
                                     -Interval)];
              [{TS, _} | _] when is_number(FirstTS0) andalso Start < FirstTS0 ->
                  % only if we know there is nothing in the past can we
                  % generate a 0 pad
                  [{T, Empty} || T <- lists:seq(RoundTSFun(TS) - Interval, Start,
                                                -Interval)];
              _ when FirstTS0 =:= undefined andalso Total =:= undefined ->
                     [{T, Empty} || T <- lists:seq(NowRound, Start, -Interval)];
              [] -> % samples have been seen, use the total to pad
                     [{T, Total} || T <- lists:seq(NowRound, Start, -Interval)];
              _ -> []
          end,

    {_, Res1} = lists:foldl(
                  fun(T, {Last, Acc}) ->
                          case maps:find(T, Lookup) of
                              {ok, {_, V}} ->
                                  {V, [{T, V} | Acc]};
                              error when Last =:= undefined ->
                                  {Last, Acc};
                              error -> % this pads the last value into the future
                                  {Last, [{T, Last} | Acc]}
                          end
                  end, {undefined, []},
                  lists:seq(Start, NowRound, Interval)),
    Res1 ++ Pad.


%% @doc Sums a list of slides
%%
%% Takes the last known timestamp and creates an template version of the
%% sliding window. Timestamps are then truncated and summed with the value
%% in the template slide.
%% @end
-spec sum([slide()]) -> slide().
sum(Slides) -> sum(Slides, no_pad).

sum([#slide{size = Size, interval = Interval} | _] = Slides, Pad) ->
    % take the freshest timestamp as reference point for summing operation
    Now = lists:max([Last || #slide{last = Last} <- Slides]),
    Start = Now - Size,
    sum(Now, Start, Interval, Slides, Pad).


sum(Now, Start, Interval, [Slide | _ ] = All, Pad) ->
    Fun = fun({TS, Value}, Acc) ->
                  maps:update_with(TS, fun(V) -> add_to_total(V, Value) end,
                                   Value, Acc)
          end,
    {Total, Dict} =
        lists:foldl(fun(#slide{total = T} = S, {Tot, Acc}) ->
                           Samples = to_normalized_list(Now, Start, Interval, S,
                                                        Pad, fun ceil/1),
                           Total = add_to_total(T, Tot),
                           Folded = lists:foldl(Fun, Acc, Samples),
                           {Total, Folded}
                    end, {undefined, #{}}, All),

    {First, Buffer} = case lists:sort(maps:to_list(Dict)) of
                          [] ->
                              F = case [TS || #slide{first = TS} <- All,
                                              is_integer(TS)] of
                                      [] -> undefined;
                                      FS -> lists:min(FS)
                                  end,
                              {F, []};
                          [{F, _} | _ ] = B ->
                              {F, lists:reverse(B)}
                      end,
    Slide#slide{buf1 = Buffer, buf2 = [], total = Total, n = length(Buffer),
                first = First, last = Now}.


truncated_seq(_First, _Last, _Incr, 0) ->
    [];
truncated_seq(TS, TS, _Incr, MaxN) when MaxN > 0 ->
    [TS];
truncated_seq(First, Last, Incr, MaxN) when First =< Last andalso MaxN > 0 ->
    End = min(Last, First + (MaxN * Incr) - Incr),
    lists:seq(First, End, Incr);
truncated_seq(First, Last, Incr, MaxN) ->
    End = max(Last, First + (MaxN * Incr) - Incr),
    lists:seq(First, End, Incr).


take_since([{DropTS, drop} | T], Now, Start, N, [{TS, Evt} | _] = Acc,
           Interval) ->
    case T of
        [] ->
            Fill = [{TS0, Evt} || TS0 <- truncated_seq(TS - Interval,
                                                       max(DropTS, Start),
                                                       -Interval, N)],
            {undefined, lists:reverse(Fill) ++ Acc};
        [{TS0, _} = E | Rest] when TS0 >= Start, N > 0 ->
            Fill = [{TS1, Evt} || TS1 <- truncated_seq(TS0 + Interval,
                                                       max(TS0 + Interval, TS - Interval),
                                                       Interval, N)],
            take_since(Rest, Now, Start, decr(N), [E | Fill ++ Acc], Interval);
        [Prev | _] -> % next sample is out of range so needs to be filled from Start
            Fill = [{TS1, Evt} || TS1 <- truncated_seq(Start, max(Start, TS - Interval),
                                                       Interval, N)],
            {Prev, Fill ++ Acc}
    end;
take_since([{TS, V} = H | T], Now, Start, N, Acc, Interval) when TS >= Start,
                                                                 N > 0,
                                                                 TS =< Now,
                                                                 is_tuple(V) ->
    take_since(T, Now, Start, decr(N), [H|Acc], Interval);
take_since([{TS,_} | T], Now, Start, N, Acc, Interval) when TS >= Start, N > 0 ->
    take_since(T, Now, Start, decr(N), Acc, Interval);
take_since([Prev | _], _, _, _, Acc, _) ->
    {Prev, Acc};
take_since(_, _, _, _, Acc, _) ->
    %% Don't reverse; already the wanted order.
    {undefined, Acc}.

decr(N) when is_integer(N) ->
    N-1;
decr(N) -> N.

n_diff(A, B) when is_integer(A) ->
    A - B.

ceil(X) when X < 0 ->
    trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

map_timestamp(TS, Start, Interval, Round) ->
    Factor = Round((TS - Start) / Interval),
    Start + Interval * Factor.

buffer(#slide{buf1 = Buf1, buf2 = Buf2}) ->
    Buf1 ++ Buf2.
