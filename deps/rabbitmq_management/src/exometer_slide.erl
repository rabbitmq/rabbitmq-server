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
%%   file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
%% The window can be converted to a list using `to_list/1' or folded
%% over using `foldl/3'.
%% @end
%%
%%
%% All modifications are (C) 2007-2016 Pivotal Software, Inc. All rights reserved.
%% The Initial Developer of the Original Code is Basho Technologies, Inc.
%%
-module(exometer_slide).

-export([new/2, new/3, new/5,
         reset/1,
         add_element/2,
         add_element/3,
         add_element/4,
         to_list/1,
         foldl/3,
         foldl/4]).

-export([timestamp/0,
	 last_two/1,
	 last/1]).

-export([sum/1]).

-compile(inline).
-compile(inline_list_funcs).

-import(lists, [reverse/1]).

-type value() :: any().
-type cur_state() :: any().
-type timestamp() :: timestamp().
-type sample_fun() :: fun((timestamp(), value(), cur_state()) ->
                                 cur_state()).
-type transform_fun() :: fun((timestamp(), cur_state()) ->
                                    cur_state()).

-type fold_acc() :: any().
-type fold_fun() :: fun(({timestamp(),value()}, fold_acc()) -> fold_acc()).

%% Fixed size event buffer
-record(slide, {size = 0 :: integer(),  % ms window
                n = 0 :: integer(),  % number of elements in buf1
                max_n :: undefined | integer(),  % max no of elements
                incremental = false :: boolean(),
                interval :: integer(),
                last = 0 :: integer(), % millisecond timestamp
                buf1 = [] :: list(),
                buf2 = [] :: list(),
                total :: any()}).

-opaque slide() :: #slide{}.

-export_type([slide/0]).

-spec timestamp() -> timestamp().
%% @doc Generate a millisecond-resolution timestamp.
%%
%% This timestamp format is used e.g. by the `exometer_slide' and
%% `exometer_histogram' implementations.
%% @end
timestamp() ->
    time_compat:os_system_time(milli_seconds).

-spec new(integer(), integer(),
          sample_fun(), transform_fun(), list()) -> slide().
%% @doc Callback function for exometer_histogram
%%
%% This function is not intended to be used directly. The arguments
%% `_SampleFun' and `_TransformFun' are ignored.
%% @end
new(Size, _Period, _SampleFun, _TransformFun, Opts) ->
    new(Size, Opts).

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
           incremental = proplists:get_value(incremental, Opts, false),
           buf1 = [],
           buf2 = []}.

-spec reset(slide()) -> slide().
%% @doc Empty the buffer
%%
reset(Slide) ->
    Slide#slide{n = 0, buf1 = [], buf2 = [], last = 0}.

-spec add_element(value(), slide()) -> slide().
%% @doc Add an element to the buffer, tagging it with the current time.
%%
%% Note that the buffer is a sliding window. Values will be discarded as they
%% move out of the specified time span.
%% @end
%%
add_element(Evt, Slide) ->
    add_element(timestamp(), Evt, Slide, false).

-spec add_element(timestamp(), value(), slide()) -> slide().
%% @doc Add an element to the buffer, tagged with the given timestamp.
%%
%% Apart from the specified timestamp, this function works just like
%% {@link add_element/2}.
%% @end
%%
add_element(TS, Evt, Slide) ->
    add_element(TS, Evt, Slide, false).

-spec add_element(timestamp(), value(), slide(), true) ->
                         {boolean(), slide()};
                 (timestamp(), value(), slide(), false) ->
                         slide().
%% @doc Add an element to the buffer, optionally indicating if a swap occurred.
%%
%% This function works like {@link add_element/3}, but will also indicate
%% whether the sliding window buffer swapped lists (this means that the
%% 'primary' buffer list became full and was swapped to 'secondary', starting
%% over with an empty primary list. If `Wrap == true', the return value will be
%% `{Bool,Slide}', where `Bool==true' means that a swap occurred, and
%% `Bool==false' means that it didn't.
%%
%% If `Wrap == false', this function works exactly like {@link add_element/3}.
%%
%% One possible use of the `Wrap == true' option could be to keep a sliding
%% window buffer of values that are pushed e.g. to an external stats service.
%% The swap indication could be a trigger point where values are pushed in order
%% to not lose entries.
%% @end
%%
add_element(_TS, _Evt, Slide, Wrap) when Slide#slide.size == 0 ->
    add_ret(Wrap, false, Slide);
add_element(TS, Evt, #slide{last = Last, interval = Interval, total = Total0,
                            incremental = true} = Slide, _Wrap)
  when (TS - Last) < Interval ->
    Total = add_to_total(Evt, Total0),
    Slide#slide{total = Total};
add_element(TS, Evt, #slide{last = Last, interval = Interval} = Slide, _Wrap)
  when (TS - Last) < Interval ->
    Slide#slide{total = Evt};
add_element(TS, Evt, #slide{last = Last, size = Sz, incremental = true,
                            n = N, max_n = MaxN, total = Total0,
                            buf1 = Buf1} = Slide, Wrap) ->
    N1 = N+1,
    Total = add_to_total(Evt, Total0),
    if TS - Last > Sz; N1 > MaxN ->
            %% swap
            add_ret(Wrap, true, Slide#slide{last = TS,
                                            n = 1,
                                            buf1 = [{TS, Total}],
                                            buf2 = Buf1,
					    total = Total});
       true ->
            add_ret(Wrap, false, Slide#slide{n = N1, buf1 = [{TS, Total} | Buf1],
					     last = TS, total = Total})
    end;
add_element(TS, Evt, #slide{last = Last, size = Sz,
                            n = N, max_n = MaxN,
                            buf1 = Buf1} = Slide, Wrap) ->
    N1 = N+1,
    if TS - Last > Sz; N1 > MaxN ->
            %% swap
            add_ret(Wrap, true, Slide#slide{last = TS,
                                            n = 1,
                                            buf1 = [{TS, Evt}],
                                            buf2 = Buf1,
					    total = Evt});
       true ->
            add_ret(Wrap, false, Slide#slide{n = N1, buf1 = [{TS, Evt} | Buf1],
					     last = TS, total = Evt})
    end.

add_to_total(Evt, undefined) ->
    Evt;
add_to_total({A0}, {B0}) ->
    {B0 + A0};
add_to_total({A0, A1}, {B0, B1}) ->
    {B0 + A0, B1 + A1};
add_to_total({A0, A1, A2}, {B0, B1, B2}) ->
    {B0 + A0, B1 + A1, B2 + A2};
add_to_total({A0, A1, A2, A3, A4, A5, A6}, {B0, B1, B2, B3, B4, B5, B6}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6};
add_to_total({A0, A1, A2, A3, A4, A5, A6, A7}, {B0, B1, B2, B3, B4, B5, B6, B7}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6, B7 + A7};
add_to_total({A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14,
	      A15, A16, A17, A18, A19},
	     {B0, B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, B12, B13, B14,
	      B15, B16, B17, B18, B19}) ->
    {B0 + A0, B1 + A1, B2 + A2, B3 + A3, B4 + A4, B5 + A5, B6 + A6, B7 + A7, B8 + A8,
     B9 + A9, B10 + A10, B11 + A11, B12 + A12, B13 + A13, B14 + A14, B15 + A15, B16 + A16,
     B17 + A17, B18 + A18, B19 + A19}.

add_ret(false, _, Slide) ->
    Slide;
add_ret(true, Flag, Slide) ->
    {Flag, Slide}.


-spec to_list(#slide{}) -> [{timestamp(), value()}].
%% @doc Convert the sliding window into a list of timestamped values.
%% @end
to_list(Slide) ->
    to_list(timestamp(), Slide).

to_list(_Now, #slide{size = Sz}) when Sz == 0 ->
    [];
to_list(Now, #slide{size = Sz, n = N, max_n = MaxN, buf1 = Buf1, buf2 = Buf2}) ->
    Start = Now - Sz,
    take_since(Buf2, Start, n_diff(MaxN, N), reverse(Buf1)).

-spec last_two(slide()) -> [{timestamp(), value()}].
%% @doc Returns the newest 2 elements on the sample
% last_two(#slide{buf1 = [{TS, T0} = H1 | _], total = T, interval = I}) when T =/= undefined,
% 									   T =/= T0->
%     [{TS + I, T}, H1];
% last_two(#slide{buf1 = [], buf2 = [{TS, T0} = H1 | _], total = T, interval = I})
%   when T =/= undefined, T =/= T0 ->
%     [{TS + I, T}, H1];
last_two(#slide{buf1 = [H1, H2 | _]}) ->
    [H1, H2];
last_two(#slide{buf1 = [H1], buf2 = [H2 | _]}) ->
    [H1, H2];
last_two(#slide{buf1 = [H1], buf2 = []}) ->
    [H1];
last_two(#slide{buf1 = [], buf2 = [H1, H2 | _]}) ->
    [H1, H2];
last_two(#slide{buf1 = [], buf2 = [H1]}) ->
    [H1];
last_two(_) ->
    [].

-spec last(slide()) -> value() | undefined.
last(#slide{total = T}) when T =/= undefined ->
    T;
last(#slide{buf1 = [{_TS, T} | _]}) ->
    T;
last(#slide{buf2 = [{_TS, T} | _]}) ->
    T;
last(_) ->
    undefined.

-spec foldl(timestamp(), fold_fun(), fold_acc(), slide()) -> fold_acc().
%% @doc Fold over the sliding window, starting from `Timestamp'.
%%
%% The fun should as `fun({Timestamp, Value}, Acc) -> NewAcc'.
%% The values are processed in order from oldest to newest.
%% @end
foldl(_Timestamp, _Fun, _Acc, #slide{size = Sz}) when Sz == 0 ->
    [];
foldl(Timestamp, Fun, Acc, #slide{n = N, max_n = MaxN, buf2 = Buf2} = Slide) ->
    Start = Timestamp,
    %% Ensure real actuals are reflected, if no more data is coming we might never
    %% shown the last value (i.e. total messages after queue delete)
    Buf1 = [last | maybe_add_last_sample(Start, Slide)],
    lists:foldr(Fun, lists:foldl(Fun, Acc, take_since(
					     Buf2, Start, n_diff(MaxN,N), [])), Buf1).

maybe_add_last_sample(_Start, #slide{total = T, buf1 = [{TS, _} | _] = Buf1})
  when T =/= undefined ->
    [{TS + 1, T} | Buf1];
maybe_add_last_sample(_Start, #slide{total = T, buf2 = [{TS, _} | _]})
  when T =/= undefined ->
    [{TS + 1, T}];
maybe_add_last_sample(Start, #slide{total = T}) when T =/= undefined ->
    [{Start, T}];
maybe_add_last_sample(_Start, #slide{buf1 = Buf1}) ->
    Buf1.

-spec foldl(fold_fun(), fold_acc(), slide()) -> fold_acc().
%% @doc Fold over all values in the sliding window.
%%
%% The fun should as `fun({Timestamp, Value}, Acc) -> NewAcc'.
%% The values are processed in order from oldest to newest.
%% @end
foldl(Fun, Acc, #slide{size = Sz} = Slide) ->
    foldl(timestamp() - Sz, Fun, Acc, Slide).

%% @doc Normalize an incremental set of slides for summing
%%
%% Puts samples into buckets based on Now
%% Discards anything older than Now - Size
%% Fills in blanks in the ideal sequence with the last known value or undefined
%% @end
-spec normalize_incremental_slide(timestamp(), integer(), slide()) -> slide().
normalize_incremental_slide(Now, Interval, #slide{size = Size} = Slide) ->
    Start = Now - Size,
    Res = lists:foldl(fun({TS, Value}, Dict) when TS - Start > 0 ->
                              NewTS = map_timestamp(TS, Start, Interval),
                              orddict:update(NewTS, fun({T, V}) when T > TS -> {T, V};
                                                       (_) -> {TS, Value}
                                                    end, {TS, Value}, Dict);
                         (_, Dict) -> Dict end, orddict:new(),
                      to_list(Now, Slide)),

    {_, Res1} = lists:foldl(
                  fun(T, {Last, Acc}) ->
                    case orddict:find(T, Res) of
                       {ok, {_, V}} -> {V, [{T, V} | Acc]};
                       error when Last =:= undefined -> {Last, Acc};
                       error -> {Last, [{T, Last} | Acc]}
                    end
                  end,
                  {undefined, []},
                  lists:seq(Start, Now, Interval)),
    Slide#slide{buf1 = Res1}.

-spec sum([slide()]) -> slide().
%% @doc Sums a list of slides
%%
%% Takes the last known timestamp and creates an template version of the
%% sliding window. Timestamps are then truncated and summed with the value
%% in the template slide.
%% @end
sum(Slides) ->
    % take the freshest timestamp as reference point for summing operation
    Now = lists:max([Last || #slide{last = Last} <- Slides]),
    sum(Now, Slides).

sum(Now, [Slide = #slide{interval = Interval, size = Size, incremental = true} | _] = All) ->
    Start = Now - Size,
    Fun = fun(last, Dict) ->
		  Dict;
	     ({TS, Value}, Dict) ->
                  orddict:update(TS, fun(V) -> add_to_total(V, Value) end,
                                 Value, Dict)
          end,
    Dict = lists:foldl(fun(S, Acc) ->
                               Normalized = normalize_incremental_slide(Now, Interval, S),
                               %% Unwanted last sample here
                               foldl(Start, Fun, Acc, Normalized#slide{total = undefined})
                       end, orddict:new(), All),
    Buffer = lists:reverse(orddict:to_list(Dict)),
    Total = lists:foldl(fun(#slide{total = T}, Acc) ->
                                add_to_total(T, Acc)
                        end, undefined, All),
    Slide#slide{buf1 = Buffer, buf2 = [], total = Total};
sum(Now, [Slide = #slide{size = Size, interval = Interval} | _] = All) ->
    Start = Now - Size,
    Fun = fun(last, Dict) ->
		  Dict;
	     ({TS, Value}, Dict) ->
                  NewTS = map_timestamp(TS, Start, Interval),
                  orddict:update(NewTS, fun(V) -> add_to_total(V, Value) end,
                                 Value, Dict)
          end,
    Dict = lists:foldl(fun(S, Acc) ->
                               %% Unwanted last sample here
                               foldl(Start, Fun, Acc, S#slide{total = undefined})
                       end, orddict:new(), All),
    Buffer = lists:reverse(orddict:to_list(Dict)),
    Total = lists:foldl(fun(#slide{total = T}, Acc) ->
                                add_to_total(T, Acc)
                        end, undefined, All),
    Slide#slide{buf1 = Buffer, buf2 = [], total = Total}.

take_since([{TS,_} = H|T], Start, N, Acc) when TS >= Start, N > 0 ->
    take_since(T, Start, decr(N), [H|Acc]);
take_since(_, _, _, Acc) ->
    %% Don't reverse; already the wanted order.
    Acc.

decr(N) when is_integer(N) ->
    N-1.

n_diff(A, B) when is_integer(A) ->
    A - B;
n_diff(_, B) ->
    B.

ceil(X) when X < 0 ->
    trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

map_timestamp(TS, Start, Interval) ->
    Factor = ceil((TS - Start) / Interval),
    Start + Interval * Factor.

