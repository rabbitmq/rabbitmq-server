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
%% timestamp (millisecond resolution, see `exometer_util:timestamp/0')
%% and prepended to the primary list. When the time span between the oldest
%% and the newest entry in the primary list exceeds the given window size,
%% the primary list is shifted into the secondary list position, and the
%% new entry is added to a new (empty) primary list.
%%
%% The window can be converted to a list using `to_list/1' or folded
%% over using `foldl/3'.
%% @end
-module(exometer_slide).

-export([new/2, new/5,
         reset/1,
         add_element/2,
         add_element/3,
         add_element/4,
         to_list/1,
         foldl/3,
         foldl/4]).

-compile(inline).
-compile(inline_list_funcs).

-import(lists, [reverse/1]).
-import(exometer_util, [timestamp/0]).

-type value() :: any().
-type cur_state() :: any().
-type timestamp() :: exometer_util:timestamp().
-type sample_fun() :: fun((timestamp(), value(), cur_state()) ->
                                 cur_state()).
-type transform_fun() :: fun((timestamp(), cur_state()) ->
                                    cur_state()).

-type fold_acc() :: any().
-type fold_fun() :: fun(({timestamp(),value()}, fold_acc()) -> fold_acc()).

%% Fixed size event buffer
-record(slide, {size = 0 :: integer(),  % ms window
                n = 0    :: integer(),  % number of elements in buf1
                max_n    :: undefined | integer(),  % max no of elements
                last = 0 :: integer(), % millisecond timestamp
                buf1 = []    :: list(),
                buf2 = []    :: list()}).

-spec new(integer(), integer(),
          sample_fun(), transform_fun(), list()) -> #slide{}.
%% @doc Callback function for exometer_histogram
%%
%% This function is not intended to be used directly. The arguments
%% `_SampleFun' and `_TransformFun' are ignored.
%% @end
new(Size, _Period, _SampleFun, _TransformFun, Opts) ->
    new(Size, Opts).

-spec new(_Size::integer(), _Options::list()) -> #slide{}.
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
new(Size, Opts) ->
    #slide{size = Size,
           max_n = proplists:get_value(max_n, Opts, infinity),
           last = timestamp(),
           buf1 = [],
           buf2 = []}.

-spec reset(#slide{}) -> #slide{}.
%% @doc Empty the buffer
%%
reset(Slide) ->
    Slide#slide{n = 0, buf1 = [], buf2 = [], last = 0}.

-spec add_element(value(), #slide{}) -> #slide{}.
%% @doc Add an element to the buffer, tagging it with the current time.
%%
%% Note that the buffer is a sliding window. Values will be discarded as they
%% move out of the specified time span.
%% @end
%%
add_element(Evt, Slide) ->
    add_element(timestamp(), Evt, Slide, false).

-spec add_element(timestamp(), value(), #slide{}) -> #slide{}.
%% @doc Add an element to the buffer, tagged with the given timestamp.
%%
%% Apart from the specified timestamp, this function works just like
%% {@link add_element/2}.
%% @end
%%
add_element(TS, Evt, Slide) ->
    add_element(TS, Evt, Slide, false).

-spec add_element(timestamp(), value(), #slide{}, true) ->
                         {boolean(), #slide{}};
                 (timestamp(), value(), #slide{}, false) ->
                         #slide{}.
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
add_element(TS, Evt, #slide{last = Last, size = Sz,
                            n = N, max_n = MaxN,
                            buf1 = Buf1} = Slide, Wrap) ->
    N1 = N+1,
    if TS - Last > Sz; N1 > MaxN ->
            %% swap
            add_ret(Wrap, true, Slide#slide{last = TS,
                                            n = 1,
                                            buf1 = [{TS, Evt}],
                                            buf2 = Buf1});
       true ->
            add_ret(Wrap, false, Slide#slide{n = N1, buf1 = [{TS, Evt} | Buf1]})
    end.

add_ret(false, _, Slide) ->
    Slide;
add_ret(true, Flag, Slide) ->
    {Flag, Slide}.


-spec to_list(#slide{}) -> [{timestamp(), value()}].
%% @doc Convert the sliding window into a list of timestamped values.
%% @end
to_list(#slide{size = Sz}) when Sz == 0 ->
    [];
to_list(#slide{size = Sz, n = N, max_n = MaxN, buf1 = Buf1, buf2 = Buf2}) ->
    Start = timestamp() - Sz,
    take_since(Buf2, Start, n_diff(MaxN, N), reverse(Buf1)).

-spec foldl(timestamp(), fold_fun(), fold_acc(), #slide{}) -> fold_acc().
%% @doc Fold over the sliding window, starting from `Timestamp'.
%%
%% The fun should as `fun({Timestamp, Value}, Acc) -> NewAcc'.
%% The values are processed in order from oldest to newest.
%% @end
foldl(_Timestamp, _Fun, _Acc, #slide{size = Sz}) when Sz == 0 ->
    [];
foldl(Timestamp, Fun, Acc, #slide{size = Sz, n = N, max_n = MaxN,
                                  buf1 = Buf1, buf2 = Buf2}) ->
    Start = Timestamp - Sz,
    lists:foldr(
      Fun, lists:foldl(Fun, Acc, take_since(
                                   Buf2, Start, n_diff(MaxN,N), [])), Buf1).

-spec foldl(fold_fun(), fold_acc(), #slide{}) -> fold_acc().
%% @doc Fold over all values in the sliding window.
%%
%% The fun should as `fun({Timestamp, Value}, Acc) -> NewAcc'.
%% The values are processed in order from oldest to newest.
%% @end
foldl(Fun, Acc, Slide) ->
    foldl(timestamp(), Fun, Acc, Slide).

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
